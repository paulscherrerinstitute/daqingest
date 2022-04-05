use crate::bsread::{parse_zmtp_message, BsreadMessage};
use crate::bsread::{ChannelDesc, GlobalTimestamp, HeadA, HeadB};
use crate::channelwriter::{ChannelWriter, ChannelWriterF64};
use crate::netbuf::NetBuf;
use async_channel::{Receiver, Sender};
#[allow(unused)]
use bytes::BufMut;
use err::Error;
use futures_core::Stream;
use futures_util::{pin_mut, StreamExt};
use log::*;
use netpod::timeunits::*;
use scylla::batch::{Batch, BatchType, Consistency};
use scylla::prepared_statement::PreparedStatement;
use scylla::transport::errors::QueryError;
use scylla::{Session as ScySession, SessionBuilder};
use serde_json::Value as JsVal;
use std::collections::BTreeMap;
use std::ffi::CStr;
use std::fmt;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

pub trait ErrConv<T> {
    fn err_conv(self) -> Result<T, Error>;
}

impl<T> ErrConv<T> for Result<T, QueryError> {
    fn err_conv(self) -> Result<T, Error> {
        match self {
            Ok(k) => Ok(k),
            Err(e) => Err(Error::with_msg_no_trace(format!("{e:?}"))),
        }
    }
}

#[allow(unused)]
fn test_listen() -> Result<(), Error> {
    use std::time::Duration;
    let fut = async move {
        let _ = tokio::time::timeout(Duration::from_millis(16000), futures_util::future::ready(0u32)).await;
        Ok::<_, Error>(())
    };
    taskrun::run(fut)
}

#[allow(unused)]
fn test_service() -> Result<(), Error> {
    let fut = async move {
        let sock = tokio::net::TcpListener::bind("0.0.0.0:9999").await?;
        loop {
            info!("accepting...");
            let (conn, remote) = sock.accept().await?;
            info!("new connection from {:?}", remote);
            let mut zmtp = Zmtp::new(conn, SocketType::PUSH);
            let fut = async move {
                while let Some(item) = zmtp.next().await {
                    info!("item from {:?}  {:?}", remote, item);
                }
                Ok::<_, Error>(())
            };
            taskrun::spawn(fut);
        }
    };
    taskrun::run(fut)
}

pub fn get_series_id(chn: &ChannelDesc) -> u32 {
    use md5::Digest;
    let mut h = md5::Md5::new();
    h.update(chn.name.as_bytes());
    let f = h.finalize();
    u32::from_le_bytes(f.as_slice()[0..4].try_into().unwrap())
}

pub struct CommonQueries {
    pub qu1: PreparedStatement,
    pub qu2: PreparedStatement,
    pub qu_insert_ts_msp: PreparedStatement,
    pub qu_insert_scalar_f64: PreparedStatement,
}

struct BsreadClient {
    #[allow(unused)]
    scylla: String,
    addr: String,
    do_pulse_id: bool,
    rcvbuf: Option<u32>,
    tmp_vals_pulse_map: Vec<(i64, i32, i64, i32)>,
    scy: Arc<ScySession>,
    channel_writers: BTreeMap<u32, Box<dyn ChannelWriter>>,
    common_queries: Arc<CommonQueries>,
}

impl BsreadClient {
    pub async fn new(scylla: String, addr: String, do_pulse_id: bool, rcvbuf: Option<u32>) -> Result<Self, Error> {
        let scy = SessionBuilder::new()
            .default_consistency(Consistency::Quorum)
            .known_node(&scylla)
            .use_keyspace("ks1", false)
            .build()
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let qu1 = scy
            .prepare("insert into pulse_pkey (a, pulse_a) values (?, ?)")
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let qu2 = scy
            .prepare("insert into pulse (pulse_a, pulse_b, ts_a, ts_b) values (?, ?, ?, ?)")
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let qu_insert_ts_msp = scy
            .prepare("insert into ts_msp (series, ts_msp) values (?, ?)")
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let qu_insert_scalar_f64 = scy
            .prepare("insert into events_scalar_f64 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)")
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let common_queries = CommonQueries {
            qu1,
            qu2,
            qu_insert_ts_msp,
            qu_insert_scalar_f64,
        };
        let ret = Self {
            scylla,
            addr,
            do_pulse_id,
            rcvbuf,
            tmp_vals_pulse_map: vec![],
            scy: Arc::new(scy),
            channel_writers: Default::default(),
            common_queries: Arc::new(common_queries),
        };
        Ok(ret)
    }

    pub async fn run(&mut self) -> Result<(), Error> {
        let mut conn = tokio::net::TcpStream::connect(&self.addr).await?;
        if let Some(v) = self.rcvbuf {
            set_rcv_sock_opts(&mut conn, v)?;
        }
        let mut zmtp = Zmtp::new(conn, SocketType::PULL);
        let mut i1 = 0u64;
        let mut msgc = 0u64;
        let mut dh_md5_last = String::new();
        let mut frame_diff_count = 0u64;
        let mut hash_mismatch_count = 0u64;
        let mut head_b = HeadB::empty();
        while let Some(item) = zmtp.next().await {
            match item {
                Ok(ev) => match ev {
                    ZmtpEvent::ZmtpCommand(_) => (),
                    ZmtpEvent::ZmtpMessage(msg) => {
                        msgc += 1;
                        match parse_zmtp_message(&msg) {
                            Ok(bm) => {
                                if msg.frames().len() - 2 * bm.head_b.channels.len() != 2 {
                                    frame_diff_count += 1;
                                    if frame_diff_count < 1000 {
                                        warn!(
                                            "chn len {}  frame diff {}",
                                            bm.head_b.channels.len(),
                                            msg.frames().len() - 2 * bm.head_b.channels.len()
                                        );
                                    }
                                }
                                if bm.head_b_md5 != bm.head_a.hash {
                                    hash_mismatch_count += 1;
                                    // TODO keep logging data header changes, just suppress too frequent messages.
                                    if hash_mismatch_count < 200 {
                                        error!(
                                            "Invalid bsread message: hash mismatch.  dhcompr {:?}",
                                            bm.head_a.dh_compression
                                        );
                                    }
                                }
                                {
                                    if bm.head_b_md5 != dh_md5_last {
                                        head_b = bm.head_b.clone();
                                        if dh_md5_last.is_empty() {
                                            info!("data header hash {}  mh {}", bm.head_b_md5, bm.head_a.hash);
                                            dh_md5_last = bm.head_b_md5.clone();
                                            for chn in &head_b.channels {
                                                info!("Setup writer for {}", chn.name);
                                                self.setup_channel_writers(chn)?;
                                            }
                                        } else {
                                            error!("changed data header hash {}  mh {}", bm.head_b_md5, bm.head_a.hash);
                                            dh_md5_last = bm.head_b_md5.clone();
                                            // TODO
                                            // Update only the changed channel writers.
                                            // Flush buffers before creating new channel writer.
                                        }
                                    }
                                }
                                if self.do_pulse_id {
                                    let mut i3 = u32::MAX;
                                    for (i, ch) in head_b.channels.iter().enumerate() {
                                        if ch.name == "SINEG01-RLLE-STA:MASTER-EVRPULSEID" {
                                            i3 = i as u32;
                                        }
                                    }
                                    // TODO need to know the facility!
                                    if i3 < u32::MAX {
                                        let i4 = 2 * i3 + 2;
                                        if i4 >= msg.frames.len() as u32 {
                                        } else {
                                            let fr = &msg.frames[i4 as usize];
                                            self.insert_pulse_map(fr, &msg, &bm).await?;
                                        }
                                    }
                                }
                                if msg.frames.len() < 2 + 2 * head_b.channels.len() {
                                    // TODO count always, throttle log.
                                    error!("not enough frames for data header");
                                }
                                let gts = bm.head_a.global_timestamp;
                                let ts = (gts.sec as u64) * SEC + gts.ns as u64;
                                let pulse = bm.head_a.pulse_id.as_u64().unwrap_or(0);
                                for i1 in 0..head_b.channels.len() {
                                    let chn = &head_b.channels[i1];
                                    let fr = &msg.frames[2 + 2 * i1];
                                    let series = get_series_id(chn);
                                    if !self.channel_writers.contains_key(&series) {}
                                    if let Some(cw) = self.channel_writers.get_mut(&series) {
                                        cw.write_msg(ts, pulse, fr)?.await?;
                                    } else {
                                        // TODO check for missing writers.
                                        //warn!("no writer for {}", chn.name);
                                    }
                                }
                            }
                            Err(e) => {
                                error!("{}", e);
                                for frame in &msg.frames {
                                    info!("Frame: {:?}", frame);
                                }
                                zmtp.dump_input_state();
                                zmtp.dump_conn_state();
                            }
                        }
                    }
                },
                Err(e) => {
                    error!("{}", e);
                    return Err(e);
                }
            }
            i1 += 1;
            if false && i1 > 10000 {
                break;
            }
            if false && msgc > 10000 {
                break;
            }
        }
        Ok(())
    }

    fn setup_channel_writers(&mut self, chn: &ChannelDesc) -> Result<(), Error> {
        let series = get_series_id(chn);
        match chn.ty.as_str() {
            "float64" => match &chn.shape {
                JsVal::Array(a) => {
                    if a.len() == 1 {
                        if let Some(n) = a[0].as_u64() {
                            if n == 1 {
                                if chn.encoding == "big" {
                                    let cw =
                                        ChannelWriterF64::new(series, self.common_queries.clone(), self.scy.clone());
                                    self.channel_writers.insert(series, Box::new(cw));
                                } else {
                                    warn!("No LE avail");
                                }
                            } else {
                                warn!("array f64 writer not yet available.")
                            }
                        }
                    } else {
                        warn!("f64 writer not yet available for shape {:?}", a)
                    }
                }
                s => {
                    warn!("setup_channel_writers  shape not supported {:?}", s);
                }
            },
            k => {
                warn!("setup_channel_writers  data type not supported {:?}", k);
            }
        }
        Ok(())
    }

    async fn insert_pulse_map(&mut self, fr: &ZmtpFrame, msg: &ZmtpMessage, bm: &BsreadMessage) -> Result<(), Error> {
        trace!("data len {}", fr.data.len());
        // TODO take pulse-id also from main header and compare.
        let pulse_f64 = f64::from_be_bytes(fr.data[..].try_into().unwrap());
        trace!("pulse_f64 {pulse_f64}");
        let pulse = pulse_f64 as u64;
        if false {
            let i4 = 3;
            // TODO this next frame should be described somehow in the json header or?
            info!("next val len {}", msg.frames[i4 as usize + 1].data.len());
            let ts_a = u64::from_be_bytes(msg.frames[i4 as usize + 1].data[0..8].try_into().unwrap());
            let ts_b = u64::from_be_bytes(msg.frames[i4 as usize + 1].data[8..16].try_into().unwrap());
            info!("ts_a {ts_a}  ts_b {ts_b}");
        }
        let _ts = bm.head_a.global_timestamp.sec * SEC + bm.head_a.global_timestamp.ns;
        if true {
            let pulse_a = (pulse >> 14) as i64;
            let pulse_b = (pulse & 0x3fff) as i32;
            let ts_a = bm.head_a.global_timestamp.sec as i64;
            let ts_b = bm.head_a.global_timestamp.ns as i32;
            self.tmp_vals_pulse_map.push((pulse_a, pulse_b, ts_a, ts_b));
        }
        if self.tmp_vals_pulse_map.len() >= 200 {
            let ts1 = Instant::now();
            // TODO use facility, channel_name, ... as partition key.
            self.scy
                .execute(&self.common_queries.qu1, (1i32, self.tmp_vals_pulse_map[0].0))
                .await
                .err_conv()?;
            let mut batch = Batch::new(BatchType::Unlogged);
            for _ in 0..self.tmp_vals_pulse_map.len() {
                batch.append_statement(self.common_queries.qu2.clone());
            }
            let _ = self.scy.batch(&batch, &self.tmp_vals_pulse_map).await.err_conv()?;
            let nn = self.tmp_vals_pulse_map.len();
            self.tmp_vals_pulse_map.clear();
            let ts2 = Instant::now();
            let dt = ts2.duration_since(ts1).as_secs_f32() * 1e3;
            info!("insert {} items in {:6.2} ms", nn, dt);
        }
        Ok(())
    }
}

pub async fn zmtp_client(scylla: &str, addr: &str, rcvbuf: Option<u32>, do_pulse_id: bool) -> Result<(), Error> {
    let mut client = BsreadClient::new(scylla.into(), addr.into(), do_pulse_id, rcvbuf).await?;
    client.run().await
}

fn set_rcv_sock_opts(conn: &mut TcpStream, rcvbuf: u32) -> Result<(), Error> {
    use std::mem::size_of;
    use std::os::unix::prelude::AsRawFd;
    let fd = conn.as_raw_fd();
    unsafe {
        type N = libc::c_int;
        let n: N = rcvbuf as _;
        let ec = libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            &n as *const N as _,
            size_of::<N>() as _,
        );
        if ec != 0 {
            error!("ec {ec}");
            if ec != 0 {
                return Err(Error::with_msg_no_trace(format!("can not set socket option")));
            }
        }
    }
    unsafe {
        type N = libc::c_int;
        let mut n: N = -1;
        let mut l = size_of::<N>() as libc::socklen_t;
        let ec = libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            &mut n as *mut N as _,
            &mut l as _,
        );
        if ec != 0 {
            let errno = *libc::__errno_location();
            let es = CStr::from_ptr(libc::strerror(errno));
            warn!("can not query socket option  ec {ec}  errno {errno}  es {es:?}");
            error!("can not query socket option");
        } else {
            info!("SO_RCVBUF {n}");
        }
    }
    Ok(())
}

#[derive(Clone, Debug)]
enum ConnState {
    InitSend,
    InitRecv1,
    InitRecv2,
    InitRecv3,
    InitRecv4,
    InitRecv5,
    ReadFrameFlags,
    ReadFrameShort,
    ReadFrameLong,
    ReadFrameBody(usize),
}

impl ConnState {
    fn need_min(&self) -> usize {
        use ConnState::*;
        match self {
            InitSend => 0,
            InitRecv1 => 1,
            InitRecv2 => 9,
            InitRecv3 => 1,
            InitRecv4 => 1,
            InitRecv5 => 52,
            ReadFrameFlags => 1,
            ReadFrameShort => 1,
            ReadFrameLong => 8,
            ReadFrameBody(msglen) => *msglen,
        }
    }
}

pub enum SocketType {
    PUSH,
    PULL,
}

#[derive(Debug)]
enum InpState {
    Empty,
    Netbuf(usize, usize, usize),
}

impl Default for InpState {
    fn default() -> Self {
        InpState::Empty
    }
}

pub struct Zmtp {
    done: bool,
    complete: bool,
    socket_type: SocketType,
    conn: TcpStream,
    conn_state: ConnState,
    buf: NetBuf,
    outbuf: NetBuf,
    out_enable: bool,
    msglen: usize,
    has_more: bool,
    is_command: bool,
    peer_ver: (u8, u8),
    frames: Vec<ZmtpFrame>,
    inp_eof: bool,
    data_tx: Sender<u32>,
    data_rx: Receiver<u32>,
    input_state: Vec<InpState>,
    input_state_ix: usize,
    conn_state_log: Vec<ConnState>,
    conn_state_log_ix: usize,
}

impl Zmtp {
    fn new(conn: TcpStream, socket_type: SocketType) -> Self {
        let (tx, rx) = async_channel::bounded(1);
        Self {
            done: false,
            complete: false,
            socket_type,
            conn,
            conn_state: ConnState::InitSend,
            buf: NetBuf::new(1024 * 128),
            outbuf: NetBuf::new(1024 * 128),
            out_enable: false,
            msglen: 0,
            has_more: false,
            is_command: false,
            peer_ver: (0, 0),
            frames: vec![],
            inp_eof: false,
            data_tx: tx,
            data_rx: rx,
            input_state: vec![0; 64].iter().map(|_| InpState::default()).collect(),
            input_state_ix: 0,
            conn_state_log: vec![0; 64].iter().map(|_| ConnState::InitSend).collect(),
            conn_state_log_ix: 0,
        }
    }

    pub fn out_channel(&self) -> Sender<u32> {
        self.data_tx.clone()
    }

    fn inpbuf_conn(&mut self) -> (&mut TcpStream, ReadBuf) {
        (&mut self.conn, self.buf.read_buf_for_fill())
    }

    fn outbuf_conn(&mut self) -> (&mut TcpStream, &[u8]) {
        (&mut self.conn, self.outbuf.data())
    }

    fn record_input_state(&mut self) {
        let st = self.buf.state();
        self.input_state[self.input_state_ix] = InpState::Netbuf(st.0, st.1, self.buf.cap() - st.1);
        self.input_state_ix = (1 + self.input_state_ix) % self.input_state.len();
    }

    fn record_conn_state(&mut self) {
        self.conn_state_log[self.conn_state_log_ix] = self.conn_state.clone();
        self.conn_state_log_ix = (1 + self.conn_state_log_ix) % self.conn_state_log.len();
    }

    fn dump_input_state(&self) {
        info!("---------------------------------------------------------");
        info!("INPUT STATE DUMP");
        let mut i = self.input_state_ix;
        for _ in 0..self.input_state.len() {
            info!("{i:4}  {:?}", self.input_state[i]);
            i = (1 + i) % self.input_state.len();
        }
        info!("---------------------------------------------------------");
    }

    fn dump_conn_state(&self) {
        info!("---------------------------------------------------------");
        info!("CONN STATE DUMP");
        let mut i = self.conn_state_log_ix;
        for _ in 0..self.conn_state_log.len() {
            info!("{i:4}  {:?}", self.conn_state_log[i]);
            i = (1 + i) % self.conn_state_log.len();
        }
        info!("---------------------------------------------------------");
    }

    fn loop_body(mut self: Pin<&mut Self>, cx: &mut Context) -> Option<Poll<Result<ZmtpEvent, Error>>> {
        use Poll::*;
        let mut item_count = 0;
        // TODO should I better keep one serialized item in Self so that I know how much space it needs?
        let serialized: Int<Result<(), Error>> = if self.out_enable && self.outbuf.wcap() >= self.outbuf.cap() / 2 {
            match self.data_rx.poll_next_unpin(cx) {
                Ready(Some(_item)) => {
                    // TODO item should be something that we can convert into a zmtp message.
                    Int::Empty
                }
                Ready(None) => Int::Done,
                Pending => Int::Pend,
            }
        } else {
            Int::NoWork
        };
        item_count += serialized.item_count();
        let write: Int<Result<(), _>> = if item_count > 0 {
            Int::NoWork
        } else if self.outbuf.len() > 0 {
            let (w, b) = self.outbuf_conn();
            pin_mut!(w);
            match w.poll_write(cx, b) {
                Ready(k) => match k {
                    Ok(k) => match self.outbuf.adv(k) {
                        Ok(()) => {
                            trace!("sent {} bytes", k);
                            self.outbuf.rewind_if_needed();
                            Int::Empty
                        }
                        Err(e) => {
                            error!("advance error {:?}", e);
                            Int::Item(Err(e))
                        }
                    },
                    Err(e) => {
                        error!("output write error {:?}", e);
                        Int::Item(Err(e.into()))
                    }
                },
                Pending => Int::Pend,
            }
        } else {
            Int::NoWork
        };
        match write {
            Int::NoWork => {}
            _ => {
                trace!("write result: {:?}  {}", write, self.outbuf.len());
            }
        }
        item_count += write.item_count();
        let read: Int<Result<(), _>> = if item_count > 0 || self.inp_eof {
            Int::NoWork
        } else {
            if self.buf.cap() < self.conn_state.need_min() {
                self.done = true;
                let e = Error::with_msg_no_trace(format!(
                    "buffer too small for need_min  {}  {}",
                    self.buf.cap(),
                    self.conn_state.need_min()
                ));
                Int::Item(Err(e))
            } else if self.buf.len() < self.conn_state.need_min() {
                self.record_input_state();
                let (w, mut rbuf) = self.inpbuf_conn();
                pin_mut!(w);
                match w.poll_read(cx, &mut rbuf) {
                    Ready(k) => match k {
                        Ok(()) => {
                            let nf = rbuf.filled().len();
                            if nf == 0 {
                                info!("EOF");
                                self.inp_eof = true;
                                self.record_input_state();
                                Int::Done
                            } else {
                                trace!("received {} bytes", rbuf.filled().len());
                                if false {
                                    let t = rbuf.filled().len();
                                    let t = if t < 32 { t } else { 32 };
                                    trace!("got data  {:?}", &rbuf.filled()[0..t]);
                                }
                                match self.buf.wadv(nf) {
                                    Ok(()) => {
                                        self.record_input_state();
                                        Int::Empty
                                    }
                                    Err(e) => {
                                        error!("netbuf wadv fail  nf {nf}");
                                        Int::Item(Err(e))
                                    }
                                }
                            }
                        }
                        Err(e) => Int::Item(Err(e.into())),
                    },
                    Pending => Int::Pend,
                }
            } else {
                Int::NoWork
            }
        };
        item_count += read.item_count();
        let parsed = if item_count > 0 || self.buf.len() < self.conn_state.need_min() {
            Int::NoWork
        } else {
            match self.parse_item() {
                Ok(k) => match k {
                    Some(k) => Int::Item(Ok(k)),
                    None => Int::Empty,
                },
                Err(e) => Int::Item(Err(e)),
            }
        };
        item_count += parsed.item_count();
        let _ = item_count;
        {
            use Int::*;
            match (serialized, write, read, parsed) {
                (NoWork | Done, NoWork | Done, NoWork | Done, NoWork | Done) => {
                    warn!("all NoWork or Done");
                    return Some(Pending);
                }
                (Item(Err(e)), _, _, _) => {
                    self.done = true;
                    return Some(Ready(Err(e)));
                }
                (_, Item(Err(e)), _, _) => {
                    self.done = true;
                    return Some(Ready(Err(e)));
                }
                (_, _, Item(Err(e)), _) => {
                    self.done = true;
                    return Some(Ready(Err(e)));
                }
                (_, _, _, Item(Err(e))) => {
                    self.done = true;
                    return Some(Ready(Err(e)));
                }
                (Item(_), _, _, _) => {
                    return None;
                }
                (_, Item(_), _, _) => {
                    return None;
                }
                (_, _, Item(_), _) => {
                    return None;
                }
                (_, _, _, Item(Ok(item))) => {
                    return Some(Ready(Ok(item)));
                }
                (Empty, _, _, _) => return None,
                (_, Empty, _, _) => return None,
                (_, _, Empty, _) => return None,
                (_, _, _, Empty) => return None,
                #[allow(unreachable_patterns)]
                (Pend, Pend | NoWork | Done, Pend | NoWork | Done, Pend | NoWork | Done) => return Some(Pending),
                #[allow(unreachable_patterns)]
                (Pend | NoWork | Done, Pend, Pend | NoWork | Done, Pend | NoWork | Done) => return Some(Pending),
                #[allow(unreachable_patterns)]
                (Pend | NoWork | Done, Pend | NoWork | Done, Pend, Pend | NoWork | Done) => return Some(Pending),
                #[allow(unreachable_patterns)]
                (Pend | NoWork | Done, Pend | NoWork | Done, Pend | NoWork | Done, Pend) => return Some(Pending),
            }
        };
    }

    fn parse_item(&mut self) -> Result<Option<ZmtpEvent>, Error> {
        self.record_conn_state();
        match self.conn_state {
            ConnState::InitSend => {
                info!("parse_item  InitSend");
                self.outbuf.put_slice(&[0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 3, 1])?;
                self.conn_state = ConnState::InitRecv1;
                Ok(None)
            }
            ConnState::InitRecv1 => {
                let b = self.buf.read_u8()?;
                if b != 0xff {
                    Err(Error::with_msg_no_trace(format!("InitRecv1 peer is not zmtp 3.x")))
                } else {
                    self.conn_state = ConnState::InitRecv2;
                    Ok(None)
                }
            }
            ConnState::InitRecv2 => {
                self.buf.adv(8)?;
                let b = self.buf.read_u8()?;
                if b & 0x01 != 1 {
                    Err(Error::with_msg_no_trace(format!("InitRecv2 peer is not zmtp 3.x")))
                } else {
                    self.conn_state = ConnState::InitRecv3;
                    Ok(None)
                }
            }
            ConnState::InitRecv3 => {
                let maj = self.buf.read_u8()?;
                if maj != 3 {
                    Err(Error::with_msg_no_trace(format!("InitRecv3 peer is not zmtp 3.x")))
                } else {
                    self.peer_ver.0 = maj;
                    self.outbuf.put_slice(&[0x4e, 0x55, 0x4c, 0x4c])?;
                    let a = vec![0; 48];
                    self.outbuf.put_slice(&a)?;
                    self.conn_state = ConnState::InitRecv4;
                    Ok(None)
                }
            }
            ConnState::InitRecv4 => {
                let minver = self.buf.read_u8()?;
                if minver > 1 {
                    Err(Error::with_msg_no_trace(format!(
                        "InitRecv3 peer is not zmtp 3.0 or 3.1"
                    )))
                } else {
                    self.peer_ver.1 = minver;
                    info!("InitRecv4  peer version {:?}", self.peer_ver);
                    self.conn_state = ConnState::InitRecv5;
                    Ok(None)
                }
            }
            ConnState::InitRecv5 => {
                {
                    let b2 = self.buf.read_bytes(20)?;
                    let mut i = 0;
                    while i < b2.len() && b2[i] != 0 {
                        i += 1;
                    }
                    if i >= b2.len() {
                        return Err(Error::with_msg_no_trace(format!("InitRecv5  bad mechanism from peer")));
                    } else {
                        let sec = String::from_utf8(b2[..i].to_vec())?;
                        info!("Peer security mechanism  {}  [{}]", sec.len(), sec);
                    }
                }
                self.buf.adv(32)?;
                match self.socket_type {
                    SocketType::PUSH => {
                        self.outbuf
                            .put_slice(&b"\x04\x1a\x05READY\x0bSocket-Type\x00\x00\x00\x04PUSH"[..])?;
                    }
                    SocketType::PULL => {
                        self.outbuf
                            .put_slice(&b"\x04\x1a\x05READY\x0bSocket-Type\x00\x00\x00\x04PULL"[..])?;
                    }
                }
                self.out_enable = true;
                self.conn_state = ConnState::ReadFrameFlags;
                Ok(None)
            }
            ConnState::ReadFrameFlags => {
                let flags = self.buf.read_u8()?;
                let has_more = flags & 0x01 != 0;
                let long_size = flags & 0x02 != 0;
                let is_command = flags & 0x04 != 0;
                if is_command {
                    if has_more {
                        error!("received command with has_more flag (error in peer)");
                    }
                    if self.has_more {
                        debug!(
                            "received command frame while in multipart, having {}",
                            self.frames.len()
                        );
                    }
                } else {
                    self.has_more = has_more;
                }
                self.is_command = is_command;
                trace!(
                    "parse_item  ReadFrameFlags  has_more {}  long_size {}  is_command {}",
                    has_more,
                    long_size,
                    is_command
                );
                if long_size {
                    self.conn_state = ConnState::ReadFrameLong;
                } else {
                    self.conn_state = ConnState::ReadFrameShort;
                }
                Ok(None)
            }
            ConnState::ReadFrameShort => {
                self.msglen = self.buf.read_u8()? as usize;
                trace!("parse_item  ReadFrameShort  msglen {}", self.msglen);
                self.conn_state = ConnState::ReadFrameBody(self.msglen);
                if self.msglen > self.buf.cap() / 2 {
                    error!("msglen {} too large for this client", self.msglen);
                    return Err(Error::with_msg_no_trace(format!(
                        "larger msglen not yet supported  {}",
                        self.msglen,
                    )));
                }
                Ok(None)
            }
            ConnState::ReadFrameLong => {
                self.msglen = self.buf.read_u64()? as usize;
                trace!("parse_item  ReadFrameShort  msglen {}", self.msglen);
                self.conn_state = ConnState::ReadFrameBody(self.msglen);
                if self.msglen > self.buf.cap() / 2 {
                    error!("msglen {} too large for this client", self.msglen);
                    return Err(Error::with_msg_no_trace(format!(
                        "larger msglen not yet supported  {}",
                        self.msglen,
                    )));
                }
                Ok(None)
            }
            ConnState::ReadFrameBody(msglen) => {
                // TODO do not copy here...
                let data = self.buf.read_bytes(msglen)?.to_vec();
                self.conn_state = ConnState::ReadFrameFlags;
                self.msglen = 0;
                if false {
                    let n1 = data.len().min(256);
                    let s = String::from_utf8_lossy(&data[..n1]);
                    trace!("parse_item  ReadFrameBody  msglen {}  string {}", msglen, s);
                }
                if self.is_command {
                    if data.len() >= 7 {
                        if &data[0..5] == b"\x04PING" {
                            if data.len() > 32 {
                                // TODO close connection?
                                error!("Oversized PING");
                            } else {
                                let ttl = u16::from_be_bytes(data[5..7].try_into().unwrap());
                                let ctx = &data[7..];
                                debug!("received PING  ttl {ttl}  ctx {:?}", &ctx);
                                if self.outbuf.wcap() < data.len() {
                                    warn!("can not respond with PONG because output buffer full");
                                } else {
                                    let size = 5 + ctx.len() as u8;
                                    self.outbuf.put_u8(0x04).unwrap();
                                    self.outbuf.put_u8(size).unwrap();
                                    self.outbuf.put_slice(b"\x04PONG").unwrap();
                                    self.outbuf.put_slice(ctx).unwrap();
                                }
                                if self.outbuf.wcap() < 32 {
                                    warn!("can not send my PING because output buffer full");
                                } else {
                                    let ctx = b"daqingest";
                                    let size = 5 + ctx.len() as u8;
                                    self.outbuf.put_u8(0x04).unwrap();
                                    self.outbuf.put_u8(size).unwrap();
                                    self.outbuf.put_slice(b"\x04PING").unwrap();
                                    self.outbuf.put_slice(ctx).unwrap();
                                }
                            }
                        }
                    }
                    let g = ZmtpFrame {
                        msglen: self.msglen,
                        has_more: self.has_more,
                        is_command: self.is_command,
                        data,
                    };
                    Ok(Some(ZmtpEvent::ZmtpCommand(g)))
                } else {
                    let g = ZmtpFrame {
                        msglen: self.msglen,
                        has_more: self.has_more,
                        is_command: self.is_command,
                        data,
                    };
                    self.frames.push(g);
                    if self.has_more {
                        Ok(None)
                    } else {
                        let g = ZmtpMessage {
                            frames: mem::replace(&mut self.frames, vec![]),
                        };
                        if false && g.frames.len() != 118 {
                            info!("EMIT {} frames", g.frames.len());
                            if let Some(fr) = g.frames.get(0) {
                                let d = fr.data();
                                let nn = d.len().min(16);
                                let s = String::from_utf8_lossy(&d[..nn]);
                                info!("DATA  0  {}  {:?}  {:?}", nn, &d[..nn], s);
                            }
                            if let Some(fr) = g.frames.get(1) {
                                let d = fr.data();
                                let nn = d.len().min(16);
                                let s = String::from_utf8_lossy(&d[..nn]);
                                info!("DATA  1  {}  {:?}  {:?}", nn, &d[..nn], s);
                            }
                        }
                        Ok(Some(ZmtpEvent::ZmtpMessage(g)))
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct ZmtpMessage {
    frames: Vec<ZmtpFrame>,
}

impl ZmtpMessage {
    pub fn frames(&self) -> &Vec<ZmtpFrame> {
        &self.frames
    }

    pub fn emit_to_buffer(&self, out: &mut NetBuf) -> Result<(), Error> {
        let n = self.frames.len();
        for (i, fr) in self.frames.iter().enumerate() {
            let mut flags: u8 = 2;
            if i < n - 1 {
                flags |= 1;
            }
            out.put_u8(flags)?;
            out.put_u64(fr.data().len() as u64)?;
            out.put_slice(fr.data())?;
        }
        Ok(())
    }
}

pub struct ZmtpFrame {
    msglen: usize,
    has_more: bool,
    is_command: bool,
    data: Vec<u8>,
}

impl ZmtpFrame {
    pub fn data(&self) -> &[u8] {
        &self.data
    }
}

impl fmt::Debug for ZmtpFrame {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let data = match String::from_utf8(self.data.clone()) {
            Ok(s) => s
                .chars()
                .take(32)
                .filter(|x| {
                    //
                    x.is_ascii_alphanumeric() || x.is_ascii_punctuation() || x.is_ascii_whitespace()
                })
                .collect::<String>(),
            Err(_) => format!("Binary {{ len: {} }}", self.data.len()),
        };
        f.debug_struct("ZmtpFrame")
            .field("msglen", &self.msglen)
            .field("has_more", &self.has_more)
            .field("is_command", &self.is_command)
            .field("data.len", &self.data.len())
            .field("data", &data)
            .finish()
    }
}

enum Int<T> {
    NoWork,
    Pend,
    Empty,
    Item(T),
    Done,
}

impl<T> Int<T> {
    fn item_count(&self) -> u32 {
        if let Int::Item(_) = self {
            1
        } else {
            0
        }
    }
}

impl<T> fmt::Debug for Int<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::NoWork => write!(f, "NoWork"),
            Self::Pend => write!(f, "Pend"),
            Self::Empty => write!(f, "Empty"),
            Self::Item(_) => write!(f, "Item"),
            Self::Done => write!(f, "Done"),
        }
    }
}

#[derive(Debug)]
pub enum ZmtpEvent {
    ZmtpCommand(ZmtpFrame),
    ZmtpMessage(ZmtpMessage),
}

impl Stream for Zmtp {
    type Item = Result<ZmtpEvent, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        if self.complete {
            panic!("poll_next on complete")
        } else if self.done {
            self.complete = true;
            return Ready(None);
        } else {
            loop {
                match Self::loop_body(self.as_mut(), cx) {
                    Some(Ready(k)) => break Ready(Some(k)),
                    Some(Pending) => break Pending,
                    None => continue,
                }
            }
        }
    }
}

#[allow(unused)]
struct DummyData {
    ts: u64,
    pulse: u64,
    value: i64,
}

impl DummyData {
    #[allow(unused)]
    fn make_zmtp_msg(&self) -> Result<ZmtpMessage, Error> {
        let head_b = HeadB {
            htype: "bsr_d-1.1".into(),
            channels: vec![ChannelDesc {
                name: "TESTCHAN".into(),
                ty: "int64".into(),
                shape: JsVal::Array(vec![JsVal::Number(serde_json::Number::from(1i32))]),
                encoding: "little".into(),
                compression: todo!(),
            }],
        };
        let hb = serde_json::to_vec(&head_b).unwrap();
        use md5::Digest;
        let mut h = md5::Md5::new();
        h.update(&hb);
        let mut md5hex = String::with_capacity(32);
        for c in h.finalize() {
            use fmt::Write;
            write!(&mut md5hex, "{:02x}", c).unwrap();
        }
        let head_a = HeadA {
            htype: "bsr_m-1.1".into(),
            hash: md5hex,
            pulse_id: serde_json::Number::from(self.pulse),
            global_timestamp: GlobalTimestamp {
                sec: self.ts / SEC,
                ns: self.ts % SEC,
            },
            dh_compression: None,
        };
        // TODO write directly to output buffer.
        let ha = serde_json::to_vec(&head_a).unwrap();
        let hf = self.value.to_le_bytes().to_vec();
        let hp = [(self.ts / SEC).to_be_bytes(), (self.ts % SEC).to_be_bytes()].concat();
        let mut msg = ZmtpMessage { frames: vec![] };
        let fr = ZmtpFrame {
            msglen: 0,
            has_more: false,
            is_command: false,
            data: ha,
        };
        msg.frames.push(fr);
        let fr = ZmtpFrame {
            msglen: 0,
            has_more: false,
            is_command: false,
            data: hb,
        };
        msg.frames.push(fr);
        let fr = ZmtpFrame {
            msglen: 0,
            has_more: false,
            is_command: false,
            data: hf,
        };
        msg.frames.push(fr);
        let fr = ZmtpFrame {
            msglen: 0,
            has_more: false,
            is_command: false,
            data: hp,
        };
        msg.frames.push(fr);
        Ok(msg)
    }
}
