use crate::bsread::{BsreadMessage, ChannelDescDecoded, Parser};
use crate::bsread::{ChannelDesc, GlobalTimestamp, HeadA, HeadB};
use crate::channelwriter::{ChannelWriter, ChannelWriterAll};
use crate::errconv::ErrConv;
use crate::netbuf::NetBuf;
use async_channel::{Receiver, Sender};
#[allow(unused)]
use bytes::BufMut;
use err::Error;
use futures_util::{pin_mut, Future, FutureExt, Stream, StreamExt};
use log::*;
use netpod::timeunits::*;
use scylla::batch::{Batch, BatchType, Consistency};
use scylla::prepared_statement::PreparedStatement;
use scylla::{Session as ScySession, SessionBuilder};
use serde_json::Value as JsVal;
use stats::CheckEvery;
use std::collections::BTreeMap;
use std::fmt;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

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

pub fn __get_series_id(chn: &ChannelDesc) -> u64 {
    // TODO use a more stable format (with ScalarType, Shape) as hash input.
    // TODO do not depend at all on the mapping, instead look it up on demand and cache.
    use md5::Digest;
    let mut h = md5::Md5::new();
    h.update(chn.name.as_bytes());
    h.update(chn.ty.as_bytes());
    h.update(format!("{:?}", chn.shape).as_bytes());
    let f = h.finalize();
    u64::from_le_bytes(f.as_slice()[0..8].try_into().unwrap())
}

pub async fn get_series_id(_scy: &ScySession, _chn: &ChannelDescDecoded) -> Result<u64, Error> {
    error!("TODO get_series_id");
    err::todoval()
}

pub struct CommonQueries {
    pub qu1: PreparedStatement,
    pub qu2: PreparedStatement,
    pub qu_insert_ts_msp: PreparedStatement,
    pub qu_insert_scalar_u16: PreparedStatement,
    pub qu_insert_scalar_u32: PreparedStatement,
    pub qu_insert_scalar_i16: PreparedStatement,
    pub qu_insert_scalar_i32: PreparedStatement,
    pub qu_insert_scalar_f32: PreparedStatement,
    pub qu_insert_scalar_f64: PreparedStatement,
    pub qu_insert_array_u16: PreparedStatement,
    pub qu_insert_array_i16: PreparedStatement,
    pub qu_insert_array_i32: PreparedStatement,
    pub qu_insert_array_f32: PreparedStatement,
    pub qu_insert_array_f64: PreparedStatement,
    pub qu_insert_array_bool: PreparedStatement,
}

#[derive(Clone)]
pub struct ZmtpClientOpts {
    pub backend: String,
    pub scylla: Vec<String>,
    pub sources: Vec<String>,
    pub do_pulse_id: bool,
    pub rcvbuf: Option<usize>,
    pub array_truncate: Option<usize>,
    pub process_channel_count_limit: Option<usize>,
    pub skip_insert: bool,
}

struct ClientRun {
    #[allow(unused)]
    client: Pin<Box<BsreadClient>>,
    fut: Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>,
}

impl ClientRun {
    fn new(client: BsreadClient) -> Self {
        let mut client = Box::pin(client);
        let client2 = unsafe { &mut *(&mut client as &mut _ as *mut _) } as &mut BsreadClient;
        let fut = client2.run();
        let fut = Box::pin(fut) as _;
        Self { client, fut }
    }
}

impl Future for ClientRun {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        self.fut.poll_unpin(cx)
    }
}

struct BsreadClient {
    opts: ZmtpClientOpts,
    source_addr: String,
    do_pulse_id: bool,
    rcvbuf: Option<usize>,
    tmp_vals_pulse_map: Vec<(i64, i32, i64, i32)>,
    scy: Arc<ScySession>,
    channel_writers: BTreeMap<u64, Box<dyn ChannelWriter + Send>>,
    common_queries: Arc<CommonQueries>,
    print_stats: CheckEvery,
    parser: Parser,
}

impl BsreadClient {
    pub async fn new(
        opts: ZmtpClientOpts,
        source_addr: String,
        scy: Arc<ScySession>,
        common_queries: Arc<CommonQueries>,
    ) -> Result<Self, Error> {
        let ret = Self {
            source_addr,
            do_pulse_id: opts.do_pulse_id,
            rcvbuf: opts.rcvbuf,
            opts,
            tmp_vals_pulse_map: vec![],
            scy,
            channel_writers: Default::default(),
            common_queries,
            print_stats: CheckEvery::new(Duration::from_millis(2000)),
            parser: Parser::new(),
        };
        Ok(ret)
    }

    pub async fn run(&mut self) -> Result<(), Error> {
        let mut conn = tokio::net::TcpStream::connect(&self.source_addr).await?;
        if let Some(v) = self.rcvbuf {
            crate::linuxhelper::set_rcv_sock_opts(&mut conn, v as u32)?;
        }
        let mut zmtp = Zmtp::new(conn, SocketType::PULL);
        let mut i1 = 0u64;
        let mut msgc = 0u64;
        let mut dh_md5_last = String::new();
        let mut frame_diff_count = 0u64;
        let mut hash_mismatch_count = 0u64;
        let mut head_b = HeadB::empty();
        let mut bytes_payload = 0u64;
        let mut rows_inserted = 0u32;
        let mut time_spent_inserting = Duration::from_millis(0);
        let mut series_ids = Vec::new();
        let mut msg_dt_ema = stats::EMA::with_k(0.01);
        let mut msg_ts_last = Instant::now();
        while let Some(item) = zmtp.next().await {
            let tsnow = Instant::now();
            match item {
                Ok(ev) => match ev {
                    ZmtpEvent::ZmtpCommand(_) => (),
                    ZmtpEvent::ZmtpMessage(msg) => {
                        msgc += 1;
                        {
                            let dt = tsnow.duration_since(msg_ts_last);
                            msg_dt_ema.update(dt.as_secs_f32());
                            msg_ts_last = tsnow;
                        }
                        match self.parser.parse_zmtp_message(&msg) {
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
                                        series_ids.clear();
                                        head_b = bm.head_b.clone();
                                        if dh_md5_last.is_empty() {
                                            info!("data header hash {}", bm.head_b_md5);
                                            dh_md5_last = bm.head_b_md5.clone();
                                            let scy = self.scy.clone();
                                            for chn in &head_b.channels {
                                                info!("Setup writer for {}", chn.name);
                                                let cd: ChannelDescDecoded = chn.try_into()?;
                                                match self.setup_channel_writers(&scy, &cd).await {
                                                    Ok(_) => {}
                                                    Err(e) => {
                                                        warn!("can not set up writer for {}  {e:?}", chn.name);
                                                    }
                                                }
                                            }
                                        } else {
                                            error!("TODO  changed data header hash {}", bm.head_b_md5);
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
                                // TODO limit warn rate
                                if pulse != 0 && (pulse < 14781000000 || pulse > 16000000000) {
                                    // TODO limit log rate
                                    warn!("Bad pulse {}  for {}", pulse, self.source_addr);
                                }
                                for i1 in 0..head_b
                                    .channels
                                    .len()
                                    .min(self.opts.process_channel_count_limit.unwrap_or(4000))
                                {
                                    // TODO skip decoding if header unchanged.
                                    let chn = &head_b.channels[i1];
                                    let chd: ChannelDescDecoded = chn.try_into()?;
                                    let fr = &msg.frames[2 + 2 * i1];
                                    // TODO refactor to make correctness evident.
                                    if i1 >= series_ids.len() {
                                        series_ids.resize(head_b.channels.len(), (0u8, 0u64));
                                    }
                                    if series_ids[i1].0 == 0 {
                                        let series = get_series_id(&self.scy, &chd).await?;
                                        series_ids[i1].0 = 1;
                                        series_ids[i1].1 = series;
                                    }
                                    let series = series_ids[i1].1;
                                    if let Some(_cw) = self.channel_writers.get_mut(&series) {
                                        let _ = ts;
                                        let _ = fr;
                                        // TODO hand off item to a writer item queue.
                                        err::todo();
                                        /*let res = cw.write_msg(ts, pulse, fr)?.await?;
                                        rows_inserted += res.nrows;
                                        time_spent_inserting = time_spent_inserting + res.dt;
                                        bytes_payload += fr.data().len() as u64;*/
                                    } else {
                                        // TODO check for missing writers.
                                        warn!("no writer for {}", chn.name);
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
            let dt = self.print_stats.is_elapsed_now();
            if dt > 0. {
                let nrs = rows_inserted as f32 / dt;
                let dt_ins = time_spent_inserting.as_secs_f32() * 1e3;
                let r = bytes_payload as f32 / dt * 1e-3;
                info!("insert {nrs:.0} 1/s  dt-ins {dt_ins:4.0} ms  payload {r:8.3} kB/s");
                rows_inserted = 0;
                time_spent_inserting = Duration::from_millis(0);
                bytes_payload = 0;
                if msg_dt_ema.update_count() > 100 {
                    let ema = msg_dt_ema.ema();
                    if ema < 0.005 {
                        let emv = msg_dt_ema.emv().sqrt();
                        warn!("MSG FREQ  {}  {:9.5}  {:9.5}", self.source_addr, ema, emv);
                    }
                }
            }
        }
        Ok(())
    }

    async fn setup_channel_writers(&mut self, scy: &ScySession, cd: &ChannelDescDecoded) -> Result<(), Error> {
        let series = get_series_id(scy, cd).await?;
        let has_comp = cd.compression.is_some();
        if has_comp {
            warn!("Compression not yet supported  [{}]", cd.name);
            return Ok(());
        }
        let trunc = self.opts.array_truncate.unwrap_or(64);
        let cw = ChannelWriterAll::new(
            series,
            self.common_queries.clone(),
            self.scy.clone(),
            cd.scalar_type.clone(),
            cd.shape.clone(),
            cd.byte_order.clone(),
            trunc,
            self.opts.skip_insert,
        )?;
        let shape_dims = cd.shape.to_scylla_vec();
        self.channel_writers.insert(series, Box::new(cw));
        if !self.opts.skip_insert {
            error!("TODO use PGSQL and existing function instead.");
            err::todo();
            // TODO insert correct facility name
            self.scy
            .query(
                "insert into series_by_channel (facility, channel_name, series, scalar_type, shape_dims) values (?, ?, ?, ?, ?) if not exists",
                (&self.opts.backend, &cd.name, series as i64, cd.scalar_type.to_scylla_i32(), &shape_dims),
            )
            .await
            .err_conv()?;
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

pub async fn zmtp_client(opts: ZmtpClientOpts) -> Result<(), Error> {
    let scy = SessionBuilder::new().default_consistency(Consistency::Quorum);
    let mut scy = scy;
    for a in &opts.scylla {
        scy = scy.known_node(a);
    }
    // TODO use keyspace from configuration.
    err::todo();
    let scy = scy
        .use_keyspace("ks1", false)
        .build()
        .await
        .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
    let scy = Arc::new(scy);

    error!("TODO redo the pulse mapping");
    err::todo();
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
    let qu_insert_scalar_u16 = scy
        .prepare("insert into events_scalar_u16 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)")
        .await
        .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
    let qu_insert_scalar_u32 = scy
        .prepare("insert into events_scalar_u32 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)")
        .await
        .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
    let qu_insert_scalar_i16 = scy
        .prepare("insert into events_scalar_i16 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)")
        .await
        .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
    let qu_insert_scalar_i32 = scy
        .prepare("insert into events_scalar_i32 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)")
        .await
        .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
    let qu_insert_scalar_f32 = scy
        .prepare("insert into events_scalar_f32 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)")
        .await
        .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
    let qu_insert_scalar_f64 = scy
        .prepare("insert into events_scalar_f64 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)")
        .await
        .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
    let qu_insert_array_u16 = scy
        .prepare("insert into events_array_u16 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)")
        .await
        .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
    let qu_insert_array_i16 = scy
        .prepare("insert into events_array_i16 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)")
        .await
        .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
    let qu_insert_array_i32 = scy
        .prepare("insert into events_array_i32 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)")
        .await
        .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
    let qu_insert_array_f32 = scy
        .prepare("insert into events_array_f32 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)")
        .await
        .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
    let qu_insert_array_f64 = scy
        .prepare("insert into events_array_f64 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)")
        .await
        .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
    let qu_insert_array_bool = scy
        .prepare("insert into events_array_bool (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)")
        .await
        .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
    let common_queries = CommonQueries {
        qu1,
        qu2,
        qu_insert_ts_msp,
        qu_insert_scalar_u16,
        qu_insert_scalar_u32,
        qu_insert_scalar_i16,
        qu_insert_scalar_i32,
        qu_insert_scalar_f32,
        qu_insert_scalar_f64,
        qu_insert_array_u16,
        qu_insert_array_i16,
        qu_insert_array_i32,
        qu_insert_array_f32,
        qu_insert_array_f64,
        qu_insert_array_bool,
    };
    let common_queries = Arc::new(common_queries);
    let mut jhs = vec![];
    for source_addr in &opts.sources {
        let client = BsreadClient::new(opts.clone(), source_addr.into(), scy.clone(), common_queries.clone()).await?;
        let fut = ClientRun::new(client);
        //clients.push(fut);
        let jh = tokio::spawn(fut);
        jhs.push(jh);
    }
    futures_util::future::join_all(jhs).await;
    Ok(())
}

pub struct BsreadDumper {
    source_addr: String,
    parser: Parser,
}

impl BsreadDumper {
    pub fn new(source_addr: String) -> Self {
        Self {
            source_addr,
            parser: Parser::new(),
        }
    }

    pub async fn run(&mut self) -> Result<(), Error> {
        let src = if self.source_addr.starts_with("tcp://") {
            self.source_addr[6..].into()
        } else {
            self.source_addr.clone()
        };
        let conn = tokio::net::TcpStream::connect(&src).await?;
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
                        match self.parser.parse_zmtp_message(&msg) {
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
                                if bm.head_b_md5 != dh_md5_last {
                                    head_b = bm.head_b.clone();
                                    if dh_md5_last.is_empty() {
                                        info!("data header hash {}", bm.head_b_md5);
                                    } else {
                                        error!("changed data header hash {}  mh {}", bm.head_b_md5, bm.head_a.hash);
                                    }
                                    dh_md5_last = bm.head_b_md5.clone();
                                }
                                if msg.frames.len() < 2 + 2 * head_b.channels.len() {
                                    // TODO count always, throttle log.
                                    error!("not enough frames for data header");
                                }
                                let gts = bm.head_a.global_timestamp;
                                let ts = (gts.sec as u64) * SEC + gts.ns as u64;
                                let pulse = bm.head_a.pulse_id.as_u64().unwrap_or(0);
                                let mut bytes_payload = 0u64;
                                for i1 in 0..head_b.channels.len() {
                                    let chn = &head_b.channels[i1];
                                    let _cd: ChannelDescDecoded = chn.try_into()?;
                                    let fr = &msg.frames[2 + 2 * i1];
                                    bytes_payload += fr.data().len() as u64;
                                }
                                info!("zmtp message  ts {ts}  pulse {pulse}  bytes_payload {bytes_payload}");
                            }
                            Err(e) => {
                                for frame in &msg.frames {
                                    info!("Frame: {:?}", frame);
                                }
                                zmtp.dump_input_state();
                                zmtp.dump_conn_state();
                                error!("bsread parse error: {e:?}");
                                break;
                            }
                        }
                    }
                },
                Err(e) => {
                    error!("zmtp item error: {e:?}");
                    return Err(e);
                }
            }
            i1 += 1;
            if true && i1 > 20 {
                break;
            }
            if true && msgc > 20 {
                break;
            }
        }
        Ok(())
    }
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

    fn inpbuf_conn(&mut self, need_min: usize) -> (&mut TcpStream, ReadBuf) {
        (&mut self.conn, self.buf.read_buf_for_fill(need_min))
    }

    fn outbuf_conn(&mut self) -> (&mut TcpStream, &[u8]) {
        (&mut self.conn, self.outbuf.data())
    }

    #[allow(unused)]
    #[inline(always)]
    fn record_input_state(&mut self) {}

    #[allow(unused)]
    fn record_input_state_2(&mut self) {
        let st = self.buf.state();
        self.input_state[self.input_state_ix] = InpState::Netbuf(st.0, st.1, self.buf.cap() - st.1);
        self.input_state_ix = (1 + self.input_state_ix) % self.input_state.len();
    }

    #[allow(unused)]
    #[inline(always)]
    fn record_conn_state(&mut self) {}

    #[allow(unused)]
    fn record_conn_state_2(&mut self) {
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
            let need_min = self.conn_state.need_min();
            if self.buf.cap() < need_min {
                self.done = true;
                let e = Error::with_msg_no_trace(format!(
                    "buffer too small for need_min  {}  {}",
                    self.buf.cap(),
                    self.conn_state.need_min()
                ));
                Int::Item(Err(e))
            } else if self.buf.len() < need_min {
                self.record_input_state();
                let (w, mut rbuf) = self.inpbuf_conn(need_min);
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
