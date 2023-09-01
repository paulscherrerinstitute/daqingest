use crate::bsread::ChannelDesc;
use crate::bsread::GlobalTimestamp;
use crate::bsread::HeadA;
use crate::bsread::HeadB;
use crate::zmtp::ZmtpEvent;
use async_channel::Receiver;
use async_channel::Sender;
use err::thiserror;
use err::ThisError;
use futures_util::pin_mut;
use futures_util::Stream;
use futures_util::StreamExt;
use netpod::log::*;
use netpod::timeunits::SEC;
use serde_json::Value as JsVal;
use slidebuf::SlideBuf;
use std::fmt;
use std::io;
use std::mem;
use std::pin::Pin;
use std::string::FromUtf8Error;
use std::task::Context;
use std::task::Poll;
use taskrun::tokio;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;
use tokio::net::TcpStream;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("bad")]
    Bad,
    #[error("NetBuf({0})")]
    NetBuf(#[from] slidebuf::Error),
    #[error("zmtp peer is not v3.x")]
    ZmtpInitPeerNot3x,
    #[error("zmtp peer is not v3.0 or v3.1")]
    ZmtpInitPeerUnsupportedVersion,
    #[error("zmtp bad mechanism")]
    BadPeerMechanism,
    #[error("zmtp message too large {0}")]
    MsgTooLarge(usize),
    #[error("buffer too small, need-min {0} cap {1}")]
    BufferTooSmallForNeedMin(usize, usize),
    #[error("FromUtf8Error")]
    FromUtf8Error(#[from] FromUtf8Error),
    #[error("IO")]
    IO(#[from] io::Error),
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
    #[allow(unused)]
    LockScan(usize),
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
            LockScan(n) => *n,
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
    buf: SlideBuf,
    outbuf: SlideBuf,
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
    pub fn new(conn: TcpStream, socket_type: SocketType) -> Self {
        let (tx, rx) = async_channel::bounded(1);
        Self {
            done: false,
            complete: false,
            socket_type,
            conn,
            //conn_state: ConnState::LockScan(1),
            conn_state: ConnState::InitSend,
            buf: SlideBuf::new(1024 * 128),
            outbuf: SlideBuf::new(1024 * 128),
            out_enable: false,
            msglen: 0,
            has_more: false,
            is_command: false,
            peer_ver: (0, 0),
            frames: Vec::new(),
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

    fn inpbuf_conn(&mut self, need_min: usize) -> Result<(&mut TcpStream, ReadBuf), Error> {
        let buf = self.buf.available_writable_area(need_min)?;
        let buf = ReadBuf::new(buf);
        Ok((&mut self.conn, buf))
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

    pub fn dump_input_state(&self) {
        info!("---------------------------------------------------------");
        info!("INPUT STATE DUMP");
        let mut i = self.input_state_ix;
        for _ in 0..self.input_state.len() {
            info!("{i:4}  {:?}", self.input_state[i]);
            i = (1 + i) % self.input_state.len();
        }
        info!("---------------------------------------------------------");
    }

    pub fn dump_conn_state(&self) {
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
        let write: Int<Result<(), Error>> = if item_count > 0 {
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
                            Int::Item(Err(e.into()))
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
                let e = Error::BufferTooSmallForNeedMin(self.conn_state.need_min(), self.buf.cap());
                Int::Item(Err(e))
            } else if self.buf.len() < need_min {
                self.record_input_state();
                // TODO refactor error handling in this function
                let (w, mut rbuf) = match self.inpbuf_conn(need_min) {
                    Ok(x) => x,
                    Err(e) => return Some(Ready(Err(e.into()))),
                };
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
                                        Int::Item(Err(e.into()))
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
                    return Some(Ready(Err(e.into())));
                }
                (_, Item(Err(e)), _, _) => {
                    self.done = true;
                    return Some(Ready(Err(e.into())));
                }
                (_, _, Item(Err(e)), _) => {
                    self.done = true;
                    return Some(Ready(Err(e.into())));
                }
                (_, _, _, Item(Err(e))) => {
                    self.done = true;
                    return Some(Ready(Err(e.into())));
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
                    Err(Error::ZmtpInitPeerNot3x)
                } else {
                    self.conn_state = ConnState::InitRecv2;
                    Ok(None)
                }
            }
            ConnState::InitRecv2 => {
                self.buf.adv(8)?;
                let b = self.buf.read_u8()?;
                if b & 0x01 != 1 {
                    Err(Error::ZmtpInitPeerNot3x)
                } else {
                    self.conn_state = ConnState::InitRecv3;
                    Ok(None)
                }
            }
            ConnState::InitRecv3 => {
                let maj = self.buf.read_u8()?;
                if maj != 3 {
                    Err(Error::ZmtpInitPeerNot3x)
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
                    Err(Error::ZmtpInitPeerUnsupportedVersion)
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
                        return Err(Error::BadPeerMechanism);
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
                    return Err(Error::MsgTooLarge(self.msglen as usize));
                }
                Ok(None)
            }
            ConnState::ReadFrameLong => {
                self.msglen = self.buf.read_u64_be()? as usize;
                trace!("parse_item  ReadFrameLong  msglen {}", self.msglen);
                self.conn_state = ConnState::ReadFrameBody(self.msglen);
                if self.msglen > self.buf.cap() / 2 {
                    error!("msglen {} too large for this client", self.msglen);
                    return Err(Error::MsgTooLarge(self.msglen));
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
                        msglen: msglen,
                        has_more: self.has_more,
                        is_command: self.is_command,
                        data,
                    };
                    Ok(Some(ZmtpEvent::ZmtpCommand(g)))
                } else {
                    let g = ZmtpFrame {
                        msglen: msglen,
                        has_more: self.has_more,
                        is_command: self.is_command,
                        data,
                    };
                    self.frames.push(g);
                    if self.has_more {
                        Ok(None)
                    } else {
                        let g = ZmtpMessage {
                            frames: mem::replace(&mut self.frames, Vec::new()),
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
            ConnState::LockScan(n) => {
                if n > 1024 * 20 {
                    warn!("could not lock within {n} bytes");
                }
                const NBACK: usize = 2;
                let data = self.buf.data();
                let mut found_at = None;
                debug!("{}", String::from_utf8_lossy(data));
                debug!("try to lock within {} bytes", data.len());
                let needle = br##"{"dh_compression":"##;
                for (i1, b) in data.iter().enumerate() {
                    if i1 >= NBACK && *b == needle[0] {
                        let dd = &data[i1..];
                        {
                            let nn = dd.len().min(32);
                            debug!("pre {}", String::from_utf8_lossy(&dd[..nn]));
                        }
                        if dd.len() >= needle.len() {
                            if &dd[..needle.len()] == needle {
                                debug!("found at {i1}");
                                found_at = Some(i1);
                                break;
                            }
                        }
                    }
                }
                let mut locked = false;
                if let Some(nf) = found_at {
                    if nf >= NBACK {
                        if false {
                            let s1 = data[nf - NBACK..].iter().take(32).fold(String::new(), |mut a, x| {
                                use std::fmt::Write;
                                let _ = write!(a, "{:02x} ", *x);
                                a
                            });
                            debug!("BUF {s1}");
                        }
                        if data[nf - 2] == 0x01 && data[nf - 1] > 0x70 && data[nf - 1] < 0xd0 {
                            locked = true;
                        }
                    }
                }
                if locked {
                    self.conn_state = ConnState::ReadFrameFlags;
                } else {
                    self.conn_state = ConnState::LockScan(data.len() + 1);
                }
                Ok(None)
            }
        }
    }
}

#[derive(Debug)]
pub struct ZmtpMessage {
    pub frames: Vec<ZmtpFrame>,
}

impl ZmtpMessage {
    pub fn frames(&self) -> &Vec<ZmtpFrame> {
        &self.frames
    }

    pub fn emit_to_buffer(&self, out: &mut SlideBuf) -> Result<(), Error> {
        let n = self.frames.len();
        for (i, fr) in self.frames.iter().enumerate() {
            let mut flags: u8 = 2;
            if i < n - 1 {
                flags |= 1;
            }
            out.put_u8(flags)?;
            out.put_u64_be(fr.data().len() as u64)?;
            out.put_slice(fr.data())?;
        }
        Ok(())
    }
}

pub struct ZmtpFrame {
    pub msglen: usize,
    pub has_more: bool,
    pub is_command: bool,
    pub data: Vec<u8>,
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
        let mut msg = ZmtpMessage { frames: Vec::new() };
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
