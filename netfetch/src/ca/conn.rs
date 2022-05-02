use super::proto::{CaItem, CaMsg, CaMsgTy, CaProto};
use crate::ca::proto::{CreateChan, EventAdd, HeadInfo, ReadNotify};
use err::Error;
use futures_util::{Future, FutureExt, Stream, StreamExt};
use libc::c_int;
use log::*;
use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::unix::AsyncFd;
use tokio::net::TcpStream;

#[derive(Debug)]
enum ChannelError {
    NoSuccess,
}

#[derive(Debug)]
struct EventedState {
    ts_last: Instant,
}

#[derive(Debug)]
enum MonitoringState {
    AddingEvent,
    Evented(EventedState),
    Reading,
    Read,
    Muted,
}

#[derive(Debug)]
struct CreatedState {
    cid: u32,
    sid: u32,
    ts_created: Instant,
    state: MonitoringState,
}

#[derive(Debug)]
enum ChannelState {
    Init,
    Creating { cid: u32, ts_beg: Instant },
    Created(CreatedState),
    Error(ChannelError),
}

enum CaConnState {
    Init,
    Listen,
    PeerReady,
    Done,
}

struct IdStore {
    next: u32,
}

impl IdStore {
    fn new() -> Self {
        Self { next: 0 }
    }

    fn next(&mut self) -> u32 {
        let ret = self.next;
        self.next += 1;
        ret
    }
}

pub struct CaConn {
    state: CaConnState,
    proto: CaProto,
    cid_store: IdStore,
    ioid_store: IdStore,
    subid_store: IdStore,
    channels: BTreeMap<u32, ChannelState>,
    cid_by_name: BTreeMap<String, u32>,
    cid_by_subid: BTreeMap<u32, u32>,
    name_by_cid: BTreeMap<u32, String>,
    poll_count: usize,
}

impl CaConn {
    pub fn new(tcp: TcpStream) -> Self {
        Self {
            state: CaConnState::Init,
            proto: CaProto::new(tcp),
            cid_store: IdStore::new(),
            ioid_store: IdStore::new(),
            subid_store: IdStore::new(),
            channels: BTreeMap::new(),
            cid_by_name: BTreeMap::new(),
            cid_by_subid: BTreeMap::new(),
            name_by_cid: BTreeMap::new(),
            poll_count: 0,
        }
    }

    pub fn channel_add(&mut self, channel: String) {
        let cid = self.cid_by_name(&channel);
        if self.channels.contains_key(&cid) {
        } else {
            self.channels.insert(cid, ChannelState::Init);
        }
    }

    fn cid_by_name(&mut self, name: &str) -> u32 {
        if let Some(cid) = self.cid_by_name.get(name) {
            *cid
        } else {
            let cid = self.cid_store.next();
            self.cid_by_name.insert(name.into(), cid);
            self.name_by_cid.insert(cid, name.into());
            cid
        }
    }

    fn name_by_cid(&self, cid: u32) -> Option<&str> {
        self.name_by_cid.get(&cid).map(|x| x.as_str())
    }
}

impl Stream for CaConn {
    type Item = Result<(), Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        self.poll_count += 1;
        if self.poll_count > 3000 {
            error!("TODO CaConn reached poll_count limit");
            return Ready(None);
        }
        loop {
            break match &self.state {
                CaConnState::Init => {
                    let msg = CaMsg { ty: CaMsgTy::Version };
                    self.proto.push_out(msg);
                    let msg = CaMsg {
                        ty: CaMsgTy::ClientName,
                    };
                    self.proto.push_out(msg);
                    let msg = CaMsg { ty: CaMsgTy::HostName };
                    self.proto.push_out(msg);
                    self.state = CaConnState::Listen;
                    continue;
                }
                CaConnState::Listen => match self.proto.poll_next_unpin(cx) {
                    Ready(Some(k)) => match k {
                        Ok(k) => match k {
                            CaItem::Empty => {
                                info!("CaItem::Empty");
                                Ready(Some(Ok(())))
                            }
                            CaItem::Msg(msg) => match msg.ty {
                                CaMsgTy::VersionRes(n) => {
                                    if n < 12 || n > 13 {
                                        error!("See some unexpected version {n}  channel search may not work.");
                                        Ready(Some(Ok(())))
                                    } else {
                                        info!("Received peer version {n}");
                                        self.state = CaConnState::PeerReady;
                                        continue;
                                    }
                                }
                                k => {
                                    warn!("Got some other unhandled message: {k:?}");
                                    Ready(Some(Ok(())))
                                }
                            },
                        },
                        Err(e) => {
                            error!("got error item from CaProto {e:?}");
                            Ready(Some(Ok(())))
                        }
                    },
                    Ready(None) => {
                        warn!("CaProto is done");
                        self.state = CaConnState::Done;
                        continue;
                    }
                    Pending => Pending,
                },
                CaConnState::PeerReady => {
                    // TODO unify with Listen state where protocol gets polled as well.
                    let mut msgs_tmp = vec![];
                    // TODO profile, efficient enough?
                    let keys: Vec<u32> = self.channels.keys().map(|x| *x).collect();
                    for cid in keys {
                        match self.channels.get_mut(&cid).unwrap() {
                            ChannelState::Init => {
                                let name = self
                                    .name_by_cid(cid)
                                    .ok_or_else(|| Error::with_msg_no_trace("name for cid not known"));
                                let name = match name {
                                    Ok(k) => k,
                                    Err(e) => return Ready(Some(Err(e))),
                                };
                                info!("Sending CreateChan for {}", name);
                                let msg = CaMsg {
                                    ty: CaMsgTy::CreateChan(CreateChan {
                                        cid,
                                        channel: name.into(),
                                    }),
                                };
                                msgs_tmp.push(msg);
                                // TODO handle not-found error:
                                let ch_s = self.channels.get_mut(&cid).unwrap();
                                *ch_s = ChannelState::Creating {
                                    cid,
                                    ts_beg: Instant::now(),
                                };
                            }
                            _ => {}
                        }
                    }
                    let mut do_wake_again = false;
                    if msgs_tmp.len() > 0 {
                        info!("msgs_tmp.len() {}", msgs_tmp.len());
                        do_wake_again = true;
                    }
                    // TODO be careful to not overload outgoing message queue.
                    for msg in msgs_tmp {
                        self.proto.push_out(msg);
                    }
                    let res = match self.proto.poll_next_unpin(cx) {
                        Ready(Some(Ok(k))) => {
                            match k {
                                CaItem::Msg(k) => match k.ty {
                                    CaMsgTy::SearchRes(k) => {
                                        let a = k.addr.to_be_bytes();
                                        let addr = format!("{}.{}.{}.{}:{}", a[0], a[1], a[2], a[3], k.tcp_port);
                                        info!("Search result indicates server address: {addr}");
                                    }
                                    CaMsgTy::CreateChanRes(k) => {
                                        // TODO handle cid-not-found which can also indicate peer error.
                                        let cid = k.cid;
                                        let name = self.name_by_cid(cid);
                                        info!("Channel created for {name:?} now register for events");
                                        let subid = self.subid_store.next();
                                        self.cid_by_subid.insert(subid, cid);
                                        let msg = CaMsg {
                                            ty: CaMsgTy::EventAdd(EventAdd {
                                                sid: k.sid,
                                                data_type: k.data_type,
                                                data_count: k.data_count,
                                                subid,
                                            }),
                                        };
                                        self.proto.push_out(msg);
                                        do_wake_again = true;
                                        // TODO handle not-found error:
                                        let ch_s = self.channels.get_mut(&k.cid).unwrap();
                                        *ch_s = ChannelState::Created(CreatedState {
                                            cid: k.cid,
                                            sid: k.sid,
                                            ts_created: Instant::now(),
                                            state: MonitoringState::AddingEvent,
                                        });
                                        info!(
                                            "Channel is created  cid {}  sid {}  name {}",
                                            k.cid, k.sid, self.name_by_cid[&k.cid]
                                        );
                                    }
                                    CaMsgTy::EventAddRes(k) => {
                                        // TODO handle subid-not-found which can also be peer error:
                                        let cid = *self.cid_by_subid.get(&k.subid).unwrap();
                                        // TODO get rid of the string clone when I don't want the log output any longer:
                                        let name: String = self.name_by_cid(cid).unwrap().into();
                                        // TODO handle not-found error:
                                        let ch_s = self.channels.get_mut(&cid).unwrap();
                                        match ch_s {
                                            ChannelState::Created(st) => {
                                                match st.state {
                                                    MonitoringState::AddingEvent => {
                                                        info!("Confirmation {name} is subscribed.");
                                                        // TODO get ts from faster common source:
                                                        st.state = MonitoringState::Evented(EventedState {
                                                            ts_last: Instant::now(),
                                                        });
                                                    }
                                                    MonitoringState::Evented(ref mut st) => {
                                                        // TODO get ts from faster common source:
                                                        st.ts_last = Instant::now();
                                                    }
                                                    _ => {
                                                        warn!("bad state? not always, could be late message.");
                                                    }
                                                }
                                            }
                                            _ => {
                                                warn!("unexpected state: EventAddRes while having {ch_s:?}");
                                            }
                                        }
                                    }
                                    _ => {}
                                },
                                _ => {}
                            }
                            Ready(Some(Ok(())))
                        }
                        Ready(Some(Err(e))) => {
                            error!("CaProto yields error: {e:?}");
                            Ready(Some(Err(e)))
                        }
                        Ready(None) => {
                            warn!("CaProto is done");
                            self.state = CaConnState::Done;
                            Ready(Some(Ok(())))
                        }
                        Pending => Pending,
                    };
                    if do_wake_again {
                        info!("do_wake_again");
                        cx.waker().wake_by_ref();
                    }
                    res
                }
                CaConnState::Done => Ready(None),
            };
        }
    }
}

enum FindIocState {
    Init,
    WaitWritable,
    WaitReadable,
}

struct SockBox(c_int);

impl Drop for SockBox {
    fn drop(self: &mut Self) {
        if self.0 != -1 {
            unsafe {
                libc::close(self.0);
                self.0 = -1;
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Tcp4Addr {
    pub addr: Ipv4Addr,
    pub port: u16,
}

pub struct FindIoc {
    state: FindIocState,
    channel: String,
    search_id: u32,
    sock: SockBox,
    afd: Option<AsyncFd<i32>>,
    addr: libc::sockaddr_in,
    addr_len: usize,
    deadline: Pin<Box<tokio::time::Sleep>>,
    result: Option<Tcp4Addr>,
}

// Do low-level approach first to make sure it works as specified.
impl FindIoc {
    pub fn new(channel: String) -> Self {
        let addr = unsafe { std::mem::transmute_copy(&[0u8; std::mem::size_of::<libc::sockaddr_in>()]) };
        Self {
            state: FindIocState::Init,
            channel,
            search_id: 0x12345678,
            sock: SockBox(-1),
            afd: None,
            addr: addr,
            addr_len: 0,
            deadline: Box::pin(tokio::time::sleep(Duration::from_millis(200))),
            result: None,
        }
    }

    unsafe fn create_socket(&mut self) -> Result<(), Error> {
        let ec = libc::socket(libc::AF_INET, libc::SOCK_DGRAM, 0);
        if ec == -1 {
            return Err("can not create socket".into());
        }
        let sock = SockBox(ec);
        {
            let opt: libc::c_int = 1;
            let ec = libc::setsockopt(
                sock.0,
                libc::SOL_SOCKET,
                libc::SO_BROADCAST,
                &opt as *const _ as _,
                std::mem::size_of::<libc::c_int>() as _,
            );
            if ec == -1 {
                return Err("can not enable broadcast".into());
            }
        }
        {
            let ec = libc::fcntl(sock.0, libc::F_SETFL, libc::O_NONBLOCK);
            if ec == -1 {
                return Err("can not set nonblock".into());
            }
        }
        //let ip: [u8; 4] = [172, 26, 120, 71];
        let ip: [u8; 4] = [0, 0, 0, 0];
        let addr = libc::sockaddr_in {
            sin_family: libc::AF_INET as u16,
            sin_port: 0,
            sin_addr: libc::in_addr {
                s_addr: u32::from_ne_bytes(ip),
            },
            sin_zero: [0; 8],
        };
        let addr_len = std::mem::size_of::<libc::sockaddr_in>();
        self.addr = addr.clone();
        self.addr_len = addr_len;
        let ec = libc::bind(sock.0, &addr as *const _ as _, addr_len as _);
        if ec == -1 {
            return Err("can not bind socket".into());
        }
        {
            let mut addr = libc::sockaddr_in {
                sin_family: libc::AF_INET as u16,
                sin_port: 0,
                sin_addr: libc::in_addr { s_addr: 0 },
                sin_zero: [0; 8],
            };
            let mut addr_len = std::mem::size_of::<libc::sockaddr_in>();
            let ec = libc::getsockname(sock.0, &mut addr as *mut _ as _, &mut addr_len as *mut _ as _);
            if ec == -1 {
                error!("getsockname {ec}");
                return Err("can not convert raw socket to tokio socket".into());
            } else {
                if false {
                    let ipv4 = Ipv4Addr::from(addr.sin_addr.s_addr.to_ne_bytes());
                    let tcp_port = u16::from_be(addr.sin_port);
                    info!("bound local socket to {:?} port {}", ipv4, tcp_port);
                }
            }
        }
        self.sock = sock;
        Ok(())
    }

    unsafe fn try_write(&mut self) -> Result<(), Error> {
        let sock = self.sock.0;
        let ip: [u8; 4] = [172, 26, 120, 255];
        let addr = libc::sockaddr_in {
            sin_family: libc::AF_INET as u16,
            sin_port: (5064 as u16).to_be(),
            sin_addr: libc::in_addr {
                s_addr: u32::from_ne_bytes(ip),
            },
            sin_zero: [0; 8],
        };
        let addr_len = std::mem::size_of::<libc::sockaddr_in>();
        let mut buf = vec![
            //
            0u8, 0, 0, 0, //
            0, 0, 0, 13, //
            0, 0, 0, 0, //
            0, 0, 0, 0, //
            //
            //
            0, 6, 0, 0, //
            0, 0, 0, 13, //
            0, 0, 0, 0, //
            0, 0, 0, 0,
            //
            //
        ];
        let chb = self.channel.as_bytes();
        let npadded = (chb.len() + 1 + 7) / 8 * 8;
        let npad = npadded - self.channel.len();
        buf.extend_from_slice(chb);
        buf.extend_from_slice(&vec![0u8; npad]);
        let npl = (npadded as u16).to_be_bytes();
        buf[16 + 2] = npl[0];
        buf[16 + 3] = npl[1];
        let a = self.search_id.to_be_bytes();
        for (x, y) in buf[16 + 12..16 + 16].iter_mut().zip(a.into_iter()) {
            *x = y;
        }
        let ec = libc::sendto(
            sock,
            &buf[0] as *const _ as _,
            buf.len() as _,
            0,
            &addr as *const _ as _,
            addr_len as _,
        );
        if ec == -1 {
            let errno = *libc::__errno_location();
            if errno == libc::EAGAIN {
                error!("NOT YET READY FOR SENDING...");
                return Err("socket not ready for write".into());
            } else {
                return Err("can not send".into());
            }
        }
        Ok(())
    }

    unsafe fn try_read(&mut self) -> Result<(), Error> {
        let sock = self.sock.0;
        let mut saddr_mem = [0u8; std::mem::size_of::<libc::sockaddr>()];
        let mut saddr_len: libc::socklen_t = saddr_mem.len() as _;
        let mut buf = vec![0u8; 1024];
        let ec = libc::recvfrom(
            sock,
            buf.as_mut_ptr() as _,
            buf.len() as _,
            libc::O_NONBLOCK,
            &mut saddr_mem as *mut _ as _,
            &mut saddr_len as *mut _ as _,
        );
        if ec == -1 {
            let errno = *libc::__errno_location();
            if errno == libc::EAGAIN {
                error!("try_read BUT NOT YET READY FOR READING...");
                return Err("socket not ready for read".into());
            } else {
                return Err("can not read".into());
            }
        } else if ec < 0 {
            error!("unexpected received {ec}");
        } else if ec == 0 {
            error!("received zero bytes");
        } else {
            let saddr2: libc::sockaddr_in = std::mem::transmute_copy(&saddr_mem);
            let src_addr = Ipv4Addr::from(saddr2.sin_addr.s_addr.to_ne_bytes());
            let src_port = u16::from_be(saddr2.sin_port);
            info!("received from {:?} port {}", src_addr, src_port);
            if false {
                let mut s1 = String::new();
                for i in 0..(ec as usize) {
                    s1.extend(format!(" {:02x}", buf[i]).chars());
                }
                debug!("received answer {s1}");
                debug!(
                    "received answer string {}",
                    String::from_utf8_lossy(buf[..ec as usize].into())
                );
            }
            // TODO handle if we get a too large answer.
            let mut nb = crate::netbuf::NetBuf::new(2048);
            nb.put_slice(&buf[..ec as usize])?;
            let mut msgs = vec![];
            loop {
                let n = nb.data().len();
                if n == 0 {
                    break;
                }
                if n < 16 {
                    error!("incomplete message, not enough for header");
                    break;
                }
                let hi = HeadInfo::from_netbuf(&mut nb)?;
                if nb.data().len() < hi.payload() {
                    error!("incomplete message, missing payload");
                    break;
                }
                let msg = CaMsg::from_proto_infos(&hi, nb.data())?;
                nb.adv(hi.payload())?;
                msgs.push(msg);
            }
            info!("received {} msgs", msgs.len());
            for (msg_ix, msg) in msgs.iter().enumerate() {
                match &msg.ty {
                    CaMsgTy::SearchRes(k) => {
                        if k.id != self.search_id {
                            warn!("id mismatch  {} vs {}", k.id, self.search_id);
                        }
                        if false {
                            let addr = Ipv4Addr::from(k.addr.to_be_bytes());
                            info!("Converted address: {addr:?}");
                        }
                        info!(
                            "Received: {}/{}  {:?}  {:?}  {}",
                            msg_ix,
                            msgs.len(),
                            self.channel,
                            src_addr,
                            k.tcp_port
                        );
                        if self.result.is_none() {
                            self.result = Some(Tcp4Addr {
                                addr: src_addr,
                                port: k.tcp_port,
                            });
                        } else {
                            warn!("Result already populated for {}", self.channel);
                        }
                    }
                    _ => {
                        info!("{msg:?}");
                    }
                }
            }
        }
        Ok(())
    }
}

impl Future for FindIoc {
    type Output = Result<Tcp4Addr, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        loop {
            match self.deadline.poll_unpin(cx) {
                Ready(()) => {
                    break Ready(
                        self.result
                            .clone()
                            .ok_or_else(|| Error::with_msg_no_trace(format!("can not find host for {}", self.channel))),
                    );
                }
                Pending => {}
            }
            break match &mut self.state {
                FindIocState::Init => match unsafe { Self::create_socket(&mut self) } {
                    Ok(()) => {
                        let afd = tokio::io::unix::AsyncFd::new(self.sock.0).expect("can not create AsyncFd");
                        self.afd = Some(afd);
                        self.state = FindIocState::WaitWritable;
                        continue;
                    }
                    Err(e) => {
                        error!("can not create socket {e:?}");
                        Ready(Err(e))
                    }
                },
                FindIocState::WaitWritable => match self.afd.as_mut().unwrap().poll_write_ready(cx) {
                    Ready(Ok(ref mut g)) => {
                        g.clear_ready();
                        match unsafe { Self::try_write(&mut self) } {
                            Ok(()) => {
                                self.state = FindIocState::WaitReadable;
                                continue;
                            }
                            Err(e) => Ready(Err(e)),
                        }
                    }
                    Ready(Err(e)) => Ready(Err(e.into())),
                    Pending => Pending,
                },
                FindIocState::WaitReadable => match self.afd.as_mut().unwrap().poll_read_ready(cx) {
                    Ready(Ok(ref mut g)) => {
                        g.clear_ready();
                        match unsafe { Self::try_read(&mut self) } {
                            Ok(()) => {
                                continue;
                            }
                            Err(e) => Ready(Err(e)),
                        }
                    }
                    Ready(Err(e)) => {
                        error!("WaitReadable Err");
                        Ready(Err(e.into()))
                    }
                    Pending => Pending,
                },
            };
        }
    }
}

impl std::fmt::Debug for FindIoc {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        fmt.debug_struct("FindIoc").finish()
    }
}
