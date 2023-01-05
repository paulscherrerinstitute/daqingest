use crate::ca::proto::{CaMsg, CaMsgTy, HeadInfo};
use err::Error;
use futures_util::{Future, FutureExt, Stream};
use libc::c_int;
use log::*;
use std::collections::{BTreeMap, VecDeque};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::unix::AsyncFd;

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

// TODO should be able to get away with non-atomic counters.
static BATCH_ID: AtomicUsize = AtomicUsize::new(0);
static SEARCH_ID2: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct BatchId(u32);

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct SearchId(u32);

struct SearchBatch {
    ts_beg: Instant,
    tgts: VecDeque<usize>,
    channels: Vec<String>,
    sids: Vec<SearchId>,
    done: Vec<bool>,
}

#[derive(Debug)]
pub struct FindIocRes {
    pub channel: String,
    pub query_addr: Option<SocketAddrV4>,
    pub response_addr: Option<SocketAddrV4>,
    pub addr: Option<SocketAddrV4>,
    pub dt: Duration,
}

pub struct FindIocStream {
    tgts: Vec<SocketAddrV4>,
    channels_input: VecDeque<String>,
    in_flight: BTreeMap<BatchId, SearchBatch>,
    in_flight_max: usize,
    bid_by_sid: BTreeMap<SearchId, BatchId>,
    batch_send_queue: VecDeque<BatchId>,
    sock: SockBox,
    afd: AsyncFd<i32>,
    buf1: Vec<u8>,
    send_addr: SocketAddrV4,
    out_queue: VecDeque<FindIocRes>,
    ping: Pin<Box<tokio::time::Sleep>>,
    channels_per_batch: usize,
    batch_run_max: Duration,
    bids_all_done: BTreeMap<BatchId, ()>,
    bids_timed_out: BTreeMap<BatchId, ()>,
    sids_done: BTreeMap<SearchId, ()>,
    result_for_done_sid_count: u64,
    sleeper: Pin<Box<dyn Future<Output = ()> + Send>>,
}

impl FindIocStream {
    pub fn new(tgts: Vec<SocketAddrV4>, batch_run_max: Duration, in_flight_max: usize, batch_size: usize) -> Self {
        let sock = unsafe { Self::create_socket() }.unwrap();
        let afd = AsyncFd::new(sock.0).unwrap();
        Self {
            tgts,
            channels_input: VecDeque::new(),
            in_flight: BTreeMap::new(),
            bid_by_sid: BTreeMap::new(),
            batch_send_queue: VecDeque::new(),
            sock,
            afd,
            buf1: vec![0; 1024],
            send_addr: SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 5064),
            out_queue: VecDeque::new(),
            ping: Box::pin(tokio::time::sleep(Duration::from_millis(200))),
            bids_all_done: BTreeMap::new(),
            bids_timed_out: BTreeMap::new(),
            sids_done: BTreeMap::new(),
            result_for_done_sid_count: 0,
            in_flight_max,
            channels_per_batch: batch_size,
            batch_run_max,
            sleeper: Box::pin(tokio::time::sleep(Duration::from_millis(500))),
        }
    }

    pub fn quick_state(&self) -> String {
        format!(
            "channels_input {}  in_flight {}  bid_by_sid {}  out_queue {}  result_for_done_sid_count {}  bids_timed_out {}",
            self.channels_input.len(),
            self.in_flight.len(),
            self.bid_by_sid.len(),
            self.out_queue.len(),
            self.result_for_done_sid_count,
            self.bids_timed_out.len()
        )
    }

    pub fn job_queue_len(&self) -> usize {
        self.channels_input.len()
    }

    pub fn push(&mut self, x: String) {
        self.channels_input.push_back(x);
    }

    fn buf_and_batch(&mut self, bid: &BatchId) -> Option<(&mut Vec<u8>, &mut SearchBatch)> {
        match self.in_flight.get_mut(bid) {
            Some(batch) => Some((&mut self.buf1, batch)),
            None => None,
        }
    }

    unsafe fn create_socket() -> Result<SockBox, Error> {
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
        Ok(sock)
    }

    unsafe fn try_send(sock: i32, addr: &SocketAddrV4, buf: &[u8]) -> Poll<Result<(), Error>> {
        let ip = addr.ip().octets();
        let port = addr.port();
        let addr = libc::sockaddr_in {
            sin_family: libc::AF_INET as u16,
            sin_port: port.to_be(),
            sin_addr: libc::in_addr {
                s_addr: u32::from_ne_bytes(ip),
            },
            sin_zero: [0; 8],
        };
        let addr_len = std::mem::size_of::<libc::sockaddr_in>();
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
                return Poll::Pending;
            } else {
                return Poll::Ready(Err("FindIocStream can not send".into()));
            }
        }
        Poll::Ready(Ok(()))
    }

    unsafe fn try_read(sock: i32) -> Poll<Result<(SocketAddrV4, Vec<(SearchId, SocketAddrV4)>), Error>> {
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
                return Poll::Pending;
            } else {
                return Poll::Ready(Err("FindIocStream can not read".into()));
            }
        } else if ec < 0 {
            error!("unexpected received {ec}");
            Poll::Ready(Err(Error::with_msg_no_trace(format!("try_read  ec {ec}"))))
        } else if ec == 0 {
            Poll::Ready(Err(Error::with_msg_no_trace(format!("try_read  ec {ec}"))))
        } else {
            let saddr2: libc::sockaddr_in = std::mem::transmute_copy(&saddr_mem);
            let src_addr = Ipv4Addr::from(saddr2.sin_addr.s_addr.to_ne_bytes());
            let src_port = u16::from_be(saddr2.sin_port);
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
            if ec > 2048 {
                // TODO handle if we get a too large answer.
                error!("received packet too large");
                panic!();
            }
            let mut nb = crate::netbuf::NetBuf::new(2048);
            nb.put_slice(&buf[..ec as usize])?;
            let mut msgs = Vec::new();
            let mut accounted = 0;
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
                if hi.cmdid() == 0 && hi.payload() == 0 {
                } else if hi.cmdid() == 6 && hi.payload() == 8 {
                } else {
                    info!("cmdid {}  payload {}", hi.cmdid(), hi.payload());
                }
                if nb.data().len() < hi.payload() {
                    error!("incomplete message, missing payload");
                    break;
                }
                let msg = CaMsg::from_proto_infos(&hi, nb.data(), 32)?;
                nb.adv(hi.payload())?;
                msgs.push(msg);
                accounted += 16 + hi.payload();
            }
            if accounted != ec as usize {
                info!("unaccounted data  ec {}  accounted {}", ec, accounted);
            }
            if msgs.len() < 1 {
                warn!("received answer without messages");
            }
            if msgs.len() == 1 {
                warn!("received answer with single message: {msgs:?}");
            }
            let mut good = true;
            if let CaMsgTy::VersionRes(v) = msgs[0].ty {
                if v != 13 {
                    warn!("bad version: {msgs:?}");
                    good = false;
                }
            } else {
                debug!("first message is not a version: {:?}", msgs[0].ty);
                // Seems like a bug in many IOCs
                //good = false;
            }
            let mut res = Vec::new();
            if good {
                for msg in &msgs[1..] {
                    match &msg.ty {
                        CaMsgTy::SearchRes(k) => {
                            let addr = SocketAddrV4::new(src_addr, k.tcp_port);
                            res.push((SearchId(k.id), addr));
                        }
                        //CaMsgTy::VersionRes(13) => {}
                        _ => {
                            warn!("try_read: unknown message received  {:?}", msg.ty);
                        }
                    }
                }
            }
            Poll::Ready(Ok((SocketAddrV4::new(src_addr, src_port), res)))
        }
    }

    fn serialize_batch(buf: &mut Vec<u8>, batch: &SearchBatch) {
        buf.extend_from_slice(&[0, 0, 0, 0]);
        buf.extend_from_slice(&[0, 0, 0, 13]);
        buf.extend_from_slice(&[0, 0, 0, 0]);
        buf.extend_from_slice(&[0, 0, 0, 0]);
        for (sid, ch) in batch.sids.iter().zip(batch.channels.iter()) {
            let chb = ch.as_bytes();
            let npadded = (chb.len() + 1 + 7) / 8 * 8;
            let npad = npadded - chb.len();
            buf.extend_from_slice(&[0, 6]);
            buf.extend_from_slice(&(npadded as u16).to_be_bytes());
            buf.extend_from_slice(&[0, 0, 0, 13]);
            buf.extend_from_slice(&[0, 0, 0, 0]);
            buf.extend_from_slice(&sid.0.to_be_bytes());
            buf.extend_from_slice(chb);
            buf.extend_from_slice(&vec![0u8; npad]);
        }
    }

    fn create_in_flight(&mut self) {
        let bid = BatchId(BATCH_ID.fetch_add(1, Ordering::AcqRel) as u32);
        let mut sids = Vec::new();
        let mut chs = Vec::new();
        while chs.len() < self.channels_per_batch && self.channels_input.len() > 0 {
            let sid = SearchId(SEARCH_ID2.fetch_add(1, Ordering::AcqRel) as u32);
            self.bid_by_sid.insert(sid.clone(), bid.clone());
            sids.push(sid);
            chs.push(self.channels_input.pop_front().unwrap());
        }
        let n = chs.len();
        let batch = SearchBatch {
            ts_beg: Instant::now(),
            channels: chs,
            tgts: self.tgts.iter().enumerate().map(|x| x.0).collect(),
            sids,
            done: vec![false; n],
        };
        self.in_flight.insert(bid.clone(), batch);
        self.batch_send_queue.push_back(bid);
    }

    fn handle_result(&mut self, src: SocketAddrV4, res: Vec<(SearchId, SocketAddrV4)>) {
        let tsnow = Instant::now();
        let mut sids_remove = Vec::new();
        for (sid, addr) in res {
            self.sids_done.insert(sid.clone(), ());
            match self.bid_by_sid.get(&sid) {
                Some(bid) => {
                    sids_remove.push(sid.clone());
                    match self.in_flight.get_mut(bid) {
                        Some(batch) => {
                            let mut found_sid = false;
                            for (i2, s2) in batch.sids.iter().enumerate() {
                                if s2 == &sid {
                                    found_sid = true;
                                    batch.done[i2] = true;
                                    match batch.channels.get(i2) {
                                        Some(ch) => {
                                            let dt = tsnow.saturating_duration_since(batch.ts_beg);
                                            let res = FindIocRes {
                                                channel: ch.into(),
                                                // TODO associate a batch with a specific query address.
                                                query_addr: None,
                                                response_addr: Some(src.clone()),
                                                addr: Some(addr),
                                                dt,
                                            };
                                            self.out_queue.push_back(res);
                                        }
                                        None => {
                                            error!(
                                                "logic error  batch sids / channels lens:  {} vs {}",
                                                batch.sids.len(),
                                                batch.channels.len()
                                            );
                                        }
                                    }
                                }
                            }
                            if !found_sid {
                                error!("can not find sid {sid:?} in batch {bid:?}");
                            }
                            let all_done = batch.done.iter().all(|x| *x);
                            if all_done {
                                self.bids_all_done.insert(bid.clone(), ());
                                self.in_flight.remove(bid);
                            }
                        }
                        None => {
                            // TODO analyze reasons
                            error!("no batch for {bid:?}");
                        }
                    }
                }
                None => {
                    // TODO analyze reasons
                    if self.sids_done.contains_key(&sid) {
                        self.result_for_done_sid_count += 1;
                    } else {
                        error!("no bid for {sid:?}");
                    }
                }
            }
        }
        for sid in sids_remove {
            self.bid_by_sid.remove(&sid);
        }
    }

    fn clear_timed_out(&mut self) {
        let tsnow = Instant::now();
        let mut bids = Vec::new();
        let mut sids = Vec::new();
        let mut chns = Vec::new();
        let mut dts = Vec::new();
        for (bid, batch) in &mut self.in_flight {
            let dt = tsnow.saturating_duration_since(batch.ts_beg);
            if dt > self.batch_run_max {
                self.bids_timed_out.insert(bid.clone(), ());
                for (i2, sid) in batch.sids.iter().enumerate() {
                    if batch.done[i2] == false {
                        debug!("Timeout: {bid:?} {}", batch.channels[i2]);
                        sids.push(sid.clone());
                        chns.push(batch.channels[i2].clone());
                        dts.push(dt);
                    }
                }
                bids.push(bid.clone());
            }
        }
        for ((sid, ch), dt) in sids.into_iter().zip(chns).zip(dts) {
            let res = FindIocRes {
                query_addr: None,
                response_addr: None,
                channel: ch,
                addr: None,
                dt,
            };
            self.out_queue.push_back(res);
            self.bid_by_sid.remove(&sid);
        }
        for bid in bids {
            self.in_flight.remove(&bid);
        }
    }
}

impl Stream for FindIocStream {
    type Item = Result<VecDeque<FindIocRes>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        match self.ping.poll_unpin(cx) {
            Ready(_) => {
                self.ping = Box::pin(tokio::time::sleep(Duration::from_millis(200)));
                cx.waker().wake_by_ref();
            }
            Pending => {}
        }
        self.clear_timed_out();
        loop {
            let mut loop_again = false;
            if self.out_queue.is_empty() == false {
                let ret = std::mem::replace(&mut self.out_queue, VecDeque::new());
                break Ready(Some(Ok(ret)));
            }
            if !self.buf1.is_empty() {
                match self.afd.poll_write_ready(cx) {
                    Ready(Ok(mut g)) => match unsafe { Self::try_send(self.sock.0, &self.send_addr, &self.buf1) } {
                        Ready(Ok(())) => {
                            self.buf1.clear();
                            loop_again = true;
                        }
                        Ready(Err(e)) => {
                            error!("{e:?}");
                        }
                        Pending => {
                            g.clear_ready();
                            warn!("socket seemed ready for write, but is not");
                            loop_again = true;
                        }
                    },
                    Ready(Err(e)) => {
                        let e = Error::with_msg_no_trace(format!("{e:?}"));
                        error!("poll_write_ready {e:?}");
                    }
                    Pending => {}
                }
            }
            while self.buf1.is_empty() {
                match self.batch_send_queue.pop_front() {
                    Some(bid) => {
                        match self.buf_and_batch(&bid) {
                            Some((buf1, batch)) => {
                                match batch.tgts.pop_front() {
                                    Some(tgtix) => {
                                        Self::serialize_batch(buf1, batch);
                                        match self.tgts.get(tgtix) {
                                            Some(tgt) => {
                                                let tgt = tgt.clone();
                                                //info!("Serialize and queue {bid:?}");
                                                self.send_addr = tgt.clone();
                                                self.batch_send_queue.push_back(bid);
                                                loop_again = true;
                                            }
                                            None => {
                                                self.buf1.clear();
                                                self.batch_send_queue.push_back(bid);
                                                loop_again = true;
                                                error!("tgtix does not exist");
                                            }
                                        }
                                    }
                                    None => {
                                        //info!("Batch exhausted");
                                        loop_again = true;
                                    }
                                }
                            }
                            None => {
                                if self.bids_all_done.contains_key(&bid) {
                                    // Already answered from another target
                                    //trace!("bid {bid:?} from batch send queue not in flight  AND  all done");
                                } else {
                                    warn!("bid {bid:?} from batch send queue not in flight  NOT done");
                                }
                                loop_again = true;
                            }
                        }
                    }
                    None => break,
                }
            }
            while !self.channels_input.is_empty() && self.in_flight.len() < self.in_flight_max {
                self.create_in_flight();
                loop_again = true;
            }
            break match self.afd.poll_read_ready(cx) {
                Ready(Ok(mut g)) => match unsafe { Self::try_read(self.sock.0) } {
                    Ready(Ok((src, res))) => {
                        self.handle_result(src, res);
                        continue;
                    }
                    Ready(Err(e)) => {
                        error!("Error from try_read {e:?}");
                        Ready(Some(Err(e)))
                    }
                    Pending => {
                        g.clear_ready();
                        continue;
                    }
                },
                Ready(Err(e)) => {
                    let e = Error::with_msg_no_trace(format!("{e:?}"));
                    error!("poll_read_ready {e:?}");
                    Ready(Some(Err(e)))
                }
                Pending => {
                    if loop_again {
                        continue;
                    } else {
                        if self.channels_input.is_empty() && self.in_flight.is_empty() && self.out_queue.is_empty() {
                            match self.sleeper.poll_unpin(cx) {
                                Ready(_) => {
                                    self.sleeper = Box::pin(tokio::time::sleep(Duration::from_millis(500)));
                                    continue;
                                }
                                Pending => Pending,
                            }
                        } else {
                            Pending
                        }
                    }
                }
            };
        }
    }
}

impl futures_util::stream::FusedStream for FindIocStream {
    fn is_terminated(&self) -> bool {
        false
    }
}
