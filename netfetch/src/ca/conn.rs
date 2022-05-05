use super::proto::{self, CaItem, CaMsg, CaMsgTy, CaProto, EventAddRes};
use super::store::DataStore;
use crate::bsread::ChannelDescDecoded;
use crate::ca::proto::{CreateChan, EventAdd, HeadInfo, ReadNotify};
use crate::series::{Existence, SeriesId};
use err::Error;
use futures_util::stream::FuturesUnordered;
use futures_util::{Future, FutureExt, Stream, StreamExt, TryFutureExt};
use libc::c_int;
use log::*;
use netpod::timeunits::SEC;
use netpod::{ScalarType, Shape};
use std::collections::{BTreeMap, VecDeque};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime};
use tokio::io::unix::AsyncFd;
use tokio::net::TcpStream;

const INSERT_FUTS_MAX: usize = 10;

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
    FetchSeriesId,
    AddingEvent(SeriesId),
    Evented(SeriesId, EventedState),
    // TODO we also want to read while staying subscribed:
    Reading,
    Read,
    Muted,
}

#[derive(Debug)]
struct CreatedState {
    cid: u32,
    sid: u32,
    scalar_type: ScalarType,
    shape: Shape,
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
    data_store: Arc<DataStore>,
    fut_get_series: FuturesUnordered<
        Pin<Box<dyn Future<Output = Result<(u32, u32, u16, u16, Existence<SeriesId>), Error>> + Send>>,
    >,
    value_insert_futs: FuturesUnordered<Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>>,
}

impl CaConn {
    pub fn new(tcp: TcpStream, data_store: Arc<DataStore>) -> Self {
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
            data_store,
            fut_get_series: FuturesUnordered::new(),
            value_insert_futs: FuturesUnordered::new(),
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

    fn handle_event_add_res(&mut self, ev: proto::EventAddRes) {
        // TODO handle subid-not-found which can also be peer error:
        let cid = *self.cid_by_subid.get(&ev.subid).unwrap();
        // TODO get rid of the string clone when I don't want the log output any longer:
        let name: String = self.name_by_cid(cid).unwrap().into();
        // TODO handle not-found error:
        let mut series_2 = None;
        let ch_s = self.channels.get_mut(&cid).unwrap();
        match ch_s {
            ChannelState::Created(st) => {
                match st.state {
                    MonitoringState::AddingEvent(ref series) => {
                        let series = series.clone();
                        series_2 = Some(series.clone());
                        info!("Confirmation {name} is subscribed.");
                        // TODO get ts from faster common source:
                        st.state = MonitoringState::Evented(
                            series,
                            EventedState {
                                ts_last: Instant::now(),
                            },
                        );
                    }
                    MonitoringState::Evented(ref series, ref mut st) => {
                        series_2 = Some(series.clone());
                        // TODO get ts from faster common source:
                        st.ts_last = Instant::now();
                    }
                    _ => {
                        error!("unexpected state: EventAddRes while having {ch_s:?}");
                    }
                }
            }
            _ => {
                error!("unexpected state: EventAddRes while having {ch_s:?}");
            }
        }
        {
            let series = series_2.unwrap();
            // TODO where to actually get the time from?
            let ts = SystemTime::now();
            let epoch = ts.duration_since(std::time::UNIX_EPOCH).unwrap();
            // TODO decide on better msp/lsp: random offset!
            // As long as one writer is active, the msp is arbitrary.
            let ts = epoch.as_secs() * 1000000000 + epoch.subsec_nanos() as u64;
            let ts_msp = ts / (30 * SEC) * (30 * SEC);
            let ts_lsp = ts - ts_msp;
            // TODO make sure that I only accept types I expect.
            use crate::ca::proto::CaDataScalarValue::*;
            use crate::ca::proto::CaDataValue::*;
            match ev.value {
                Scalar(v) => match v {
                    F64(val) => match ch_s {
                        ChannelState::Created(st) => match st.shape {
                            Shape::Scalar => match st.scalar_type {
                                ScalarType::F64 => self.insert_scalar_f64(series, ts_msp, ts_lsp, val),
                                _ => {
                                    error!("unexpected value type");
                                }
                            },
                            _ => {
                                error!("unexpected value shape");
                            }
                        },
                        _ => {
                            error!("got value but channel not created");
                        }
                    },
                    _ => {}
                },
                Array => {}
            }
        }
    }

    fn insert_scalar_f64(&mut self, series: SeriesId, ts_msp: u64, ts_lsp: u64, val: f64) {
        let pulse = 0 as u64;
        let y = unsafe { &*(self as *const CaConn) };
        let fut1 = y
            .data_store
            .scy
            .execute(&y.data_store.qu_insert_ts_msp, (series.id() as i64, ts_msp as i64))
            .map(|_| Ok::<_, Error>(()))
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")));
        let fut2 = y
            .data_store
            .scy
            .execute(
                &y.data_store.qu_insert_scalar_f64,
                (series.id() as i64, ts_msp as i64, ts_lsp as i64, pulse as i64, val),
            )
            .map(|_| Ok::<_, Error>(()))
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")));
        let fut = fut1.and_then(move |_a| fut2);
        if self.value_insert_futs.len() > INSERT_FUTS_MAX {
            warn!("can not keep up");
        } else {
            self.value_insert_futs.push(Box::pin(fut) as _);
        }
    }
}

impl Stream for CaConn {
    type Item = Result<(), Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        self.poll_count += 1;
        if false && self.poll_count > 3000 {
            error!("TODO CaConn reached poll_count limit");
            return Ready(None);
        }
        loop {
            while self.value_insert_futs.len() > 0 {
                match self.fut_get_series.poll_next_unpin(cx) {
                    Pending => break,
                    _ => {}
                }
            }
            while self.fut_get_series.len() > 0 {
                match self.fut_get_series.poll_next_unpin(cx) {
                    Ready(Some(Ok(k))) => {
                        info!("Have SeriesId {k:?}");
                        let cid = k.0;
                        let sid = k.1;
                        let data_type = k.2;
                        let data_count = k.3;
                        let series = match k.4 {
                            Existence::Created(k) => k,
                            Existence::Existing(k) => k,
                        };
                        let subid = self.subid_store.next();
                        self.cid_by_subid.insert(subid, cid);
                        let name = self.name_by_cid(cid).unwrap().to_string();
                        let msg = CaMsg {
                            ty: CaMsgTy::EventAdd(EventAdd {
                                sid,
                                data_type,
                                data_count,
                                subid,
                            }),
                        };
                        self.proto.push_out(msg);
                        // TODO handle not-found error:
                        let ch_s = self.channels.get_mut(&cid).unwrap();
                        *ch_s = ChannelState::Created(CreatedState {
                            cid,
                            sid,
                            // TODO handle error better! Transition channel to Error state?
                            scalar_type: ScalarType::from_ca_id(data_type)?,
                            shape: Shape::from_ca_count(data_count)?,
                            ts_created: Instant::now(),
                            state: MonitoringState::AddingEvent(series),
                        });
                        let scalar_type = ScalarType::from_ca_id(data_type)?;
                        let shape = Shape::from_ca_count(data_count)?;
                        let _cd = ChannelDescDecoded {
                            name: name.to_string(),
                            scalar_type,
                            shape,
                            agg_kind: netpod::AggKind::Plain,
                            // TODO these play no role in series id:
                            byte_order: netpod::ByteOrder::LE,
                            compression: None,
                        };
                        cx.waker().wake_by_ref();
                    }
                    Ready(Some(Err(e))) => error!("series error: {e:?}"),
                    Ready(None) => {}
                    Pending => break,
                }
            }
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
                                CaItem::Msg(k) => {
                                    match k.ty {
                                        CaMsgTy::SearchRes(k) => {
                                            let a = k.addr.to_be_bytes();
                                            let addr = format!("{}.{}.{}.{}:{}", a[0], a[1], a[2], a[3], k.tcp_port);
                                            info!("Search result indicates server address: {addr}");
                                        }
                                        CaMsgTy::CreateChanRes(k) => {
                                            // TODO handle cid-not-found which can also indicate peer error.
                                            let cid = k.cid;
                                            let sid = k.sid;
                                            // TODO handle error:
                                            let name = self.name_by_cid(cid).unwrap().to_string();
                                            info!("CreateChanRes {name:?}");
                                            let scalar_type = ScalarType::from_ca_id(k.data_type)?;
                                            let shape = Shape::from_ca_count(k.data_count)?;
                                            // TODO handle not-found error:
                                            let ch_s = self.channels.get_mut(&cid).unwrap();
                                            *ch_s = ChannelState::Created(CreatedState {
                                                cid,
                                                sid,
                                                scalar_type: scalar_type.clone(),
                                                shape: shape.clone(),
                                                ts_created: Instant::now(),
                                                state: MonitoringState::FetchSeriesId,
                                            });
                                            // TODO handle error in different way. Should most likely not abort.
                                            let cd = ChannelDescDecoded {
                                                name: name.to_string(),
                                                scalar_type,
                                                shape,
                                                agg_kind: netpod::AggKind::Plain,
                                                // TODO these play no role in series id:
                                                byte_order: netpod::ByteOrder::LE,
                                                compression: None,
                                            };
                                            let y = unsafe { &*(&self as &Self as *const CaConn) };
                                            let fut =
                                                y.data_store.chan_reg.get_series_id(cd).map_ok(move |series| {
                                                    (cid, k.sid, k.data_type, k.data_count, series)
                                                });
                                            // TODO throttle execution rate:
                                            self.fut_get_series.push(Box::pin(fut) as _);
                                            do_wake_again = true;
                                        }
                                        CaMsgTy::EventAddRes(k) => Self::handle_event_add_res(&mut self, k),
                                        _ => {}
                                    }
                                }
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

static BATCH_ID: AtomicUsize = AtomicUsize::new(0);
static SEARCH_ID2: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct BatchId(u32);

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct SearchId(u32);

struct SearchBatch {
    id: BatchId,
    ts_beg: Instant,
    tgts: VecDeque<usize>,
    channels: Vec<String>,
    sids: Vec<SearchId>,
    done: Vec<SearchId>,
}

#[derive(Debug)]
pub struct FindIocRes {
    pub src: SocketAddrV4,
    pub channel: String,
    pub addr: Option<SocketAddrV4>,
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
}

impl FindIocStream {
    pub fn new(tgts: Vec<SocketAddrV4>) -> Self {
        info!("FindIocStream  tgts {tgts:?}");
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
            in_flight_max: 10,
            channels_per_batch: 10,
            batch_run_max: Duration::from_millis(1500),
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
        //info!("sendto {ip:?}  {}  n {}", port, buf.len());
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
            trace!(
                "received from  src_addr {:?}  src_port {}  ec {}",
                src_addr,
                src_port,
                ec
            );
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
            //info!("received {} msgs  {:?}", msgs.len(), msgs);
            let mut res = vec![];
            for msg in msgs.iter() {
                match &msg.ty {
                    CaMsgTy::SearchRes(k) => {
                        let addr = SocketAddrV4::new(src_addr, k.tcp_port);
                        res.push((SearchId(k.id), addr));
                    }
                    _ => {}
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
        let bid = BATCH_ID.fetch_add(1, Ordering::AcqRel);
        let bid = BatchId(bid as u32);
        //info!("create_in_flight  {bid:?}");
        let mut sids = vec![];
        let mut chs = vec![];
        while chs.len() < self.channels_per_batch && self.channels_input.len() > 0 {
            let sid = SEARCH_ID2.fetch_add(1, Ordering::AcqRel);
            let sid = SearchId(sid as u32);
            self.bid_by_sid.insert(sid.clone(), bid.clone());
            sids.push(sid);
            chs.push(self.channels_input.pop_front().unwrap());
        }
        let batch = SearchBatch {
            id: bid.clone(),
            ts_beg: Instant::now(),
            channels: chs,
            tgts: self.tgts.iter().enumerate().map(|x| x.0).collect(),
            sids,
            done: vec![],
        };
        self.in_flight.insert(bid.clone(), batch);
        self.batch_send_queue.push_back(bid);
    }

    fn handle_result(&mut self, src: SocketAddrV4, res: Vec<(SearchId, SocketAddrV4)>) {
        let mut sids_remove = vec![];
        for (sid, addr) in res {
            self.sids_done.insert(sid.clone(), ());
            match self.bid_by_sid.get(&sid) {
                Some(bid) => {
                    sids_remove.push(sid.clone());
                    match self.in_flight.get_mut(bid) {
                        Some(batch) => {
                            for (i2, s2) in batch.sids.iter().enumerate() {
                                if s2 == &sid {
                                    match batch.channels.get(i2) {
                                        Some(ch) => {
                                            let res = FindIocRes {
                                                channel: ch.into(),
                                                addr: Some(addr),
                                                src: src.clone(),
                                            };
                                            self.out_queue.push_back(res);
                                        }
                                        None => {
                                            error!("no matching channel for {sid:?}");
                                        }
                                    }
                                }
                            }
                            // Book keeping:
                            batch.done.push(sid);
                            let mut all_done = true;
                            if batch.done.len() >= batch.sids.len() {
                                for s1 in &batch.sids {
                                    if !batch.done.contains(s1) {
                                        all_done = false;
                                        break;
                                    }
                                }
                            } else {
                                all_done = false;
                            }
                            if all_done {
                                //info!("all searches done for {bid:?}");
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
        let now = Instant::now();
        let mut bids = vec![];
        let mut sids = vec![];
        let mut chns = vec![];
        for (bid, batch) in &mut self.in_flight {
            if now.duration_since(batch.ts_beg) > self.batch_run_max {
                for (i2, sid) in batch.sids.iter().enumerate() {
                    if batch.done.contains(sid) == false {
                        warn!("Timeout: {bid:?} {}", batch.channels[i2]);
                    }
                    sids.push(sid.clone());
                    chns.push(batch.channels[i2].clone());
                }
                bids.push(bid.clone());
            }
        }
        for (sid, ch) in sids.into_iter().zip(chns) {
            let res = FindIocRes {
                src: SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 0),
                channel: ch,
                addr: None,
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
                                    // TODO count events
                                } else {
                                    info!("Batch {bid:?} seems already done");
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
                        //warn!("socket seemed ready for read, but is not");
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
                            Ready(None)
                        } else {
                            Pending
                        }
                    }
                }
            };
        }
    }
}
