use super::proto;
use super::ExtraInsertsConf;
use crate::senderpolling::SenderPolling;
use crate::throttletrace::ThrottleTrace;
use crate::timebin::ConnTimeBin;
use async_channel::Sender;
use core::fmt;
use dbpg::seriesbychannel::CanSendChannelInfoResult;
use dbpg::seriesbychannel::ChannelInfoQuery;
use dbpg::seriesbychannel::ChannelInfoResult;
use err::Error;
use futures_util::Future;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use log::*;
use netpod::timeunits::*;
use netpod::ScalarType;
use netpod::Shape;
use netpod::TS_MSP_GRID_SPACING;
use netpod::TS_MSP_GRID_UNIT;
use proto::CaItem;
use proto::CaMsg;
use proto::CaMsgTy;
use proto::CaProto;
use proto::CreateChan;
use proto::EventAdd;
use scywr::iteminsertqueue as scywriiq;
use scywriiq::ChannelInfoItem;
use scywriiq::ChannelStatus;
use scywriiq::ChannelStatusClosedReason;
use scywriiq::ChannelStatusItem;
use scywriiq::ConnectionStatus;
use scywriiq::ConnectionStatusItem;
use scywriiq::InsertItem;
use scywriiq::IvlItem;
use scywriiq::MuteItem;
use scywriiq::QueryItem;
use serde::Serialize;
use series::ChannelStatusSeriesId;
use series::SeriesId;
use stats::CaConnStats;
use stats::IntervalEma;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::net::SocketAddrV4;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::sync::atomic;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use taskrun::tokio;
use tokio::net::TcpStream;

#[allow(unused)]
macro_rules! trace2 {
    ($($arg:tt)*) => {
        if false {
            trace!($($arg)*);
        }
    };
}

#[allow(unused)]
macro_rules! trace3 {
    ($($arg:tt)*) => {
        if false {
            trace!($($arg)*);
        }
    };
}

#[allow(unused)]
macro_rules! trace4 {
    ($($arg:tt)*) => {
        if false {
            trace!($($arg)*);
        }
    };
}

#[derive(Clone, Debug, Serialize)]
pub enum ChannelConnectedInfo {
    Disconnected,
    Connecting,
    Connected,
    Error,
    Ended,
}

#[derive(Clone, Debug, Serialize)]
pub struct ChannelStateInfo {
    pub name: String,
    pub addr: SocketAddrV4,
    pub series: Option<SeriesId>,
    pub channel_connected_info: ChannelConnectedInfo,
    pub scalar_type: Option<ScalarType>,
    pub shape: Option<Shape>,
    // NOTE: this solution can yield to the same Instant serialize to different string representations.
    #[serde(skip_serializing_if = "Option::is_none", serialize_with = "ser_instant")]
    pub ts_created: Option<Instant>,
    #[serde(skip_serializing_if = "Option::is_none", serialize_with = "ser_instant")]
    pub ts_event_last: Option<Instant>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub item_recv_ivl_ema: Option<f32>,
    pub interest_score: f32,
}

fn ser_instant<S: serde::Serializer>(val: &Option<Instant>, ser: S) -> Result<S::Ok, S::Error> {
    match val {
        Some(val) => {
            let now = chrono::Utc::now();
            let tsnow = Instant::now();
            let t1 = if tsnow >= *val {
                let dur = tsnow.duration_since(*val);
                let dur2 = chrono::Duration::seconds(dur.as_secs() as i64)
                    .checked_add(&chrono::Duration::microseconds(dur.subsec_micros() as i64))
                    .unwrap();
                now.checked_sub_signed(dur2).unwrap()
            } else {
                let dur = (*val).duration_since(tsnow);
                let dur2 = chrono::Duration::seconds(dur.as_secs() as i64)
                    .checked_sub(&chrono::Duration::microseconds(dur.subsec_micros() as i64))
                    .unwrap();
                now.checked_add_signed(dur2).unwrap()
            };
            //info!("formatting {:?}", t1);
            let s = t1.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
            //info!("final string {:?}", s);
            ser.serialize_str(&s)
        }
        None => ser.serialize_none(),
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Cid(pub u32);

#[derive(Clone, Debug)]
enum ChannelError {
    #[allow(unused)]
    NoSuccess,
}

#[derive(Clone, Debug)]
struct EventedState {
    ts_last: Instant,
}

#[derive(Clone, Debug)]
enum MonitoringState {
    FetchSeriesId,
    AddingEvent(SeriesId),
    Evented(SeriesId, EventedState),
}

#[derive(Clone, Debug)]
struct CreatedState {
    cssid: ChannelStatusSeriesId,
    #[allow(unused)]
    cid: Cid,
    #[allow(unused)]
    sid: u32,
    data_type: u16,
    data_count: u16,
    scalar_type: ScalarType,
    shape: Shape,
    #[allow(unused)]
    ts_created: Instant,
    ts_alive_last: Instant,
    state: MonitoringState,
    ts_msp_last: u64,
    ts_msp_grid_last: u32,
    inserted_in_ts_msp: u64,
    insert_item_ivl_ema: IntervalEma,
    item_recv_ivl_ema: IntervalEma,
    insert_recv_ivl_last: Instant,
    insert_next_earliest: Instant,
    muted_before: u32,
    info_store_msp_last: u32,
}

#[derive(Clone, Debug)]
enum ChannelState {
    Init(ChannelStatusSeriesId),
    Creating {
        cssid: ChannelStatusSeriesId,
        cid: Cid,
        ts_beg: Instant,
    },
    FetchingSeriesId(CreatedState),
    Created(SeriesId, CreatedState),
    Error(ChannelError),
    Ended,
}

impl ChannelState {
    fn to_info(&self, name: String, addr: SocketAddrV4) -> ChannelStateInfo {
        let channel_connected_info = match self {
            ChannelState::Init(..) => ChannelConnectedInfo::Disconnected,
            ChannelState::Creating { .. } => ChannelConnectedInfo::Connecting,
            ChannelState::FetchingSeriesId(..) => ChannelConnectedInfo::Connecting,
            ChannelState::Created(..) => ChannelConnectedInfo::Connected,
            ChannelState::Error(..) => ChannelConnectedInfo::Error,
            ChannelState::Ended => ChannelConnectedInfo::Ended,
        };
        let scalar_type = match self {
            ChannelState::Created(_series, s) => Some(s.scalar_type.clone()),
            _ => None,
        };
        let shape = match self {
            ChannelState::Created(_series, s) => Some(s.shape.clone()),
            _ => None,
        };
        let ts_created = match self {
            ChannelState::Created(_series, s) => Some(s.ts_created.clone()),
            _ => None,
        };
        let ts_event_last = match self {
            ChannelState::Created(_series, s) => match &s.state {
                MonitoringState::Evented(_, s) => Some(s.ts_last),
                _ => None,
            },
            _ => None,
        };
        let item_recv_ivl_ema = match self {
            ChannelState::Created(_series, s) => {
                let ema = s.item_recv_ivl_ema.ema();
                if ema.update_count() == 0 {
                    None
                } else {
                    Some(ema.ema())
                }
            }
            _ => None,
        };
        let series = match self {
            ChannelState::Created(series, _) => Some(series.clone()),
            _ => None,
        };
        let interest_score = 1. / item_recv_ivl_ema.unwrap_or(1e10).max(1e-6).min(1e10);
        ChannelStateInfo {
            name,
            addr,
            series,
            channel_connected_info,
            scalar_type,
            shape,
            ts_created,
            ts_event_last,
            item_recv_ivl_ema,
            interest_score,
        }
    }
}

enum CaConnState {
    Unconnected,
    Connecting(
        SocketAddrV4,
        Pin<Box<dyn Future<Output = Result<Result<TcpStream, std::io::Error>, tokio::time::error::Elapsed>> + Send>>,
    ),
    Init,
    Listen,
    PeerReady,
    Wait(Pin<Box<dyn Future<Output = ()> + Send>>),
    Shutdown,
    EndOfStream,
}

impl fmt::Debug for CaConnState {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Unconnected => write!(fmt, "Unconnected"),
            Self::Connecting(arg0, _) => fmt.debug_tuple("Connecting").field(arg0).finish(),
            Self::Init => write!(fmt, "Init"),
            Self::Listen => write!(fmt, "Listen"),
            Self::PeerReady => write!(fmt, "PeerReady"),
            Self::Wait(_) => fmt.debug_tuple("Wait").finish(),
            Self::Shutdown => write!(fmt, "Shutdown"),
            Self::EndOfStream => write!(fmt, "EndOfStream"),
        }
    }
}

fn wait_fut(dt: u64) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    let fut = tokio::time::sleep(Duration::from_millis(dt));
    Box::pin(fut)
}

struct CidStore {
    next: u32,
}

impl CidStore {
    fn new() -> Self {
        Self { next: 0 }
    }

    fn next(&mut self) -> Cid {
        self.next += 1;
        let ret = self.next;
        Cid(ret)
    }
}

struct SubidStore {
    next: u32,
}

impl SubidStore {
    fn new() -> Self {
        Self { next: 0 }
    }

    fn next(&mut self) -> u32 {
        self.next += 1;
        let ret = self.next;
        ret
    }
}

fn info_store_msp_from_time(ts: SystemTime) -> u32 {
    let dt = ts.duration_since(SystemTime::UNIX_EPOCH).unwrap_or(Duration::ZERO);
    (dt.as_secs() / 60 * 60) as u32
}

#[derive(Debug)]
pub enum ConnCommandKind {
    SeriesLookupResult(Result<ChannelInfoResult, dbpg::seriesbychannel::Error>),
    ChannelAdd(String, ChannelStatusSeriesId),
    ChannelRemove(String),
    CheckHealth,
    Shutdown,
}

#[derive(Debug)]
pub struct ConnCommand {
    id: usize,
    kind: ConnCommandKind,
}

impl ConnCommand {
    pub fn series_lookup(qu: Result<ChannelInfoResult, dbpg::seriesbychannel::Error>) -> Self {
        Self {
            id: Self::make_id(),
            kind: ConnCommandKind::SeriesLookupResult(qu),
        }
    }

    pub fn channel_add(name: String, cssid: ChannelStatusSeriesId) -> Self {
        Self {
            id: Self::make_id(),
            kind: ConnCommandKind::ChannelAdd(name, cssid),
        }
    }

    pub fn channel_remove(name: String) -> Self {
        Self {
            id: Self::make_id(),
            kind: ConnCommandKind::ChannelRemove(name),
        }
    }

    pub fn check_health() -> Self {
        Self {
            id: Self::make_id(),
            kind: ConnCommandKind::CheckHealth,
        }
    }

    pub fn shutdown() -> Self {
        Self {
            id: Self::make_id(),
            kind: ConnCommandKind::Shutdown,
        }
    }

    fn make_id() -> usize {
        static ID: AtomicUsize = AtomicUsize::new(0);
        ID.fetch_add(1, atomic::Ordering::AcqRel)
    }

    pub fn id(&self) -> usize {
        self.id
    }
}

#[derive(Debug)]
pub struct CheckHealthResult {
    pub channel_statuses: BTreeMap<String, ChannelStateInfo>,
}

#[derive(Debug)]
pub enum ConnCommandResultKind {
    CheckHealth(CheckHealthResult),
}

#[derive(Debug)]
pub struct ConnCommandResult {
    pub id: usize,
    pub kind: ConnCommandResultKind,
}

impl ConnCommandResult {
    pub fn id(&self) -> usize {
        self.id
    }

    fn make_id() -> usize {
        static ID: AtomicUsize = AtomicUsize::new(0);
        ID.fetch_add(1, atomic::Ordering::AcqRel)
    }
}

#[derive(Debug)]
pub enum CaConnEventValue {
    None,
    EchoTimeout,
    ConnCommandResult(ConnCommandResult),
    QueryItem(QueryItem),
    EndOfStream,
}

#[derive(Debug)]
pub struct CaConnEvent {
    pub ts: Instant,
    pub value: CaConnEventValue,
}

struct SendSeriesLookup {
    tx: Sender<ConnCommand>,
}

impl CanSendChannelInfoResult for SendSeriesLookup {
    fn make_send(
        &self,
        item: Result<ChannelInfoResult, dbpg::seriesbychannel::Error>,
    ) -> dbpg::seriesbychannel::BoxedSend {
        let tx = self.tx.clone();
        let fut = async move { tx.send(ConnCommand::series_lookup(item)).await.map_err(|_| ()) };
        Box::pin(fut)
    }
}

pub struct CaConnOpts {
    insert_queue_max: usize,
    array_truncate: usize,
}

impl CaConnOpts {
    pub fn with_insert_queue_max(mut self, val: usize) -> Self {
        self.insert_queue_max = val;
        self
    }
}

impl Default for CaConnOpts {
    fn default() -> Self {
        Self {
            insert_queue_max: 20000,
            array_truncate: 2000,
        }
    }
}

pub struct CaConn {
    opts: CaConnOpts,
    backend: String,
    state: CaConnState,
    ticker: Pin<Box<tokio::time::Sleep>>,
    proto: Option<CaProto>,
    cid_store: CidStore,
    subid_store: SubidStore,
    channels: BTreeMap<Cid, ChannelState>,
    init_state_count: u64,
    cid_by_name: BTreeMap<String, Cid>,
    cid_by_subid: BTreeMap<u32, Cid>,
    name_by_cid: BTreeMap<Cid, String>,
    insert_item_queue: VecDeque<QueryItem>,
    remote_addr_dbg: SocketAddrV4,
    local_epics_hostname: String,
    stats: Arc<CaConnStats>,
    insert_ivl_min_mus: u64,
    conn_command_tx: async_channel::Sender<ConnCommand>,
    conn_command_rx: async_channel::Receiver<ConnCommand>,
    conn_backoff: f32,
    conn_backoff_beg: f32,
    inserts_counter: u64,
    extra_inserts_conf: ExtraInsertsConf,
    ioc_ping_last: Instant,
    ioc_ping_start: Option<Instant>,
    storage_insert_sender: SenderPolling<QueryItem>,
    cmd_res_queue: VecDeque<ConnCommandResult>,
    ca_conn_event_out_queue: VecDeque<CaConnEvent>,
    channel_info_query_queue: VecDeque<ChannelInfoQuery>,
    channel_info_query_sending: SenderPolling<ChannelInfoQuery>,
    time_binners: BTreeMap<Cid, ConnTimeBin>,
    thr_msg_poll: ThrottleTrace,
}

impl Drop for CaConn {
    fn drop(&mut self) {
        debug!("~~~~~~~~~~~~~~~   Drop CaConn {}", self.remote_addr_dbg);
    }
}

impl CaConn {
    pub fn new(
        opts: CaConnOpts,
        backend: String,
        remote_addr_dbg: SocketAddrV4,
        local_epics_hostname: String,
        storage_insert_tx: Sender<QueryItem>,
        channel_info_query_tx: Sender<ChannelInfoQuery>,
        stats: Arc<CaConnStats>,
    ) -> Self {
        let (cq_tx, cq_rx) = async_channel::bounded(32);
        Self {
            opts,
            backend,
            state: CaConnState::Unconnected,
            ticker: Self::new_self_ticker(),
            proto: None,
            cid_store: CidStore::new(),
            subid_store: SubidStore::new(),
            channels: BTreeMap::new(),
            init_state_count: 0,
            cid_by_name: BTreeMap::new(),
            cid_by_subid: BTreeMap::new(),
            name_by_cid: BTreeMap::new(),
            insert_item_queue: VecDeque::new(),
            remote_addr_dbg,
            local_epics_hostname,
            stats,
            insert_ivl_min_mus: 1000 * 6,
            conn_command_tx: cq_tx,
            conn_command_rx: cq_rx,
            conn_backoff: 0.02,
            conn_backoff_beg: 0.02,
            inserts_counter: 0,
            extra_inserts_conf: ExtraInsertsConf::new(),
            ioc_ping_last: Instant::now(),
            ioc_ping_start: None,
            storage_insert_sender: SenderPolling::new(storage_insert_tx),
            cmd_res_queue: VecDeque::new(),
            ca_conn_event_out_queue: VecDeque::new(),
            channel_info_query_queue: VecDeque::new(),
            channel_info_query_sending: SenderPolling::new(channel_info_query_tx),
            time_binners: BTreeMap::new(),
            thr_msg_poll: ThrottleTrace::new(Duration::from_millis(10000)),
        }
    }

    fn new_self_ticker() -> Pin<Box<tokio::time::Sleep>> {
        Box::pin(tokio::time::sleep(Duration::from_millis(500)))
    }

    pub fn conn_command_tx(&self) -> async_channel::Sender<ConnCommand> {
        self.conn_command_tx.clone()
    }

    fn is_shutdown(&self) -> bool {
        if let CaConnState::Shutdown = self.state {
            true
        } else {
            false
        }
    }

    fn trigger_shutdown(&mut self, channel_reason: ChannelStatusClosedReason) {
        self.state = CaConnState::Shutdown;
        self.proto = None;
        self.channel_state_on_shutdown(channel_reason);
    }

    fn cmd_check_health(&mut self) {
        match self.check_channels_alive() {
            Ok(_) => {}
            Err(e) => {
                error!("{e}");
                self.trigger_shutdown(ChannelStatusClosedReason::InternalError);
            }
        }
        // TODO return the result
        let mut channel_statuses = BTreeMap::new();
        for (k, v) in self.channels.iter() {
            let name = self
                .name_by_cid(*k)
                .map_or_else(|| format!("{k:?}"), ToString::to_string);
            let info = v.to_info(name.clone(), self.remote_addr_dbg);
            channel_statuses.insert(name, info);
        }
        let health = CheckHealthResult { channel_statuses };
        let res = ConnCommandResult {
            id: ConnCommandResult::make_id(),
            kind: ConnCommandResultKind::CheckHealth(health),
        };
        self.cmd_res_queue.push_back(res);
    }

    fn cmd_find_channel(&self, pattern: &str) {
        let res = if let Ok(re) = regex::Regex::new(&pattern) {
            self.name_by_cid
                .values()
                .filter(|x| re.is_match(x))
                .map(ToString::to_string)
                .collect()
        } else {
            Vec::new()
        };
        // TODO return the result
    }

    fn cmd_channel_state(&self, name: String) {
        let res = match self.cid_by_name.get(&name) {
            Some(cid) => match self.channels.get(cid) {
                Some(state) => Some(state.to_info(name, self.remote_addr_dbg.clone())),
                None => None,
            },
            None => None,
        };
        let msg = (self.remote_addr_dbg.clone(), res);
        if msg.1.is_some() {
            info!("Sending back {msg:?}");
        }
        // TODO return the result
    }

    fn cmd_channel_states_all(&self) {
        let res: Vec<_> = self
            .channels
            .iter()
            .map(|(cid, state)| {
                let name = self
                    .name_by_cid
                    .get(cid)
                    .map_or("--unknown--".into(), |x| x.to_string());
                state.to_info(name, self.remote_addr_dbg.clone())
            })
            .collect();
        let msg = (self.remote_addr_dbg.clone(), res);
        // TODO return the result
    }

    fn cmd_channel_add(&mut self, name: String, cssid: ChannelStatusSeriesId) {
        self.channel_add(name, cssid);
        // TODO return the result
        //self.stats.caconn_command_can_not_reply.inc();
    }

    fn cmd_channel_remove(&mut self, name: String) {
        self.channel_remove(name);
        // TODO return the result
        //self.stats.caconn_command_can_not_reply.inc();
    }

    fn cmd_shutdown(&mut self) {
        debug!("cmd_shutdown {}", self.remote_addr_dbg);
        self.trigger_shutdown(ChannelStatusClosedReason::ShutdownCommand);
    }

    fn cmd_extra_inserts_conf(&mut self, extra_inserts_conf: ExtraInsertsConf) {
        self.extra_inserts_conf = extra_inserts_conf;
        // TODO return the result
    }

    fn cmd_save_conn_info(&mut self) {
        let res = self.emit_channel_info_insert_items();
        let res = res.is_ok();
        // TODO return the result
    }

    fn handle_series_lookup_result(
        &mut self,
        res: Result<ChannelInfoResult, dbpg::seriesbychannel::Error>,
    ) -> Result<(), Error> {
        trace2!("handle_series_lookup_result {res:?}");
        match res {
            Ok(res) => {
                let series = res.series.into_inner();
                let item = QueryItem::ChannelStatus(ChannelStatusItem {
                    ts: SystemTime::now(),
                    series: series.clone(),
                    status: ChannelStatus::Opened,
                });
                self.insert_item_queue.push_back(item);
                if let Some(cid) = self.cid_by_name.get(&res.channel) {
                    if let Some(chst) = self.channels.get(cid) {
                        if let ChannelState::FetchingSeriesId(st2) = chst {
                            let cid = st2.cid.clone();
                            let sid = st2.sid;
                            let data_type = st2.data_type;
                            let data_count = st2.data_count;
                            match self.channel_to_evented(cid, sid, data_type, data_count, series) {
                                Ok(()) => {}
                                Err(e) => {
                                    error!("handle_series_lookup_result {e}");
                                }
                            }
                        } else {
                            warn!("TODO handle_series_lookup_result channel in bad state, reset");
                        }
                    } else {
                        warn!("TODO handle_series_lookup_result channel in bad state, reset");
                    }
                } else {
                    warn!("TODO handle_series_lookup_result channel in bad state, reset");
                }
            }
            Err(e) => {
                error!("handle_series_lookup_result got error {e}");
            }
        }
        Ok(())
    }

    fn handle_conn_command(&mut self, cx: &mut Context) -> Result<Poll<Option<()>>, Error> {
        // TODO if this loops for too long time, yield and make sure we get wake up again.
        use Poll::*;
        self.stats.caconn_loop3_count.inc();
        if self.is_shutdown() {
            Ok(Ready(None))
        } else {
            match self.conn_command_rx.poll_next_unpin(cx) {
                Ready(Some(a)) => {
                    trace3!("handle_conn_command received a command  {}", self.remote_addr_dbg);
                    match a.kind {
                        ConnCommandKind::ChannelAdd(name, cssid) => {
                            self.cmd_channel_add(name, cssid);
                            Ok(Ready(Some(())))
                        }
                        ConnCommandKind::ChannelRemove(name) => {
                            self.cmd_channel_remove(name);
                            Ok(Ready(Some(())))
                        }
                        ConnCommandKind::CheckHealth => {
                            self.cmd_check_health();
                            Ok(Ready(Some(())))
                        }
                        ConnCommandKind::Shutdown => {
                            self.cmd_shutdown();
                            Ok(Ready(Some(())))
                        }
                        ConnCommandKind::SeriesLookupResult(x) => match self.handle_series_lookup_result(x) {
                            Ok(()) => Ok(Ready(Some(()))),
                            Err(e) => Err(e),
                        },
                    }
                }
                Ready(None) => {
                    error!("Command queue closed");
                    Ok(Ready(None))
                }
                Pending => Ok(Pending),
            }
        }
    }

    pub fn stats(&self) -> Arc<CaConnStats> {
        self.stats.clone()
    }

    fn channel_add_expl(
        channel: String,
        cssid: ChannelStatusSeriesId,
        channels: &mut BTreeMap<Cid, ChannelState>,
        cid_by_name: &mut BTreeMap<String, Cid>,
        name_by_cid: &mut BTreeMap<Cid, String>,
        cid_store: &mut CidStore,
        init_state_count: &mut u64,
    ) {
        if cid_by_name.contains_key(&channel) {
            return;
        }
        let cid = Self::cid_by_name_expl(&channel, cid_by_name, name_by_cid, cid_store);
        if channels.contains_key(&cid) {
            error!("logic error");
        } else {
            channels.insert(cid, ChannelState::Init(cssid));
            // TODO do not count, use separate queue for those channels.
            *init_state_count += 1;
        }
    }

    pub fn channel_add(&mut self, channel: String, cssid: ChannelStatusSeriesId) {
        Self::channel_add_expl(
            channel,
            cssid,
            &mut self.channels,
            &mut self.cid_by_name,
            &mut self.name_by_cid,
            &mut self.cid_store,
            &mut self.init_state_count,
        )
    }

    fn channel_remove_expl(
        channel: String,
        channels: &mut BTreeMap<Cid, ChannelState>,
        cid_by_name: &mut BTreeMap<String, Cid>,
        name_by_cid: &mut BTreeMap<Cid, String>,
        cid_store: &mut CidStore,
        time_binners: &mut BTreeMap<Cid, ConnTimeBin>,
    ) {
        let cid = Self::cid_by_name_expl(&channel, cid_by_name, name_by_cid, cid_store);
        if channels.contains_key(&cid) {
            warn!("TODO actually cause the channel to get closed and removed  {}", channel);
        }
        {
            let a: Vec<_> = cid_by_name
                .iter()
                .filter(|x| x.1 == &cid)
                .map(|x| x.0.clone())
                .collect();
            for x in a {
                cid_by_name.remove(&x);
            }
        }
        channels.remove(&cid);
        name_by_cid.remove(&cid);
        // TODO emit in-progress before drop?
        time_binners.remove(&cid);
    }

    pub fn channel_remove(&mut self, channel: String) {
        Self::channel_remove_expl(
            channel,
            &mut self.channels,
            &mut self.cid_by_name,
            &mut self.name_by_cid,
            &mut self.cid_store,
            &mut self.time_binners,
        )
    }

    fn cid_by_name_expl(
        name: &str,
        cid_by_name: &mut BTreeMap<String, Cid>,
        name_by_cid: &mut BTreeMap<Cid, String>,
        cid_store: &mut CidStore,
    ) -> Cid {
        if let Some(cid) = cid_by_name.get(name) {
            *cid
        } else {
            let cid = cid_store.next();
            cid_by_name.insert(name.into(), cid);
            name_by_cid.insert(cid, name.into());
            cid
        }
    }

    fn name_by_cid(&self, cid: Cid) -> Option<&str> {
        self.name_by_cid.get(&cid).map(|x| x.as_str())
    }

    fn backoff_next(&mut self) -> u64 {
        let dt = (self.conn_backoff * 300. * 1e3) as u64;
        self.conn_backoff = (self.conn_backoff * 2.).tanh();
        dt
    }

    fn backoff_reset(&mut self) {
        self.conn_backoff = self.conn_backoff_beg;
    }

    fn channel_state_on_shutdown(&mut self, channel_reason: ChannelStatusClosedReason) {
        trace!("channel_state_on_shutdown  channels {}", self.channels.len());
        for (_cid, chst) in &mut self.channels {
            match chst {
                ChannelState::Init(..) => {
                    *chst = ChannelState::Ended;
                }
                ChannelState::Creating { .. } => {
                    *chst = ChannelState::Ended;
                }
                ChannelState::FetchingSeriesId(..) => {
                    *chst = ChannelState::Ended;
                }
                ChannelState::Created(series, ..) => {
                    let item = QueryItem::ChannelStatus(ChannelStatusItem {
                        ts: SystemTime::now(),
                        series: series.clone(),
                        status: ChannelStatus::Closed(channel_reason.clone()),
                    });
                    self.insert_item_queue.push_back(item);
                    *chst = ChannelState::Ended;
                }
                ChannelState::Error(..) => {
                    *chst = ChannelState::Ended;
                }
                ChannelState::Ended => {}
            }
        }
    }

    fn check_channels_alive(&mut self) -> Result<(), Error> {
        let tsnow = Instant::now();
        trace2!("check_channels_alive  {addr:?}", addr = &self.remote_addr_dbg);
        if let Some(started) = self.ioc_ping_start {
            if started.elapsed() >= Duration::from_millis(4000) {
                self.stats.pong_timeout().inc();
                self.ioc_ping_start = None;
                warn!("pong timeout {addr:?}", addr = self.remote_addr_dbg);
                let item = CaConnEvent {
                    ts: tsnow,
                    value: CaConnEventValue::EchoTimeout,
                };
                self.ca_conn_event_out_queue.push_back(item);
                self.trigger_shutdown(ChannelStatusClosedReason::IocTimeout);
            }
        } else {
            if self.ioc_ping_last.elapsed() > Duration::from_millis(20000) {
                if let Some(proto) = &mut self.proto {
                    self.stats.ping_start().inc();
                    self.ioc_ping_start = Some(Instant::now());
                    let msg = CaMsg { ty: CaMsgTy::Echo };
                    proto.push_out(msg);
                } else {
                    self.stats.ping_no_proto().inc();
                    warn!("can not ping {}  no proto", self.remote_addr_dbg);
                    self.trigger_shutdown(ChannelStatusClosedReason::NoProtocol);
                }
            }
        }
        let mut alive_count = 0;
        let mut not_alive_count = 0;
        for (_, st) in &self.channels {
            match st {
                ChannelState::Created(_, st) => {
                    if tsnow.duration_since(st.ts_alive_last) >= Duration::from_millis(10000) {
                        not_alive_count += 1;
                    } else {
                        alive_count += 1;
                    }
                }
                _ => {}
            }
        }
        self.stats.channel_all_count.__set(self.channels.len() as _);
        self.stats.channel_alive_count.__set(alive_count as _);
        self.stats.channel_not_alive_count.__set(not_alive_count as _);
        Ok(())
    }

    fn emit_channel_info_insert_items(&mut self) -> Result<(), Error> {
        let timenow = SystemTime::now();
        for (_, st) in &mut self.channels {
            match st {
                ChannelState::Init(_cssid) => {
                    // TODO need last-save-ts for this state.
                }
                ChannelState::Creating {
                    cid: _,
                    ts_beg: _,
                    cssid: _,
                } => {
                    // TODO need last-save-ts for this state.
                }
                ChannelState::FetchingSeriesId(..) => {
                    // TODO ?
                }
                ChannelState::Created(series, st) => {
                    // TODO if we don't wave a series id yet, dont' save? write-ampl.

                    let msp = info_store_msp_from_time(timenow.clone());
                    if msp != st.info_store_msp_last {
                        st.info_store_msp_last = msp;
                        let item = QueryItem::ChannelInfo(ChannelInfoItem {
                            ts_msp: msp,
                            series: series.clone(),
                            ivl: st.item_recv_ivl_ema.ema().ema(),
                            interest: 0.,
                            evsize: 0,
                        });
                        self.insert_item_queue.push_back(item);
                    }
                }
                ChannelState::Error(_) => {
                    // TODO need last-save-ts for this state.
                }
                ChannelState::Ended => {}
            }
        }
        Ok(())
    }

    fn channel_to_evented(
        &mut self,
        cid: Cid,
        sid: u32,
        data_type: u16,
        data_count: u16,
        series: SeriesId,
    ) -> Result<(), Error> {
        let tsnow = Instant::now();
        let name = self.name_by_cid(cid).unwrap().to_string();
        // TODO handle error better! Transition channel to Error state?
        let scalar_type = ScalarType::from_ca_id(data_type)?;
        let shape = Shape::from_ca_count(data_count)?;
        trace2!("channel_to_evented {name:?} {scalar_type:?} {shape:?}");
        self.stats.get_series_id_ok.inc();
        if series.id() == 0 {
            warn!("unexpected {series:?}");
        }
        if data_type > 6 {
            error!("data type of series unexpected: {}", data_type);
        }
        let mut tb = ConnTimeBin::empty();
        tb.setup_for(series.clone(), &scalar_type, &shape)?;
        self.time_binners.insert(cid, tb);
        let subid = self.subid_store.next();
        self.cid_by_subid.insert(subid, cid);
        // TODO convert first to CaDbrType, set to `Time`, then convert to ix:
        let data_type_asked = data_type + 14;
        let msg = CaMsg {
            ty: CaMsgTy::EventAdd(EventAdd {
                sid,
                data_type: data_type_asked,
                data_count,
                subid,
            }),
        };
        let proto = self.proto.as_mut().unwrap();
        proto.push_out(msg);
        // TODO handle not-found error:
        let ch_s = self.channels.get_mut(&cid).unwrap();
        let cssid = match ch_s {
            ChannelState::FetchingSeriesId(st2) => st2.cssid.clone(),
            _ => {
                let name = self.name_by_cid.get(&cid);
                let e = Error::with_msg_no_trace(format!("channel_to_evented  bad state  {name:?}  {ch_s:?}"));
                return Err(e);
            }
        };
        let created_state = CreatedState {
            cssid,
            cid,
            sid,
            data_type,
            data_count,
            scalar_type,
            shape,
            ts_created: tsnow,
            ts_alive_last: tsnow,
            state: MonitoringState::AddingEvent(series.clone()),
            ts_msp_last: 0,
            ts_msp_grid_last: 0,
            inserted_in_ts_msp: u64::MAX,
            insert_item_ivl_ema: IntervalEma::new(),
            item_recv_ivl_ema: IntervalEma::new(),
            insert_recv_ivl_last: tsnow,
            insert_next_earliest: tsnow,
            muted_before: 0,
            info_store_msp_last: info_store_msp_from_time(SystemTime::now()),
        };
        *ch_s = ChannelState::Created(series, created_state);
        Ok(())
    }

    fn event_add_insert(
        st: &mut CreatedState,
        series: SeriesId,
        scalar_type: ScalarType,
        shape: Shape,
        ts: u64,
        ts_local: u64,
        ev: proto::EventAddRes,
        item_queue: &mut VecDeque<QueryItem>,
        ts_msp_last: u64,
        ts_msp_grid: Option<u32>,
        stats: Arc<CaConnStats>,
    ) -> Result<(), Error> {
        // TODO decide on better msp/lsp: random offset!
        // As long as one writer is active, the msp is arbitrary.
        let (ts_msp, ts_msp_changed) = if st.inserted_in_ts_msp >= 64000 || st.ts_msp_last + HOUR <= ts {
            let div = SEC * 10;
            let ts_msp = ts / div * div;
            if ts_msp == st.ts_msp_last {
                (ts_msp, false)
            } else {
                st.ts_msp_last = ts_msp;
                st.inserted_in_ts_msp = 1;
                (ts_msp, true)
            }
        } else {
            st.inserted_in_ts_msp += 1;
            (ts_msp_last, false)
        };
        let ts_lsp = ts - ts_msp;
        let item = InsertItem {
            series: series.into(),
            ts_msp,
            ts_lsp,
            msp_bump: ts_msp_changed,
            pulse: 0,
            scalar_type,
            shape,
            val: ev.value.data.into(),
            ts_msp_grid,
            ts_local,
        };
        item_queue.push_back(QueryItem::Insert(item));
        stats.insert_item_create.inc();
        Ok(())
    }

    fn do_event_insert(
        st: &mut CreatedState,
        series: SeriesId,
        scalar_type: ScalarType,
        shape: Shape,
        ts: u64,
        ts_local: u64,
        ev: proto::EventAddRes,
        tsnow: Instant,
        item_queue: &mut VecDeque<QueryItem>,
        insert_ivl_min_mus: u64,
        stats: Arc<CaConnStats>,
        inserts_counter: &mut u64,
        extra_inserts_conf: &ExtraInsertsConf,
    ) -> Result<(), Error> {
        st.muted_before = 0;
        st.insert_item_ivl_ema.tick(tsnow);
        let em = st.insert_item_ivl_ema.ema();
        let ema = em.ema();
        let ivl_min = (insert_ivl_min_mus as f32) * 1e-6;
        let dt = (ivl_min - ema).max(0.) / em.k();
        st.insert_next_earliest = tsnow
            .checked_add(Duration::from_micros((dt * 1e6) as u64))
            .ok_or_else(|| Error::with_msg_no_trace("time overflow in next insert"))?;
        let ts_msp_last = st.ts_msp_last;
        // TODO get event timestamp from channel access field
        let ts_msp_grid = (ts / TS_MSP_GRID_UNIT / TS_MSP_GRID_SPACING * TS_MSP_GRID_SPACING) as u32;
        let ts_msp_grid = if st.ts_msp_grid_last != ts_msp_grid {
            st.ts_msp_grid_last = ts_msp_grid;
            Some(ts_msp_grid)
        } else {
            None
        };
        for (i, &(m, l)) in extra_inserts_conf.copies.iter().enumerate().rev() {
            if *inserts_counter % m == l {
                Self::event_add_insert(
                    st,
                    series.clone(),
                    scalar_type.clone(),
                    shape.clone(),
                    ts - 1 - i as u64,
                    ts_local - 1 - i as u64,
                    ev.clone(),
                    item_queue,
                    ts_msp_last,
                    ts_msp_grid,
                    stats.clone(),
                )?;
            }
        }
        Self::event_add_insert(
            st,
            series,
            scalar_type,
            shape,
            ts,
            ts_local,
            ev,
            item_queue,
            ts_msp_last,
            ts_msp_grid,
            stats,
        )?;
        *inserts_counter += 1;
        Ok(())
    }

    fn handle_event_add_res(&mut self, ev: proto::EventAddRes, tsnow: Instant) -> Result<(), Error> {
        // TODO handle subid-not-found which can also be peer error:
        let cid = *self.cid_by_subid.get(&ev.subid).unwrap();
        if false {
            let name = self.name_by_cid(cid);
            info!("event {name:?} {ev:?}");
        }
        // TODO handle not-found error:
        let mut series_2 = None;
        let ch_s = self.channels.get_mut(&cid).unwrap();
        match ch_s {
            ChannelState::Created(_series, st) => {
                st.ts_alive_last = tsnow;
                st.item_recv_ivl_ema.tick(tsnow);
                let scalar_type = st.scalar_type.clone();
                let shape = st.shape.clone();
                match st.state {
                    MonitoringState::AddingEvent(ref series) => {
                        let series = series.clone();
                        series_2 = Some(series.clone());
                        st.state = MonitoringState::Evented(series, EventedState { ts_last: tsnow });
                    }
                    MonitoringState::Evented(ref series, ref mut st) => {
                        series_2 = Some(series.clone());
                        st.ts_last = tsnow;
                    }
                    _ => {
                        error!("unexpected state: EventAddRes while having {:?}", st.state);
                    }
                }
                let series = match series_2 {
                    Some(k) => k,
                    None => {
                        error!("handle_event_add_res  but no series");
                        // TODO allow return Result
                        return Err(format!("no series id on insert").into());
                    }
                };
                let ts_local = {
                    let ts = SystemTime::now();
                    let epoch = ts.duration_since(std::time::UNIX_EPOCH).unwrap();
                    epoch.as_secs() * SEC + epoch.subsec_nanos() as u64
                };
                let ts = ev.value.ts.map_or(0, |x| x.get());
                let ts_diff = ts.abs_diff(ts_local);
                if ts_diff > SEC * 300 {
                    self.stats.ca_ts_off_4.inc();
                    //warn!("Bad time for {name}  {ts} vs {ts_local}  diff {}", ts_diff / SEC);
                    // TODO mute this channel for some time, discard the event.
                } else if ts_diff > SEC * 120 {
                    self.stats.ca_ts_off_3.inc();
                } else if ts_diff > SEC * 20 {
                    self.stats.ca_ts_off_2.inc();
                } else if ts_diff > SEC * 3 {
                    self.stats.ca_ts_off_1.inc();
                }
                if tsnow >= st.insert_next_earliest {
                    //let channel_state = self.channels.get_mut(&cid).unwrap();
                    let item_queue = &mut self.insert_item_queue;
                    let inserts_counter = &mut self.inserts_counter;
                    let extra_inserts_conf = &self.extra_inserts_conf;
                    if false {
                        if let Some(tb) = self.time_binners.get_mut(&cid) {
                            tb.push(ts, &ev.value)?;
                        } else {
                            // TODO count or report error
                        }
                    }
                    #[cfg(DISABLED)]
                    match &ev.value.data {
                        CaDataValue::Scalar(x) => match &x {
                            proto::CaDataScalarValue::F32(..) => match &scalar_type {
                                ScalarType::F32 => {}
                                _ => {
                                    error!("MISMATCH  got f32  exp {:?}", scalar_type);
                                }
                            },
                            proto::CaDataScalarValue::F64(..) => match &scalar_type {
                                ScalarType::F64 => {}
                                _ => {
                                    error!("MISMATCH  got f64  exp {:?}", scalar_type);
                                }
                            },
                            proto::CaDataScalarValue::I16(..) => match &scalar_type {
                                ScalarType::I16 => {}
                                _ => {
                                    error!("MISMATCH  got i16  exp {:?}", scalar_type);
                                }
                            },
                            proto::CaDataScalarValue::I32(..) => match &scalar_type {
                                ScalarType::I32 => {}
                                _ => {
                                    error!("MISMATCH  got i32  exp {:?}", scalar_type);
                                }
                            },
                            _ => {}
                        },
                        _ => {}
                    }
                    Self::do_event_insert(
                        st,
                        series,
                        scalar_type,
                        shape,
                        ts,
                        ts_local,
                        ev,
                        tsnow,
                        item_queue,
                        self.insert_ivl_min_mus,
                        self.stats.clone(),
                        inserts_counter,
                        extra_inserts_conf,
                    )?;
                } else {
                    self.stats.channel_fast_item_drop.inc();
                    if tsnow.duration_since(st.insert_recv_ivl_last) >= Duration::from_millis(10000) {
                        st.insert_recv_ivl_last = tsnow;
                        let ema = st.insert_item_ivl_ema.ema();
                        let item = IvlItem {
                            series: series.clone(),
                            ts,
                            ema: ema.ema(),
                            emd: ema.emv().sqrt(),
                        };
                        self.insert_item_queue.push_back(QueryItem::Ivl(item));
                    }
                    if false && st.muted_before == 0 {
                        let ema = st.insert_item_ivl_ema.ema();
                        let item = MuteItem {
                            series: series.clone(),
                            ts,
                            ema: ema.ema(),
                            emd: ema.emv().sqrt(),
                        };
                        self.insert_item_queue.push_back(QueryItem::Mute(item));
                    }
                    st.muted_before = 1;
                }
            }
            _ => {
                // TODO count instead of print
                error!("unexpected state: EventAddRes while having {ch_s:?}");
            }
        }
        Ok(())
    }

    /*
    Acts more like a stream? Can be:
    Pending
    Ready(no-more-work, something-was-done, error)
    */
    fn handle_conn_listen(&mut self, cx: &mut Context) -> Poll<Option<Result<(), Error>>> {
        use Poll::*;
        match self.proto.as_mut().unwrap().poll_next_unpin(cx) {
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
                                if n != 13 {
                                    warn!("Received peer version {n}");
                                }
                                self.state = CaConnState::PeerReady;
                                Ready(Some(Ok(())))
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
                    Ready(Some(Err(e.to_string().into())))
                }
            },
            Ready(None) => {
                warn!("handle_conn_listen CaProto is done  {:?}", self.remote_addr_dbg);
                self.state = CaConnState::Wait(wait_fut(self.backoff_next()));
                self.proto = None;
                Ready(None)
            }
            Pending => Pending,
        }
    }

    fn check_channels_state_init(&mut self, msgs_tmp: &mut Vec<CaMsg>) -> Result<(), Error> {
        // TODO profile, efficient enough?
        if self.init_state_count == 0 {
            return Ok(());
        }
        let keys: Vec<Cid> = self.channels.keys().map(|x| *x).collect();
        for cid in keys {
            match self.channels.get_mut(&cid).unwrap() {
                ChannelState::Init(cssid) => {
                    let cssid = cssid.clone();
                    let name = self
                        .name_by_cid(cid)
                        .ok_or_else(|| Error::with_msg_no_trace("name for cid not known"));
                    let name = match name {
                        Ok(k) => k.to_string(),
                        Err(e) => return Err(e),
                    };
                    let msg = CaMsg {
                        ty: CaMsgTy::CreateChan(CreateChan {
                            cid: cid.0,
                            channel: name.into(),
                        }),
                    };
                    msgs_tmp.push(msg);
                    // TODO handle not-found error:
                    let ch_s = self.channels.get_mut(&cid).unwrap();
                    *ch_s = ChannelState::Creating {
                        cssid,
                        cid,
                        ts_beg: Instant::now(),
                    };
                    self.init_state_count -= 1;
                }
                _ => {}
            }
        }
        Ok(())
    }

    // Can return:
    // Pending, error, work-done (pending state unknown), no-more-work-ever-again.
    fn handle_peer_ready(&mut self, cx: &mut Context) -> Poll<Option<Result<(), Error>>> {
        use Poll::*;
        let mut ts1 = Instant::now();
        // TODO unify with Listen state where protocol gets polled as well.
        let mut msgs_tmp = Vec::new();
        self.check_channels_state_init(&mut msgs_tmp)?;
        let ts2 = Instant::now();
        self.stats
            .time_check_channels_state_init
            .add((ts2.duration_since(ts1) * MS as u32).as_secs());
        ts1 = ts2;
        let mut do_wake_again = false;
        if msgs_tmp.len() > 0 {
            do_wake_again = true;
        }
        {
            let proto = self.proto.as_mut().unwrap();
            // TODO be careful to not overload outgoing message queue.
            for msg in msgs_tmp {
                proto.push_out(msg);
            }
        }
        let tsnow = Instant::now();
        let res = match self.proto.as_mut().unwrap().poll_next_unpin(cx) {
            Ready(Some(Ok(k))) => {
                match k {
                    CaItem::Msg(k) => {
                        match k.ty {
                            CaMsgTy::SearchRes(k) => {
                                let a = k.addr.to_be_bytes();
                                let addr = format!("{}.{}.{}.{}:{}", a[0], a[1], a[2], a[3], k.tcp_port);
                                trace!("Search result indicates server address: {addr}");
                                // TODO count this unexpected case.
                            }
                            CaMsgTy::CreateChanRes(k) => {
                                // TODO handle cid-not-found which can also indicate peer error.
                                let cid = Cid(k.cid);
                                let sid = k.sid;
                                // TODO handle error:
                                let name = self.name_by_cid(cid).unwrap().to_string();
                                trace3!("CreateChanRes {name:?}");
                                if k.data_type > 6 {
                                    error!("CreateChanRes with unexpected data_type {}", k.data_type);
                                }
                                let scalar_type = ScalarType::from_ca_id(k.data_type)?;
                                let shape = Shape::from_ca_count(k.data_count)?;
                                // TODO handle not-found error:
                                let ch_s = self.channels.get_mut(&cid).unwrap();
                                let cssid = match ch_s {
                                    ChannelState::Creating { cssid, .. } => cssid.clone(),
                                    _ => {
                                        let e = Error::with_msg_no_trace("handle_peer_ready  bad state");
                                        return Ready(Some(Err(e)));
                                    }
                                };
                                let created_state = CreatedState {
                                    cssid,
                                    cid,
                                    sid,
                                    data_type: k.data_type,
                                    data_count: k.data_count,
                                    scalar_type: scalar_type.clone(),
                                    shape: shape.clone(),
                                    ts_created: tsnow,
                                    ts_alive_last: tsnow,
                                    state: MonitoringState::FetchSeriesId,
                                    ts_msp_last: 0,
                                    ts_msp_grid_last: 0,
                                    inserted_in_ts_msp: u64::MAX,
                                    insert_item_ivl_ema: IntervalEma::new(),
                                    item_recv_ivl_ema: IntervalEma::new(),
                                    insert_recv_ivl_last: tsnow,
                                    insert_next_earliest: tsnow,
                                    muted_before: 0,
                                    info_store_msp_last: info_store_msp_from_time(SystemTime::now()),
                                };
                                *ch_s = ChannelState::FetchingSeriesId(created_state);
                                // TODO handle error in different way. Should most likely not abort.
                                let tx = SendSeriesLookup {
                                    tx: self.conn_command_tx.clone(),
                                };
                                let query = ChannelInfoQuery {
                                    backend: self.backend.clone(),
                                    channel: name.clone(),
                                    scalar_type: scalar_type.to_scylla_i32(),
                                    shape_dims: shape.to_scylla_vec(),
                                    tx: Box::pin(tx),
                                };
                                self.channel_info_query_queue.push_back(query);
                                do_wake_again = true;
                            }
                            CaMsgTy::EventAddRes(k) => {
                                trace4!("got EventAddRes: {k:?}");
                                self.stats.caconn_recv_data.inc();
                                let res = Self::handle_event_add_res(self, k, tsnow);
                                let ts2 = Instant::now();
                                self.stats
                                    .time_handle_event_add_res
                                    .add((ts2.duration_since(ts1) * MS as u32).as_secs());
                                ts1 = ts2;
                                let _ = ts1;
                                res?
                            }
                            CaMsgTy::Error(e) => {
                                warn!("channel access error message {e:?}");
                            }
                            CaMsgTy::AccessRightsRes(_) => {}
                            CaMsgTy::Echo => {
                                // let addr = &self.remote_addr_dbg;
                                if let Some(started) = self.ioc_ping_start {
                                    let dt = started.elapsed().as_secs_f32() * 1e3;
                                    if dt <= 10. {
                                        self.stats.pong_recv_010ms().inc();
                                    } else if dt <= 25. {
                                        self.stats.pong_recv_025ms().inc();
                                    } else if dt <= 50. {
                                        self.stats.pong_recv_050ms().inc();
                                    } else if dt <= 100. {
                                        self.stats.pong_recv_100ms().inc();
                                    } else if dt <= 200. {
                                        self.stats.pong_recv_200ms().inc();
                                    } else if dt <= 400. {
                                        self.stats.pong_recv_400ms().inc();
                                    } else {
                                        self.stats.pong_recv_slow().inc();
                                        // warn!("Received Echo  {dt:10.0}ms  {addr:?}");
                                    }
                                } else {
                                    let addr = &self.remote_addr_dbg;
                                    warn!("Received Echo even though we didn't asked for it  {addr:?}");
                                }
                                self.ioc_ping_last = Instant::now();
                                self.ioc_ping_start = None;
                            }
                            CaMsgTy::CreateChanFail(_) => {
                                // TODO handle CreateChanFail
                            }
                            _ => {
                                warn!("Received unexpected protocol message {:?}", k);
                            }
                        }
                    }
                    CaItem::Empty => {}
                }
                Ready(Some(Ok(())))
            }
            Ready(Some(Err(e))) => {
                error!("CaProto yields error: {e:?}  remote {:?}", self.remote_addr_dbg);
                self.trigger_shutdown(ChannelStatusClosedReason::ProtocolError);
                Ready(Some(Err(e)))
            }
            Ready(None) => {
                warn!("handle_peer_ready CaProto is done  {:?}", self.remote_addr_dbg);
                self.trigger_shutdown(ChannelStatusClosedReason::ProtocolDone);
                Ready(None)
            }
            Pending => Pending,
        };
        if do_wake_again {
            // TODO remove the need for this:
            cx.waker().wake_by_ref();
        }
        res.map_err(|e| Error::from(e.to_string()))
    }

    // `?` works not in here.
    fn _test_control_flow(&mut self, _cx: &mut Context) -> ControlFlow<Poll<Result<(), Error>>> {
        use ControlFlow::*;
        use Poll::*;
        let e = Error::with_msg_no_trace(format!("test"));
        //Err(e)?;
        let _ = e;
        Break(Pending)
    }

    fn handle_conn_state(&mut self, cx: &mut Context) -> Result<Poll<Option<()>>, Error> {
        use Poll::*;
        match &mut self.state {
            CaConnState::Unconnected => {
                trace4!("Unconnected");
                let addr = self.remote_addr_dbg.clone();
                trace!("create tcp connection to {:?}", (addr.ip(), addr.port()));
                let fut = tokio::time::timeout(Duration::from_millis(1000), TcpStream::connect(addr));
                self.state = CaConnState::Connecting(addr, Box::pin(fut));
                Ok(Ready(Some(())))
            }
            CaConnState::Connecting(ref addr, ref mut fut) => {
                trace4!("Connecting");
                match fut.poll_unpin(cx) {
                    Ready(connect_result) => {
                        match connect_result {
                            Ok(Ok(tcp)) => {
                                let addr = addr.clone();
                                self.insert_item_queue
                                    .push_back(QueryItem::ConnectionStatus(ConnectionStatusItem {
                                        ts: SystemTime::now(),
                                        addr,
                                        status: ConnectionStatus::Established,
                                    }));
                                self.backoff_reset();
                                let proto = CaProto::new(tcp, self.remote_addr_dbg.clone(), self.opts.array_truncate);
                                self.state = CaConnState::Init;
                                self.proto = Some(proto);
                                Ok(Ready(Some(())))
                            }
                            Ok(Err(_e)) => {
                                // TODO log with exponential backoff
                                let addr = addr.clone();
                                self.insert_item_queue
                                    .push_back(QueryItem::ConnectionStatus(ConnectionStatusItem {
                                        ts: SystemTime::now(),
                                        addr,
                                        status: ConnectionStatus::ConnectError,
                                    }));
                                let dt = self.backoff_next();
                                self.state = CaConnState::Wait(wait_fut(dt));
                                self.proto = None;
                                Ok(Ready(Some(())))
                            }
                            Err(e) => {
                                // TODO log with exponential backoff
                                trace!("timeout during connect to {addr:?} {e:?}");
                                let addr = addr.clone();
                                self.insert_item_queue
                                    .push_back(QueryItem::ConnectionStatus(ConnectionStatusItem {
                                        ts: SystemTime::now(),
                                        addr,
                                        status: ConnectionStatus::ConnectTimeout,
                                    }));
                                let dt = self.backoff_next();
                                self.state = CaConnState::Wait(wait_fut(dt));
                                self.proto = None;
                                Ok(Ready(Some(())))
                            }
                        }
                    }
                    Pending => Ok(Pending),
                }
            }
            CaConnState::Init => {
                trace4!("Init");
                let hostname = self.local_epics_hostname.clone();
                let proto = self.proto.as_mut().unwrap();
                let msg = CaMsg { ty: CaMsgTy::Version };
                proto.push_out(msg);
                let msg = CaMsg {
                    ty: CaMsgTy::ClientName,
                };
                proto.push_out(msg);
                let msg = CaMsg {
                    ty: CaMsgTy::HostName(hostname),
                };
                proto.push_out(msg);
                self.state = CaConnState::Listen;
                Ok(Ready(Some(())))
            }
            CaConnState::Listen => {
                trace4!("Listen");
                match {
                    let res = self.handle_conn_listen(cx);
                    res
                } {
                    Ready(Some(Ok(()))) => Ok(Ready(Some(()))),
                    Ready(Some(Err(e))) => Err(e),
                    Ready(None) => Ok(Ready(Some(()))),
                    Pending => Ok(Pending),
                }
            }
            CaConnState::PeerReady => {
                trace4!("PeerReady");
                let res = self.handle_peer_ready(cx);
                match res {
                    Ready(Some(Ok(()))) => Ok(Ready(Some(()))),
                    Ready(Some(Err(e))) => Err(e),
                    Ready(None) => Ok(Ready(Some(()))),
                    Pending => Ok(Pending),
                }
            }
            CaConnState::Wait(inst) => {
                trace4!("Wait");
                match inst.poll_unpin(cx) {
                    Ready(_) => {
                        self.state = CaConnState::Unconnected;
                        self.proto = None;
                        Ok(Ready(Some(())))
                    }
                    Pending => Ok(Pending),
                }
            }
            CaConnState::Shutdown => {
                trace4!("Shutdown");
                Ok(Ready(None))
            }
            CaConnState::EndOfStream => {
                trace4!("EndOfStream");
                Ok(Ready(None))
            }
        }
    }

    fn loop_inner(&mut self, cx: &mut Context) -> Result<Poll<Option<()>>, Error> {
        use Poll::*;
        let mut have_progress = false;
        for _ in 0..64 {
            self.stats.caconn_loop2_count.inc();
            if self.is_shutdown() {
                break;
            } else if self.insert_item_queue.len() >= self.opts.insert_queue_max {
                break;
            } else {
                match self.handle_conn_state(cx) {
                    Ok(x) => match x {
                        Ready(Some(())) => {
                            have_progress = true;
                            continue;
                        }
                        Ready(None) => {
                            error!("handle_conn_state yields {x:?}");
                            return Err(Error::with_msg_no_trace("logic error"));
                        }
                        Pending => return Ok(Pending),
                    },
                    Err(e) => return Err(e),
                }
            };
        }
        if have_progress {
            Ok(Ready(Some(())))
        } else {
            Ok(Ready(None))
        }
    }

    fn handle_own_ticker(mut self: Pin<&mut Self>, cx: &mut Context) -> Result<Poll<()>, Error> {
        use Poll::*;
        match self.ticker.poll_unpin(cx) {
            Ready(()) => {
                match self.as_mut().handle_own_ticker_tick(cx) {
                    Ok(_) => {
                        if !self.is_shutdown() {
                            self.ticker = Self::new_self_ticker();
                            let _ = self.ticker.poll_unpin(cx);
                            // cx.waker().wake_by_ref();
                        }
                        Ok(Pending)
                    }
                    Err(e) => {
                        error!("handle_own_ticker {e}");
                        self.trigger_shutdown(ChannelStatusClosedReason::InternalError);
                        Err(e)
                    }
                }
            }
            Pending => Ok(Pending),
        }
    }

    fn handle_own_ticker_tick(self: Pin<&mut Self>, _cx: &mut Context) -> Result<(), Error> {
        // debug!("tick  CaConn  {}", self.remote_addr_dbg);
        let this = self.get_mut();
        if false {
            for (_, tb) in this.time_binners.iter_mut() {
                let iiq = &mut this.insert_item_queue;
                tb.tick(iiq)?;
            }
        }
        Ok(())
    }

    fn queues_async_out_flushed(&self) -> bool {
        // self.channel_info_query_queue.is_empty() && self.channel_info_query_sending.is_idle()
        // TODO re-enable later
        true
    }

    fn attempt_flush_storage_queue(mut self: Pin<&mut Self>, cx: &mut Context) -> Result<Poll<Option<()>>, Error> {
        use Poll::*;
        let mut have_progress = false;
        for _ in 0..128 {
            let sd = &mut self.storage_insert_sender;
            if sd.is_idle() {
                if let Some(item) = self.insert_item_queue.pop_front() {
                    self.storage_insert_sender.send(item);
                }
            }
            if self.storage_insert_sender.is_sending() {
                match self.storage_insert_sender.poll_unpin(cx) {
                    Ready(Ok(())) => {
                        have_progress = true;
                    }
                    Ready(Err(_)) => return Err(Error::with_msg_no_trace("can not send into channel")),
                    Pending => return Ok(Pending),
                }
            }
        }
        if have_progress {
            Ok(Ready(Some(())))
        } else {
            Ok(Ready(None))
        }
    }

    fn attempt_flush_channel_info_query(mut self: Pin<&mut Self>, cx: &mut Context) -> Result<Poll<Option<()>>, Error> {
        use Poll::*;
        if self.is_shutdown() {
            Ok(Ready(None))
        } else {
            let sd = &mut self.channel_info_query_sending;
            if sd.is_idle() {
                if let Some(item) = self.channel_info_query_queue.pop_front() {
                    trace3!("send series query {item:?}");
                    let sd = &mut self.channel_info_query_sending;
                    sd.send(item);
                }
            }
            let sd = &mut self.channel_info_query_sending;
            if sd.is_sending() {
                match sd.poll_unpin(cx) {
                    Ready(Ok(())) => Ok(Ready(Some(()))),
                    Ready(Err(_)) => Err(Error::with_msg_no_trace("can not send into channel")),
                    Pending => Ok(Pending),
                }
            } else {
                Ok(Ready(None))
            }
        }
    }
}

impl Stream for CaConn {
    type Item = Result<CaConnEvent, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        self.stats.caconn_poll_count.inc();
        let poll_ts1 = Instant::now();
        self.stats.ca_conn_poll_fn_begin().inc();
        let ret = loop {
            self.stats.ca_conn_poll_loop_begin().inc();
            let qlen = self.insert_item_queue.len();
            if qlen >= self.opts.insert_queue_max * 2 / 3 {
                self.stats.insert_item_queue_pressure().inc();
            } else if qlen >= self.opts.insert_queue_max {
                self.stats.insert_item_queue_full().inc();
            }

            let mut have_pending = false;
            let mut have_progress = false;

            if let CaConnState::EndOfStream = self.state {
                break Ready(None);
            }
            if let Some(item) = self.cmd_res_queue.pop_front() {
                let item = CaConnEvent {
                    ts: Instant::now(),
                    value: CaConnEventValue::ConnCommandResult(item),
                };
                break Ready(Some(Ok(item)));
            }
            if let Some(item) = self.ca_conn_event_out_queue.pop_front() {
                break Ready(Some(Ok(item)));
            }
            // if let Some(item) = self.insert_item_queue.pop_front() {
            //     let ev = CaConnEvent {
            //         ts: Instant::now(),
            //         value: CaConnEventValue::QueryItem(item),
            //     };
            //     break Ready(Some(Ok(ev)));
            // }

            match self.as_mut().handle_own_ticker(cx) {
                Ok(Ready(())) => {
                    have_progress = true;
                }
                Ok(Pending) => {
                    have_pending = true;
                }
                Err(e) => break Ready(Some(Err(e))),
            }

            match self.as_mut().attempt_flush_storage_queue(cx) {
                Ok(Ready(Some(()))) => {
                    have_progress = true;
                }
                Ok(Ready(None)) => {}
                Ok(Pending) => {
                    have_pending = true;
                }
                Err(e) => break Ready(Some(Err(e))),
            }

            match self.as_mut().attempt_flush_channel_info_query(cx) {
                Ok(Ready(Some(()))) => {
                    have_progress = true;
                }
                Ok(Ready(None)) => {}
                Ok(Pending) => {
                    have_pending = true;
                }
                Err(e) => break Ready(Some(Err(e))),
            }

            match self.as_mut().handle_conn_command(cx) {
                Ok(Ready(Some(()))) => {
                    have_progress = true;
                }
                Ok(Ready(None)) => {}
                Ok(Pending) => {
                    have_pending = true;
                }
                Err(e) => break Ready(Some(Err(e))),
            }

            match self.loop_inner(cx) {
                Ok(Ready(Some(()))) => {
                    have_progress = true;
                }
                Ok(Ready(None)) => {}
                Ok(Pending) => {
                    have_pending = true;
                }
                Err(e) => {
                    error!("{e}");
                    self.state = CaConnState::EndOfStream;
                    break Ready(Some(Err(e)));
                }
            }

            break if self.is_shutdown() {
                if self.queues_async_out_flushed() {
                    debug!("end of stream {}", self.remote_addr_dbg);
                    self.state = CaConnState::EndOfStream;
                    Ready(None)
                } else {
                    if have_progress {
                        self.stats.ca_conn_poll_reloop().inc();
                        continue;
                    } else if have_pending {
                        self.stats.ca_conn_poll_pending().inc();
                        Pending
                    } else {
                        // TODO error
                        error!("logic error");
                        self.stats.logic_error().inc();
                        let e = Error::with_msg_no_trace("shutdown, not done, no progress, no pending");
                        Ready(Some(Err(e)))
                    }
                }
            } else {
                if have_progress {
                    self.stats.ca_conn_poll_reloop().inc();
                    continue;
                } else if have_pending {
                    self.stats.ca_conn_poll_pending().inc();
                    Pending
                } else {
                    self.stats.ca_conn_poll_no_progress_no_pending().inc();
                    let e = Error::with_msg_no_trace("no progress no pending");
                    Ready(Some(Err(e)))
                }
            };
        };
        let poll_ts2 = Instant::now();
        let dt = poll_ts2.saturating_duration_since(poll_ts1);
        if dt > Duration::from_millis(80) {
            warn!("long poll duration {:.0} ms", dt.as_secs_f32() * 1e3)
        } else if dt > Duration::from_millis(40) {
            info!("long poll duration {:.0} ms", dt.as_secs_f32() * 1e3)
        } else if false && dt > Duration::from_millis(5) {
            debug!("long poll duration {:.0} ms", dt.as_secs_f32() * 1e3)
        }
        ret
    }
}
