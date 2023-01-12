use super::proto;
use super::proto::CaItem;
use super::proto::CaMsg;
use super::proto::CaMsgTy;
use super::proto::CaProto;
use super::store::DataStore;
use super::ExtraInsertsConf;
use crate::bsread::ChannelDescDecoded;
use crate::ca::proto::CreateChan;
use crate::ca::proto::EventAdd;
use crate::ca::store::ChannelRegistry;
use crate::series::{Existence, SeriesId};
use crate::store::ChannelInfoItem;
use crate::store::ChannelStatus;
use crate::store::ChannelStatusItem;
use crate::store::CommonInsertItemQueueSender;
use crate::store::ConnectionStatus;
use crate::store::ConnectionStatusItem;
use crate::store::{InsertItem, IvlItem, MuteItem, QueryItem};
use async_channel::Sender;
use err::Error;
use futures_util::stream::FuturesOrdered;
use futures_util::{Future, FutureExt, Stream, StreamExt, TryFutureExt};
use log::*;
use netpod::timeunits::*;
use netpod::TS_MSP_GRID_SPACING;
use netpod::TS_MSP_GRID_UNIT;
use netpod::{ScalarType, Shape};
use serde::Serialize;
use stats::{CaConnStats, IntervalEma};
use std::collections::{BTreeMap, VecDeque};
use std::net::SocketAddrV4;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime};
use tokio::net::TcpStream;

#[derive(Clone, Debug, Serialize)]
pub enum ChannelConnectedInfo {
    Disconnected,
    Connecting,
    Connected,
    Error,
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
    #[allow(unused)]
    cid: Cid,
    #[allow(unused)]
    sid: u32,
    scalar_type: ScalarType,
    shape: Shape,
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
    series: Option<SeriesId>,
    info_store_msp_last: u32,
}

#[allow(unused)]
#[derive(Clone, Debug)]
enum ChannelState {
    Init,
    Creating { cid: Cid, ts_beg: Instant },
    Created(CreatedState),
    Error(ChannelError),
}

impl ChannelState {
    fn to_info(&self, name: String, addr: SocketAddrV4) -> ChannelStateInfo {
        let channel_connected_info = match self {
            ChannelState::Init => ChannelConnectedInfo::Disconnected,
            ChannelState::Creating { .. } => ChannelConnectedInfo::Connecting,
            ChannelState::Created(_) => ChannelConnectedInfo::Connected,
            ChannelState::Error(_) => ChannelConnectedInfo::Error,
        };
        let scalar_type = match self {
            ChannelState::Created(s) => Some(s.scalar_type.clone()),
            _ => None,
        };
        let shape = match self {
            ChannelState::Created(s) => Some(s.shape.clone()),
            _ => None,
        };
        let ts_created = match self {
            ChannelState::Created(s) => Some(s.ts_created.clone()),
            _ => None,
        };
        let ts_event_last = match self {
            ChannelState::Created(s) => match &s.state {
                MonitoringState::Evented(_, s) => Some(s.ts_last),
                _ => None,
            },
            _ => None,
        };
        let item_recv_ivl_ema = match self {
            ChannelState::Created(s) => {
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
            ChannelState::Created(s) => s.series.clone(),
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

#[allow(unused)]
struct ChannelsStates {
    channels: BTreeMap<Cid, ChannelState>,
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
    FindChannel(String, Sender<(SocketAddrV4, Vec<String>)>),
    ChannelState(String, Sender<(SocketAddrV4, Option<ChannelStateInfo>)>),
    ChannelStatesAll((), Sender<(SocketAddrV4, Vec<ChannelStateInfo>)>),
    ChannelAdd(String, Sender<bool>),
    ChannelRemove(String, Sender<bool>),
    Shutdown(Sender<bool>),
    ExtraInsertsConf(ExtraInsertsConf, Sender<bool>),
    CheckChannelsAlive(Sender<bool>),
    SaveConnInfo(Sender<bool>),
}

#[derive(Debug)]
pub struct ConnCommand {
    kind: ConnCommandKind,
}

impl ConnCommand {
    pub fn find_channel(pattern: String) -> (ConnCommand, async_channel::Receiver<(SocketAddrV4, Vec<String>)>) {
        let (tx, rx) = async_channel::bounded(1);
        let cmd = Self {
            kind: ConnCommandKind::FindChannel(pattern, tx),
        };
        (cmd, rx)
    }

    pub fn channel_state(
        name: String,
    ) -> (
        ConnCommand,
        async_channel::Receiver<(SocketAddrV4, Option<ChannelStateInfo>)>,
    ) {
        let (tx, rx) = async_channel::bounded(1);
        let cmd = Self {
            kind: ConnCommandKind::ChannelState(name, tx),
        };
        (cmd, rx)
    }

    pub fn channel_states_all() -> (
        ConnCommand,
        async_channel::Receiver<(SocketAddrV4, Vec<ChannelStateInfo>)>,
    ) {
        let (tx, rx) = async_channel::bounded(1);
        let cmd = Self {
            kind: ConnCommandKind::ChannelStatesAll((), tx),
        };
        (cmd, rx)
    }

    pub fn channel_add(name: String) -> (ConnCommand, async_channel::Receiver<bool>) {
        let (tx, rx) = async_channel::bounded(1);
        let cmd = Self {
            kind: ConnCommandKind::ChannelAdd(name, tx),
        };
        (cmd, rx)
    }

    pub fn channel_remove(name: String) -> (ConnCommand, async_channel::Receiver<bool>) {
        let (tx, rx) = async_channel::bounded(1);
        let cmd = Self {
            kind: ConnCommandKind::ChannelRemove(name, tx),
        };
        (cmd, rx)
    }

    pub fn shutdown() -> (ConnCommand, async_channel::Receiver<bool>) {
        let (tx, rx) = async_channel::bounded(1);
        let cmd = Self {
            kind: ConnCommandKind::Shutdown(tx),
        };
        (cmd, rx)
    }

    pub fn extra_inserts_conf_set(k: ExtraInsertsConf) -> (ConnCommand, async_channel::Receiver<bool>) {
        let (tx, rx) = async_channel::bounded(1);
        let cmd = Self {
            kind: ConnCommandKind::ExtraInsertsConf(k, tx),
        };
        (cmd, rx)
    }

    pub fn check_channels_alive() -> (ConnCommand, async_channel::Receiver<bool>) {
        let (tx, rx) = async_channel::bounded(1);
        let cmd = Self {
            kind: ConnCommandKind::CheckChannelsAlive(tx),
        };
        (cmd, rx)
    }

    pub fn save_conn_info() -> (ConnCommand, async_channel::Receiver<bool>) {
        let (tx, rx) = async_channel::bounded(1);
        let cmd = Self {
            kind: ConnCommandKind::SaveConnInfo(tx),
        };
        (cmd, rx)
    }
}

#[derive(Debug)]
pub enum CaConnEventValue {
    None,
    EchoTimeout,
}

#[derive(Debug)]
pub struct CaConnEvent {
    pub ts: Instant,
    pub value: CaConnEventValue,
}

pub struct CaConn {
    state: CaConnState,
    shutdown: bool,
    proto: Option<CaProto>,
    cid_store: CidStore,
    subid_store: SubidStore,
    channels: BTreeMap<Cid, ChannelState>,
    init_state_count: u64,
    cid_by_name: BTreeMap<String, Cid>,
    cid_by_subid: BTreeMap<u32, Cid>,
    name_by_cid: BTreeMap<Cid, String>,
    data_store: Arc<DataStore>,
    insert_item_queue: VecDeque<QueryItem>,
    insert_item_sender: CommonInsertItemQueueSender,
    insert_item_send_fut: Option<async_channel::Send<'static, QueryItem>>,
    fut_get_series:
        FuturesOrdered<Pin<Box<dyn Future<Output = Result<(Cid, u32, u16, u16, Existence<SeriesId>), Error>> + Send>>>,
    backend: String,
    remote_addr_dbg: SocketAddrV4,
    local_epics_hostname: String,
    array_truncate: usize,
    stats: Arc<CaConnStats>,
    insert_queue_max: usize,
    insert_ivl_min_mus: u64,
    conn_command_tx: async_channel::Sender<ConnCommand>,
    conn_command_rx: async_channel::Receiver<ConnCommand>,
    conn_backoff: f32,
    conn_backoff_beg: f32,
    inserts_counter: u64,
    extra_inserts_conf: ExtraInsertsConf,
    ioc_ping_last: Instant,
    ioc_ping_start: Option<Instant>,
}

impl CaConn {
    pub fn new(
        backend: String,
        remote_addr_dbg: SocketAddrV4,
        local_epics_hostname: String,
        data_store: Arc<DataStore>,
        insert_item_sender: CommonInsertItemQueueSender,
        array_truncate: usize,
        insert_queue_max: usize,
    ) -> Self {
        let (cq_tx, cq_rx) = async_channel::bounded(32);
        Self {
            state: CaConnState::Unconnected,
            shutdown: false,
            proto: None,
            cid_store: CidStore::new(),
            subid_store: SubidStore::new(),
            channels: BTreeMap::new(),
            init_state_count: 0,
            cid_by_name: BTreeMap::new(),
            cid_by_subid: BTreeMap::new(),
            name_by_cid: BTreeMap::new(),
            data_store,
            insert_item_queue: VecDeque::new(),
            insert_item_sender,
            insert_item_send_fut: None,
            fut_get_series: FuturesOrdered::new(),
            backend,
            remote_addr_dbg,
            local_epics_hostname,
            array_truncate,
            stats: Arc::new(CaConnStats::new()),
            insert_queue_max,
            insert_ivl_min_mus: 1000 * 6,
            conn_command_tx: cq_tx,
            conn_command_rx: cq_rx,
            conn_backoff: 0.02,
            conn_backoff_beg: 0.02,
            inserts_counter: 0,
            extra_inserts_conf: ExtraInsertsConf::new(),
            ioc_ping_last: Instant::now(),
            ioc_ping_start: None,
        }
    }

    pub fn conn_command_tx(&self) -> async_channel::Sender<ConnCommand> {
        self.conn_command_tx.clone()
    }

    fn handle_conn_command(&mut self, cx: &mut Context) -> Option<Poll<()>> {
        // TODO if this loops for too long time, yield and make sure we get wake up again.
        use Poll::*;
        loop {
            self.stats.caconn_loop3_count_inc();
            match self.conn_command_rx.poll_next_unpin(cx) {
                Ready(Some(a)) => match a.kind {
                    ConnCommandKind::FindChannel(pattern, tx) => {
                        let res = if let Ok(re) = regex::Regex::new(&pattern) {
                            self.name_by_cid
                                .values()
                                .filter(|x| re.is_match(x))
                                .map(ToString::to_string)
                                .collect()
                        } else {
                            Vec::new()
                        };
                        let msg = (self.remote_addr_dbg.clone(), res);
                        match tx.try_send(msg) {
                            Ok(_) => {}
                            Err(_) => {
                                self.stats.caconn_command_can_not_reply_inc();
                            }
                        }
                    }
                    ConnCommandKind::ChannelState(name, tx) => {
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
                        match tx.try_send(msg) {
                            Ok(_) => {}
                            Err(_) => {
                                self.stats.caconn_command_can_not_reply_inc();
                            }
                        }
                    }
                    ConnCommandKind::ChannelStatesAll((), tx) => {
                        let res = self
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
                        match tx.try_send(msg) {
                            Ok(_) => {}
                            Err(_) => {
                                self.stats.caconn_command_can_not_reply_inc();
                            }
                        }
                    }
                    ConnCommandKind::ChannelAdd(name, tx) => {
                        self.channel_add(name);
                        match tx.try_send(true) {
                            Ok(_) => {}
                            Err(_) => {
                                self.stats.caconn_command_can_not_reply_inc();
                            }
                        }
                    }
                    ConnCommandKind::ChannelRemove(name, tx) => {
                        self.channel_remove(name);
                        match tx.try_send(true) {
                            Ok(_) => {}
                            Err(_) => {
                                self.stats.caconn_command_can_not_reply_inc();
                            }
                        }
                    }
                    ConnCommandKind::Shutdown(tx) => {
                        self.shutdown = true;
                        let res = self.before_reset_of_channel_state();
                        self.state = CaConnState::Shutdown;
                        self.proto = None;
                        match tx.try_send(res.is_ok()) {
                            Ok(_) => {}
                            Err(_) => {
                                self.stats.caconn_command_can_not_reply_inc();
                            }
                        }
                    }
                    ConnCommandKind::ExtraInsertsConf(k, tx) => {
                        self.extra_inserts_conf = k;
                        match tx.try_send(true) {
                            Ok(_) => {}
                            Err(_) => {
                                self.stats.caconn_command_can_not_reply_inc();
                            }
                        }
                    }
                    ConnCommandKind::CheckChannelsAlive(tx) => {
                        let res = self.check_channels_alive();
                        let res = res.is_ok();
                        match tx.try_send(res) {
                            Ok(_) => {}
                            Err(_) => {
                                self.stats.caconn_command_can_not_reply_inc();
                            }
                        }
                    }
                    ConnCommandKind::SaveConnInfo(tx) => {
                        let res = self.save_conn_info();
                        let res = res.is_ok();
                        match tx.try_send(res) {
                            Ok(_) => {}
                            Err(_) => {
                                self.stats.caconn_command_can_not_reply_inc();
                            }
                        }
                    }
                },
                Ready(None) => {
                    error!("Command queue closed");
                }
                Pending => {
                    break Some(Pending);
                }
            }
        }
    }

    pub fn stats(&self) -> Arc<CaConnStats> {
        self.stats.clone()
    }

    pub fn channel_add(&mut self, channel: String) {
        if self.cid_by_name.contains_key(&channel) {
            return;
        }
        let cid = self.cid_by_name(&channel);
        if self.channels.contains_key(&cid) {
            error!("logic error");
        } else {
            self.channels.insert(cid, ChannelState::Init);
            // TODO do not count, use separate queue for those channels.
            self.init_state_count += 1;
        }
    }

    pub fn channel_remove(&mut self, channel: String) {
        let cid = self.cid_by_name(&channel);
        if self.channels.contains_key(&cid) {
            warn!("TODO actually cause the channel to get closed and removed  {}", channel);
        }
    }

    fn cid_by_name(&mut self, name: &str) -> Cid {
        if let Some(cid) = self.cid_by_name.get(name) {
            *cid
        } else {
            let cid = self.cid_store.next();
            self.cid_by_name.insert(name.into(), cid);
            self.name_by_cid.insert(cid, name.into());
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

    fn before_reset_of_channel_state(&mut self) -> Result<(), Error> {
        trace!("before_reset_of_channel_state  channels {}", self.channels.len());
        let mut created = 0;
        let mut warn_max = 0;
        for (_cid, chst) in &self.channels {
            match chst {
                ChannelState::Created(st) => {
                    if let Some(series) = &st.series {
                        let item = QueryItem::ChannelStatus(ChannelStatusItem {
                            ts: SystemTime::now(),
                            series: series.clone(),
                            status: ChannelStatus::Closed,
                        });
                        if created < 10 {
                            trace!("store {:?}", item);
                        }
                        self.insert_item_queue.push_back(item);
                    } else {
                        if warn_max < 10 {
                            debug!("no series for cid {:?}", st.cid);
                            warn_max += 1;
                        }
                    }
                    created += 1;
                }
                _ => (),
            }
        }
        Ok(())
    }

    fn handle_insert_futs(&mut self, cx: &mut Context) -> Poll<Result<(), Error>> {
        use Poll::*;
        loop {
            self.stats.caconn_loop4_count_inc();
            match self.insert_item_send_fut.as_mut() {
                Some(fut) => match fut.poll_unpin(cx) {
                    Ready(Ok(())) => {
                        self.stats.inserts_queue_push_inc();
                        self.insert_item_send_fut = None;
                    }
                    Ready(Err(e)) => {
                        self.insert_item_send_fut = None;
                        error!("handle_insert_futs can not send item {e}");
                        break Ready(Err(Error::with_msg_no_trace(format!("can not send the item"))));
                    }
                    Pending => {
                        if false {
                            // TODO test this case.
                            self.stats.inserts_queue_drop_inc();
                            self.insert_item_send_fut = None;
                        } else {
                            // Wait until global queue is ready (peer will see network pressure)
                            break Pending;
                        }
                    }
                },
                None => {}
            }
            if let Some(item) = self.insert_item_queue.pop_front() {
                self.stats.inserts_queue_pop_for_global_inc();
                let sender = unsafe { &*(&self.insert_item_sender as *const CommonInsertItemQueueSender) };
                if sender.is_full() {
                    self.stats.inserts_queue_drop_inc();
                } else {
                    self.insert_item_send_fut = Some(sender.send(item));
                }
            } else {
                break Ready(Ok(()));
            }
        }
    }

    fn check_channels_alive(&mut self) -> Result<(), Error> {
        let tsnow = Instant::now();
        trace!("CheckChannelsAlive  {addr:?}", addr = &self.remote_addr_dbg);
        if self.ioc_ping_last.elapsed() > Duration::from_millis(20000) {
            if let Some(started) = self.ioc_ping_start {
                if started.elapsed() > Duration::from_millis(4000) {
                    warn!("Echo timeout {addr:?}", addr = self.remote_addr_dbg);
                    self.shutdown = true;
                }
            } else {
                self.ioc_ping_start = Some(Instant::now());
                if let Some(proto) = &mut self.proto {
                    trace!("push echo to {}", self.remote_addr_dbg);
                    let msg = CaMsg { ty: CaMsgTy::Echo };
                    proto.push_out(msg);
                } else {
                    warn!("can not push echo, no proto");
                    self.shutdown = true;
                }
            }
        }
        let mut alive_count = 0;
        let mut not_alive_count = 0;
        for (_, st) in &self.channels {
            match st {
                ChannelState::Creating { cid, ts_beg } => {
                    if tsnow.duration_since(*ts_beg) >= Duration::from_millis(10000) {
                        let name = self.name_by_cid.get(cid);
                        warn!("channel Creating timed out  {}  {:?}", cid.0, name);
                    }
                }
                ChannelState::Created(st) => {
                    if tsnow.duration_since(st.ts_alive_last) >= Duration::from_millis(10000) {
                        not_alive_count += 1;
                    } else {
                        alive_count += 1;
                    }
                }
                _ => {}
            }
        }
        self.stats
            .channel_all_count
            .store(self.channels.len() as _, Ordering::Release);
        self.stats
            .channel_alive_count
            .store(alive_count as _, Ordering::Release);
        self.stats
            .channel_not_alive_count
            .store(not_alive_count as _, Ordering::Release);
        Ok(())
    }

    fn save_conn_info(&mut self) -> Result<(), Error> {
        let timenow = SystemTime::now();
        for (_, st) in &mut self.channels {
            match st {
                ChannelState::Init => {
                    // TODO need last-save-ts for this state.
                }
                ChannelState::Creating { cid: _, ts_beg: _ } => {
                    // TODO need last-save-ts for this state.
                }
                ChannelState::Created(st) => {
                    // TODO if we don't wave a series id yet, dont' save? write-ampl.

                    let msp = info_store_msp_from_time(timenow.clone());
                    if msp != st.info_store_msp_last {
                        st.info_store_msp_last = msp;
                        let item = QueryItem::ChannelInfo(ChannelInfoItem {
                            ts_msp: msp,
                            series: st.series.clone().unwrap_or(SeriesId::new(0)),
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
        series: Existence<SeriesId>,
        cx: &mut Context,
    ) -> Result<(), Error> {
        let tsnow = Instant::now();
        self.stats.get_series_id_ok_inc();
        let series = match series {
            Existence::Created(k) => k,
            Existence::Existing(k) => k,
        };
        if series.id() == 0 {
            warn!("Weird series id: {series:?}");
        }
        if data_type > 6 {
            error!("data type of series unexpected: {}", data_type);
        }
        let subid = self.subid_store.next();
        self.cid_by_subid.insert(subid, cid);
        let name = self.name_by_cid(cid).unwrap().to_string();
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
        *ch_s = ChannelState::Created(CreatedState {
            cid,
            sid,
            // TODO handle error better! Transition channel to Error state?
            scalar_type: ScalarType::from_ca_id(data_type)?,
            shape: Shape::from_ca_count(data_count)?,
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
            series: Some(series),
            info_store_msp_last: info_store_msp_from_time(SystemTime::now()),
        });
        let scalar_type = ScalarType::from_ca_id(data_type)?;
        let shape = Shape::from_ca_count(data_count)?;
        let _cd = ChannelDescDecoded {
            name: name.to_string(),
            scalar_type,
            shape,
            agg_kind: netpod::AggKind::Plain,
            // TODO these play no role in series id:
            byte_order: netpod::ByteOrder::Little,
            compression: None,
        };
        cx.waker().wake_by_ref();
        Ok(())
    }

    fn handle_get_series_futs(&mut self, cx: &mut Context) -> Poll<Result<(), Error>> {
        use Poll::*;
        while self.fut_get_series.len() > 0 {
            match self.fut_get_series.poll_next_unpin(cx) {
                Ready(Some(Ok((cid, sid, data_type, data_count, series)))) => {
                    {
                        let series = series.clone().into_inner();
                        let item = QueryItem::ChannelStatus(ChannelStatusItem {
                            ts: SystemTime::now(),
                            series: series,
                            status: ChannelStatus::Opened,
                        });
                        self.insert_item_queue.push_back(item);
                    }
                    match self.channel_to_evented(cid, sid, data_type, data_count, series, cx) {
                        Ok(_) => {}
                        Err(e) => {
                            return Ready(Err(e));
                        }
                    }
                }
                Ready(Some(Err(e))) => return Ready(Err(e)),
                Ready(None) => return Ready(Err(Error::with_msg_no_trace("series lookup stream should never end"))),
                Pending => break,
            }
        }
        return Pending;
    }

    fn event_add_insert(
        st: &mut CreatedState,
        series: SeriesId,
        scalar_type: ScalarType,
        shape: Shape,
        ts: u64,
        ev: proto::EventAddRes,
        item_queue: &mut VecDeque<QueryItem>,
        ts_msp_last: u64,
        inserted_in_ts_msp: u64,
        ts_msp_grid: Option<u32>,
        stats: Arc<CaConnStats>,
    ) -> Result<(), Error> {
        // TODO decide on better msp/lsp: random offset!
        // As long as one writer is active, the msp is arbitrary.
        let (ts_msp, ts_msp_changed) = if inserted_in_ts_msp >= 64000 || st.ts_msp_last + HOUR <= ts {
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
            series,
            ts_msp,
            ts_lsp,
            msp_bump: ts_msp_changed,
            pulse: 0,
            scalar_type,
            shape,
            val: ev.value.data,
            ts_msp_grid,
        };
        item_queue.push_back(QueryItem::Insert(item));
        stats.insert_item_create_inc();
        Ok(())
    }

    fn do_event_insert(
        st: &mut CreatedState,
        series: SeriesId,
        scalar_type: ScalarType,
        shape: Shape,
        ts: u64,
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
        let inserted_in_ts_msp = st.inserted_in_ts_msp;
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
                    ev.clone(),
                    item_queue,
                    ts_msp_last,
                    inserted_in_ts_msp,
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
            ev,
            item_queue,
            ts_msp_last,
            inserted_in_ts_msp,
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
            ChannelState::Created(st) => {
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
                    self.stats.ca_ts_off_4_inc();
                    //warn!("Bad time for {name}  {ts} vs {ts_local}  diff {}", ts_diff / SEC);
                    // TODO mute this channel for some time, discard the event.
                } else if ts_diff > SEC * 120 {
                    self.stats.ca_ts_off_3_inc();
                } else if ts_diff > SEC * 20 {
                    self.stats.ca_ts_off_2_inc();
                } else if ts_diff > SEC * 3 {
                    self.stats.ca_ts_off_1_inc();
                }
                if tsnow >= st.insert_next_earliest {
                    //let channel_state = self.channels.get_mut(&cid).unwrap();
                    let item_queue = &mut self.insert_item_queue;
                    let inserts_counter = &mut self.inserts_counter;
                    let extra_inserts_conf = &self.extra_inserts_conf;
                    Self::do_event_insert(
                        st,
                        series,
                        scalar_type,
                        shape,
                        ts,
                        ev,
                        tsnow,
                        item_queue,
                        self.insert_ivl_min_mus,
                        self.stats.clone(),
                        inserts_counter,
                        extra_inserts_conf,
                    )?;
                } else {
                    self.stats.channel_fast_item_drop_inc();
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
                            series,
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
                    Ready(Some(Err(e)))
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
                ChannelState::Init => {
                    let name = self
                        .name_by_cid(cid)
                        .ok_or_else(|| Error::with_msg_no_trace("name for cid not known"));
                    let name = match name {
                        Ok(k) => k,
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
        let mut msgs_tmp = vec![];
        self.check_channels_state_init(&mut msgs_tmp)?;
        let ts2 = Instant::now();
        self.stats
            .time_check_channels_state_init
            .fetch_add((ts2.duration_since(ts1) * MS as u32).as_secs(), Ordering::Release);
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
                                debug!("CreateChanRes {name:?}");
                                if false && name.contains(".STAT") {
                                    info!("Channel created for {}", name);
                                }
                                if k.data_type > 6 {
                                    error!("CreateChanRes with unexpected data_type {}", k.data_type);
                                }
                                let scalar_type = ScalarType::from_ca_id(k.data_type)?;
                                let shape = Shape::from_ca_count(k.data_count)?;
                                // TODO handle not-found error:
                                let ch_s = self.channels.get_mut(&cid).unwrap();
                                *ch_s = ChannelState::Created(CreatedState {
                                    cid,
                                    sid,
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
                                    series: None,
                                    info_store_msp_last: info_store_msp_from_time(SystemTime::now()),
                                });
                                // TODO handle error in different way. Should most likely not abort.
                                let cd = ChannelDescDecoded {
                                    name: name.to_string(),
                                    scalar_type,
                                    shape,
                                    agg_kind: netpod::AggKind::Plain,
                                    // TODO these play no role in series id:
                                    byte_order: netpod::ByteOrder::Little,
                                    compression: None,
                                };
                                let z = unsafe {
                                    &*(&self.data_store.chan_reg as &ChannelRegistry as *const ChannelRegistry)
                                };
                                let fut = z
                                    .get_series_id(cd, self.backend.clone())
                                    .map_ok(move |series| (cid, k.sid, k.data_type, k.data_count, series));
                                // TODO throttle execution rate:
                                self.fut_get_series.push_back(Box::pin(fut) as _);
                                do_wake_again = true;
                            }
                            CaMsgTy::EventAddRes(k) => {
                                trace!("got EventAddRes: {k:?}");
                                self.stats.caconn_recv_data_inc();
                                let res = Self::handle_event_add_res(self, k, tsnow);
                                let ts2 = Instant::now();
                                self.stats
                                    .time_handle_event_add_res
                                    .fetch_add((ts2.duration_since(ts1) * MS as u32).as_secs(), Ordering::AcqRel);
                                ts1 = ts2;
                                let _ = ts1;
                                res?
                            }
                            CaMsgTy::Error(e) => {
                                warn!("channel access error message {e:?}");
                            }
                            CaMsgTy::AccessRightsRes(_) => {}
                            CaMsgTy::Echo => {
                                let addr = &self.remote_addr_dbg;
                                if let Some(started) = self.ioc_ping_start {
                                    let dt = started.elapsed().as_secs_f32() * 1e3;
                                    if dt > 50. {
                                        info!("Received Echo  {dt:10.0}ms  {addr:?}");
                                    } else if dt > 500. {
                                        warn!("Received Echo  {dt:10.0}ms  {addr:?}");
                                    }
                                } else {
                                    info!("Received Echo even though we didn't asked for it  {addr:?}");
                                }
                                self.ioc_ping_last = Instant::now();
                                self.ioc_ping_start = None;
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
                // TODO unify this handling with the block below
                let reset_res = self.before_reset_of_channel_state();
                self.state = CaConnState::Wait(wait_fut(self.backoff_next()));
                self.proto = None;
                if let Err(e) = reset_res {
                    error!("can not destruct channel state before reset {e:?}");
                }
                Ready(Some(Err(e)))
            }
            Ready(None) => {
                warn!("handle_peer_ready CaProto is done  {:?}", self.remote_addr_dbg);
                let reset_res = self.before_reset_of_channel_state();
                self.state = CaConnState::Wait(wait_fut(self.backoff_next()));
                self.proto = None;
                if let Err(e) = reset_res {
                    error!("can not destruct channel state before reset {e:?}");
                    Ready(Some(Err(e)))
                } else {
                    Ready(None)
                }
            }
            Pending => Pending,
        };
        if do_wake_again {
            // TODO remove the need for this:
            trace!("do_wake_again");
            cx.waker().wake_by_ref();
        }
        res
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

    // `?` works not in here.
    fn handle_conn_state(&mut self, cx: &mut Context) -> Option<Poll<Result<(), Error>>> {
        use Poll::*;
        match &mut self.state {
            CaConnState::Unconnected => {
                let addr = self.remote_addr_dbg.clone();
                trace!("create tcp connection to {:?}", (addr.ip(), addr.port()));
                let fut = tokio::time::timeout(Duration::from_millis(1000), TcpStream::connect(addr));
                self.state = CaConnState::Connecting(addr, Box::pin(fut));
                None
            }
            CaConnState::Connecting(ref addr, ref mut fut) => {
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
                                let proto = CaProto::new(tcp, self.remote_addr_dbg.clone(), self.array_truncate);
                                self.state = CaConnState::Init;
                                self.proto = Some(proto);
                                None
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
                                None
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
                                None
                            }
                        }
                    }
                    Pending => Some(Pending),
                }
            }
            CaConnState::Init => {
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
                None
            }
            CaConnState::Listen => match {
                let res = self.handle_conn_listen(cx);
                res
            } {
                Ready(Some(Ok(()))) => Some(Ready(Ok(()))),
                Ready(Some(Err(e))) => Some(Ready(Err(e))),
                Ready(None) => None,
                Pending => Some(Pending),
            },
            CaConnState::PeerReady => {
                {
                    // TODO can I move this block somewhere else?
                    match self.handle_get_series_futs(cx) {
                        Ready(Ok(_)) => (),
                        Ready(Err(e)) => return Some(Ready(Err(e))),
                        Pending => (),
                    }
                }
                let res = self.handle_peer_ready(cx);
                match res {
                    Ready(Some(Ok(()))) => None,
                    Ready(Some(Err(e))) => Some(Ready(Err(e))),
                    Ready(None) => None,
                    Pending => Some(Pending),
                }
            }
            CaConnState::Wait(inst) => match inst.poll_unpin(cx) {
                Ready(_) => {
                    self.state = CaConnState::Unconnected;
                    self.proto = None;
                    None
                }
                Pending => Some(Pending),
            },
            CaConnState::Shutdown => None,
        }
    }

    fn loop_inner(&mut self, cx: &mut Context) -> Option<Poll<Result<(), Error>>> {
        loop {
            self.stats.caconn_loop2_count_inc();
            if let Some(v) = self.handle_conn_state(cx) {
                break Some(v);
            }
            if self.insert_item_queue.len() >= self.insert_queue_max {
                break None;
            }
            if self.shutdown {
                break None;
            }
        }
    }
}

impl Stream for CaConn {
    type Item = Result<CaConnEvent, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        self.stats.caconn_poll_count_inc();
        if self.shutdown {
            info!("CaConn poll in shutdown");
        }
        let mut i1 = 0;
        let ret = loop {
            i1 += 1;
            if self.shutdown {
                info!("CaConn in shutdown loop 1");
            }
            self.stats.caconn_loop1_count_inc();
            if !self.shutdown {
                self.handle_conn_command(cx);
            }
            let q = self.handle_insert_futs(cx);
            match q {
                Ready(_) => {}
                Pending => break Pending,
            }
            if self.shutdown {
                if self.insert_item_queue.len() == 0 {
                    trace!("no more items to flush");
                    if i1 >= 10 {
                        break Ready(Ok(()));
                    }
                } else {
                    //info!("more items {}", self.insert_item_queue.len());
                }
            }
            if self.insert_item_queue.len() >= self.insert_queue_max {
                break Pending;
            }
            if !self.shutdown {
                if let Some(v) = self.loop_inner(cx) {
                    if i1 >= 10 {
                        break v;
                    }
                }
            }
        };
        match &ret {
            Ready(_) => self.stats.conn_stream_ready_inc(),
            Pending => self.stats.conn_stream_pending_inc(),
        }
        if self.shutdown && self.insert_item_queue.len() == 0 {
            return Ready(None);
        }
        match ret {
            Ready(Ok(())) => {
                let item = CaConnEvent {
                    ts: Instant::now(),
                    value: CaConnEventValue::None,
                };
                Ready(Some(Ok(item)))
            }
            Ready(Err(e)) => Ready(Some(Err(e))),
            Pending => Pending,
        }
    }
}
