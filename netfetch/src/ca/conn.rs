use super::proto::{self, CaItem, CaMsg, CaMsgTy, CaProto};
use super::store::DataStore;
use crate::bsread::ChannelDescDecoded;
use crate::ca::proto::{CreateChan, EventAdd};
use crate::ca::store::ChannelRegistry;
use crate::series::{Existence, SeriesId};
use crate::store::{
    CommonInsertItemQueueSender, ConnectionStatus, ConnectionStatusItem, InsertItem, IvlItem, MuteItem, QueryItem,
};
use async_channel::Sender;
use err::Error;
use futures_util::stream::FuturesOrdered;
use futures_util::{Future, FutureExt, Stream, StreamExt, TryFutureExt};
use log::*;
use netpod::timeunits::*;
use netpod::{ScalarType, Shape};
use serde::Serialize;
use stats::{CaConnStats, IntervalEma};
use std::collections::{BTreeMap, VecDeque};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
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
    // TODO we also want to read while staying subscribed:
    #[allow(unused)]
    Reading,
    #[allow(unused)]
    Read,
    #[allow(unused)]
    Muted,
}

#[derive(Clone, Debug)]
struct CreatedState {
    #[allow(unused)]
    cid: u32,
    #[allow(unused)]
    sid: u32,
    scalar_type: ScalarType,
    shape: Shape,
    #[allow(unused)]
    ts_created: Instant,
    state: MonitoringState,
    ts_msp_last: u64,
    ts_msp_grid_last: u32,
    inserted_in_ts_msp: u64,
    insert_item_ivl_ema: IntervalEma,
    item_recv_ivl_ema: IntervalEma,
    insert_recv_ivl_last: Instant,
    insert_next_earliest: Instant,
    muted_before: u32,
}

#[allow(unused)]
#[derive(Clone, Debug)]
enum ChannelState {
    Init,
    Creating { cid: u32, ts_beg: Instant },
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
        let interest_score = 1. / item_recv_ivl_ema.unwrap_or(1e10).max(1e-6).min(1e10);
        ChannelStateInfo {
            name,
            addr,
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
}

fn wait_fut(dt: u64) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    let fut = tokio::time::sleep(Duration::from_millis(dt));
    Box::pin(fut)
}

struct IdStore {
    next: u32,
}

impl IdStore {
    fn new() -> Self {
        Self { next: 0 }
    }

    fn next(&mut self) -> u32 {
        self.next += 1;
        let ret = self.next;
        ret
    }
}

#[derive(Debug)]
pub enum ConnCommandKind {
    FindChannel(String, Sender<(SocketAddrV4, Vec<String>)>),
    ChannelState(String, Sender<(SocketAddrV4, Option<ChannelStateInfo>)>),
    ChannelStatesAll((), Sender<(SocketAddrV4, Vec<ChannelStateInfo>)>),
    ChannelAdd(String, Sender<bool>),
    ChannelRemove(String, Sender<bool>),
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
}

#[allow(unused)]
pub struct CaConn {
    state: CaConnState,
    proto: Option<CaProto>,
    cid_store: IdStore,
    ioid_store: IdStore,
    subid_store: IdStore,
    // TODO use a Cid or so instead of u32.
    channels: BTreeMap<u32, ChannelState>,
    init_state_count: u64,
    cid_by_name: BTreeMap<String, u32>,
    cid_by_subid: BTreeMap<u32, u32>,
    name_by_cid: BTreeMap<u32, String>,
    poll_count: usize,
    data_store: Arc<DataStore>,
    insert_item_queue: VecDeque<QueryItem>,
    insert_item_sender: CommonInsertItemQueueSender,
    insert_item_send_fut: Option<async_channel::Send<'static, QueryItem>>,
    fut_get_series:
        FuturesOrdered<Pin<Box<dyn Future<Output = Result<(u32, u32, u16, u16, Existence<SeriesId>), Error>> + Send>>>,
    remote_addr_dbg: SocketAddrV4,
    local_epics_hostname: String,
    array_truncate: usize,
    stats: Arc<CaConnStats>,
    insert_queue_max: usize,
    insert_ivl_min: Arc<AtomicU64>,
    conn_command_tx: async_channel::Sender<ConnCommand>,
    conn_command_rx: async_channel::Receiver<ConnCommand>,
    conn_backoff: f32,
    conn_backoff_beg: f32,
}

impl CaConn {
    pub fn new(
        remote_addr_dbg: SocketAddrV4,
        local_epics_hostname: String,
        data_store: Arc<DataStore>,
        insert_item_sender: CommonInsertItemQueueSender,
        array_truncate: usize,
        insert_queue_max: usize,
        insert_ivl_min: Arc<AtomicU64>,
    ) -> Self {
        let (cq_tx, cq_rx) = async_channel::bounded(32);
        Self {
            state: CaConnState::Unconnected,
            proto: None,
            cid_store: IdStore::new(),
            ioid_store: IdStore::new(),
            subid_store: IdStore::new(),
            channels: BTreeMap::new(),
            init_state_count: 0,
            cid_by_name: BTreeMap::new(),
            cid_by_subid: BTreeMap::new(),
            name_by_cid: BTreeMap::new(),
            poll_count: 0,
            data_store,
            insert_item_queue: VecDeque::new(),
            insert_item_sender,
            insert_item_send_fut: None,
            fut_get_series: FuturesOrdered::new(),
            remote_addr_dbg,
            local_epics_hostname,
            array_truncate,
            stats: Arc::new(CaConnStats::new()),
            insert_queue_max,
            insert_ivl_min,
            conn_command_tx: cq_tx,
            conn_command_rx: cq_rx,
            conn_backoff: 0.02,
            conn_backoff_beg: 0.02,
        }
    }

    pub fn conn_command_tx(&self) -> async_channel::Sender<ConnCommand> {
        self.conn_command_tx.clone()
    }

    fn handle_conn_command(&mut self, cx: &mut Context) -> Option<Poll<()>> {
        // TODO if this loops for too long time, yield and make sure we get wake up again.
        use Poll::*;
        loop {
            match self.conn_command_rx.poll_next_unpin(cx) {
                Ready(Some(a)) => match a.kind {
                    ConnCommandKind::FindChannel(pattern, tx) => {
                        //info!("Search for {pattern:?}");
                        let mut res = Vec::new();
                        for name in self.name_by_cid.values() {
                            if !pattern.is_empty() && name.contains(&pattern) {
                                res.push(name.clone());
                            }
                        }
                        let msg = (self.remote_addr_dbg.clone(), res);
                        match tx.try_send(msg) {
                            Ok(_) => {}
                            Err(_) => {
                                error!("response channel full or closed");
                            }
                        }
                    }
                    ConnCommandKind::ChannelState(name, tx) => {
                        //info!("State for {name:?}");
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
                                error!("response channel full or closed");
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
                                error!("response channel full or closed");
                            }
                        }
                    }
                    ConnCommandKind::ChannelAdd(name, tx) => {
                        self.channel_add(name);
                        match tx.try_send(true) {
                            Ok(_) => {}
                            Err(_) => {
                                error!("response channel full or closed");
                            }
                        }
                    }
                    ConnCommandKind::ChannelRemove(name, tx) => {
                        info!("remove {}", name);
                        self.channel_remove(name);
                        match tx.try_send(true) {
                            Ok(_) => {}
                            Err(_) => {
                                error!("response channel full or closed");
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
        let cid = self.cid_by_name(&channel);
        if self.channels.contains_key(&cid) {
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

    fn backoff_next(&mut self) -> u64 {
        let dt = (self.conn_backoff * 300. * 1e3) as u64;
        self.conn_backoff = (self.conn_backoff * 2.).tanh();
        dt
    }

    fn backoff_reset(&mut self) {
        self.conn_backoff = self.conn_backoff_beg;
    }

    fn handle_insert_futs(&mut self, cx: &mut Context) -> Poll<Result<(), Error>> {
        use Poll::*;
        loop {
            match self.insert_item_send_fut.as_mut() {
                Some(fut) => match fut.poll_unpin(cx) {
                    Ready(Ok(())) => {
                        self.stats.inserts_queue_push_inc();
                        self.insert_item_send_fut = None;
                    }
                    Ready(Err(_)) => break Ready(Err(Error::with_msg_no_trace(format!("can not send the item")))),
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

    fn channel_to_evented(
        &mut self,
        cid: u32,
        sid: u32,
        data_type: u16,
        data_count: u16,
        series: Existence<SeriesId>,
        cx: &mut Context,
    ) -> Result<(), Error> {
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
            ts_created: Instant::now(),
            state: MonitoringState::AddingEvent(series),
            ts_msp_last: 0,
            ts_msp_grid_last: 0,
            inserted_in_ts_msp: u64::MAX,
            insert_item_ivl_ema: IntervalEma::new(),
            item_recv_ivl_ema: IntervalEma::new(),
            insert_recv_ivl_last: Instant::now(),
            insert_next_earliest: Instant::now(),
            muted_before: 0,
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
        Ok(())
    }

    fn handle_get_series_futs(&mut self, cx: &mut Context) -> Poll<Result<(), Error>> {
        use Poll::*;
        while self.fut_get_series.len() > 0 {
            match self.fut_get_series.poll_next_unpin(cx) {
                Ready(Some(Ok((cid, sid, data_type, data_count, series)))) => {
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
        let (ts_msp, ts_msp_changed) = if inserted_in_ts_msp >= 20000 {
            let ts_msp = ts / (10 * SEC) * (10 * SEC);
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
            series: series.id(),
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
        insert_ivl_min: Arc<AtomicU64>,
        stats: Arc<CaConnStats>,
    ) -> Result<(), Error> {
        st.muted_before = 0;
        st.insert_item_ivl_ema.tick(tsnow);
        let em = st.insert_item_ivl_ema.ema();
        let ema = em.ema();
        let ivl_min = insert_ivl_min.load(Ordering::Acquire);
        let ivl_min = (ivl_min as f32) * 1e-6;
        let dt = (ivl_min - ema).max(0.) / em.k();
        st.insert_next_earliest = tsnow
            .checked_add(Duration::from_micros((dt * 1e6) as u64))
            .ok_or_else(|| Error::with_msg_no_trace("time overflow in next insert"))?;
        let ts_msp_last = st.ts_msp_last;
        let inserted_in_ts_msp = st.inserted_in_ts_msp;
        // TODO get event timestamp from channel access field
        let ts_msp_grid = (ts / (SEC * 10 * 6 * 2)) as u32 * (6 * 2);
        let ts_msp_grid = if st.ts_msp_grid_last != ts_msp_grid {
            st.ts_msp_grid_last = ts_msp_grid;
            Some(ts_msp_grid)
        } else {
            None
        };
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
        Ok(())
    }

    fn handle_event_add_res(&mut self, ev: proto::EventAddRes, tsnow: Instant) -> Result<(), Error> {
        // TODO handle subid-not-found which can also be peer error:
        let cid = *self.cid_by_subid.get(&ev.subid).unwrap();
        let _name = self.name_by_cid(cid).unwrap().to_string();
        // TODO get rid of the string clone when I don't want the log output any longer:
        // TODO handle not-found error:
        let mut series_2 = None;
        let ch_s = self.channels.get_mut(&cid).unwrap();
        match ch_s {
            ChannelState::Created(st) => {
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
                    Self::do_event_insert(
                        st,
                        series,
                        scalar_type,
                        shape,
                        ts,
                        ev,
                        tsnow,
                        item_queue,
                        self.insert_ivl_min.clone(),
                        self.stats.clone(),
                    )?;
                } else {
                    self.stats.channel_fast_item_drop_inc();
                    if tsnow.duration_since(st.insert_recv_ivl_last) >= Duration::from_millis(10000) {
                        st.insert_recv_ivl_last = tsnow;
                        let ema = st.insert_item_ivl_ema.ema();
                        let item = IvlItem {
                            series: series.id(),
                            ts,
                            ema: ema.ema(),
                            emd: ema.emv().sqrt(),
                        };
                        self.insert_item_queue.push_back(QueryItem::Ivl(item));
                    }
                    if false && st.muted_before == 0 {
                        let ema = st.insert_item_ivl_ema.ema();
                        let item = MuteItem {
                            series: series.id(),
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
                    Ready(Some(Ok(())))
                }
            },
            Ready(None) => {
                warn!("CaProto is done  {:?}", self.remote_addr_dbg);
                self.state = CaConnState::Wait(wait_fut(10000));
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
        let keys: Vec<u32> = self.channels.keys().map(|x| *x).collect();
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
                    debug!("Sending CreateChan for {}", name);
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
            //info!("msgs_tmp.len() {}", msgs_tmp.len());
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
                                info!("Search result indicates server address: {addr}");
                            }
                            CaMsgTy::CreateChanRes(k) => {
                                // TODO handle cid-not-found which can also indicate peer error.
                                let cid = k.cid;
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
                                    state: MonitoringState::FetchSeriesId,
                                    ts_msp_last: 0,
                                    ts_msp_grid_last: 0,
                                    inserted_in_ts_msp: u64::MAX,
                                    insert_item_ivl_ema: IntervalEma::new(),
                                    item_recv_ivl_ema: IntervalEma::new(),
                                    insert_recv_ivl_last: tsnow,
                                    insert_next_earliest: tsnow,
                                    muted_before: 0,
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
                                let z = unsafe {
                                    &*(&self.data_store.chan_reg as &ChannelRegistry as *const ChannelRegistry)
                                };
                                let fut = z
                                    .get_series_id(cd)
                                    .map_ok(move |series| (cid, k.sid, k.data_type, k.data_count, series));
                                // TODO throttle execution rate:
                                self.fut_get_series.push(Box::pin(fut) as _);
                                do_wake_again = true;
                            }
                            CaMsgTy::EventAddRes(k) => {
                                let res = Self::handle_event_add_res(self, k, tsnow);
                                let ts2 = Instant::now();
                                self.stats
                                    .time_handle_event_add_res
                                    .fetch_add((ts2.duration_since(ts1) * MS as u32).as_secs(), Ordering::AcqRel);
                                ts1 = ts2;
                                let _ = ts1;
                                res?
                            }
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
                self.state = CaConnState::Wait(wait_fut(10000));
                self.proto = None;
                Ready(None)
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
}

impl Stream for CaConn {
    type Item = Result<(), Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let ts_outer_1 = Instant::now();
        let mut ts1 = ts_outer_1;
        self.poll_count += 1;
        // TODO factor out the inner loop:
        let ret = 'outer: loop {
            self.handle_conn_command(cx);
            let q = self.handle_insert_futs(cx);
            let ts2 = Instant::now();
            self.stats
                .poll_time_handle_insert_futs
                .fetch_add((ts2.duration_since(ts1) * MS as u32).as_secs(), Ordering::AcqRel);
            ts1 = ts2;
            match q {
                Ready(_) => {}
                Pending => break Pending,
            }

            if self.insert_item_queue.len() >= self.insert_queue_max {
                break Pending;
            }

            break loop {
                break match &mut self.state {
                    CaConnState::Unconnected => {
                        let addr = self.remote_addr_dbg.clone();
                        trace!("create tcp connection to {:?}", (addr.ip(), addr.port()));
                        let fut = async move {
                            tokio::time::timeout(Duration::from_millis(500), TcpStream::connect(addr)).await
                        };
                        self.state = CaConnState::Connecting(addr, Box::pin(fut));
                        continue 'outer;
                    }
                    CaConnState::Connecting(ref addr, ref mut fut) => {
                        match fut.poll_unpin(cx) {
                            Ready(connect_result) => {
                                match connect_result {
                                    Ok(Ok(tcp)) => {
                                        let addr = addr.clone();
                                        self.insert_item_queue.push_back(QueryItem::ConnectionStatus(
                                            ConnectionStatusItem {
                                                ts: SystemTime::now(),
                                                addr,
                                                status: ConnectionStatus::Established,
                                            },
                                        ));
                                        self.backoff_reset();
                                        let proto =
                                            CaProto::new(tcp, self.remote_addr_dbg.clone(), self.array_truncate);
                                        self.state = CaConnState::Init;
                                        self.proto = Some(proto);
                                        continue 'outer;
                                    }
                                    Ok(Err(e)) => {
                                        // TODO log with exponential backoff
                                        // 172.26.24.118:2072
                                        const ADDR2: Ipv4Addr = Ipv4Addr::new(172, 26, 24, 118);
                                        if addr.ip() == &ADDR2 && addr.port() == 2072 {
                                            warn!("error during connect to {addr:?} {e:?}");
                                        }
                                        let addr = addr.clone();
                                        self.insert_item_queue.push_back(QueryItem::ConnectionStatus(
                                            ConnectionStatusItem {
                                                ts: SystemTime::now(),
                                                addr,
                                                status: ConnectionStatus::ConnectError,
                                            },
                                        ));
                                        let dt = self.backoff_next();
                                        self.state = CaConnState::Wait(wait_fut(dt));
                                        self.proto = None;
                                        continue 'outer;
                                    }
                                    Err(e) => {
                                        // TODO log with exponential backoff
                                        trace!("timeout during connect to {addr:?} {e:?}");
                                        let addr = addr.clone();
                                        self.insert_item_queue.push_back(QueryItem::ConnectionStatus(
                                            ConnectionStatusItem {
                                                ts: SystemTime::now(),
                                                addr,
                                                status: ConnectionStatus::ConnectTimeout,
                                            },
                                        ));
                                        let dt = self.backoff_next();
                                        self.state = CaConnState::Wait(wait_fut(dt));
                                        self.proto = None;
                                        continue 'outer;
                                    }
                                }
                            }
                            Pending => Pending,
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
                        continue 'outer;
                    }
                    CaConnState::Listen => match {
                        let res = self.handle_conn_listen(cx);
                        let ts2 = Instant::now();
                        self.stats
                            .time_handle_conn_listen
                            .fetch_add((ts2.duration_since(ts1) * MS as u32).as_secs(), Ordering::AcqRel);
                        ts1 = ts2;
                        res
                    } {
                        Ready(Some(Ok(()))) => Ready(Some(Ok(()))),
                        Ready(Some(Err(e))) => Ready(Some(Err(e))),
                        Ready(None) => continue 'outer,
                        Pending => Pending,
                    },
                    CaConnState::PeerReady => {
                        {
                            let _ = self.handle_get_series_futs(cx)?;
                            let ts2 = Instant::now();
                            self.stats
                                .poll_time_get_series_futs
                                .fetch_add((ts2.duration_since(ts1) * MS as u32).as_secs(), Ordering::AcqRel);
                            ts1 = ts2;
                        }
                        let res = self.handle_peer_ready(cx);
                        let ts2 = Instant::now();
                        self.stats.time_handle_peer_ready_dur(ts2.duration_since(ts1));
                        ts1 = ts2;
                        match res {
                            Ready(Some(Ok(()))) => {
                                if self.insert_item_queue.len() >= self.insert_queue_max {
                                    continue 'outer;
                                } else {
                                    continue;
                                }
                            }
                            Ready(Some(Err(e))) => Ready(Some(Err(e))),
                            Ready(None) => {
                                // TODO even though protocol is done, we might still have e.g. insert items to flush!
                                Ready(None)
                            }
                            Pending => Pending,
                        }
                    }
                    CaConnState::Wait(inst) => match inst.poll_unpin(cx) {
                        Ready(_) => {
                            self.state = CaConnState::Unconnected;
                            self.proto = None;
                            continue 'outer;
                        }
                        Pending => Pending,
                    },
                };
            };
        };
        let ts_outer_2 = Instant::now();
        self.stats.poll_time_all_dur(ts_outer_2.duration_since(ts_outer_1));
        // TODO currently, this will never stop by itself
        match &ret {
            Ready(_) => self.stats.conn_stream_ready_inc(),
            Pending => self.stats.conn_stream_pending_inc(),
        }
        ret
    }
}
