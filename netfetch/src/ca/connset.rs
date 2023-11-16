use super::conn::ChannelStateInfo;
use super::conn::CheckHealthResult;
use super::conn::ConnCommandResult;
use super::findioc::FindIocRes;
use super::statemap;
use super::statemap::ChannelState;
use super::statemap::ConnectionState;
use super::statemap::ConnectionStateValue;
use crate::ca::conn::CaConn;
use crate::ca::conn::CaConnEvent;
use crate::ca::conn::CaConnEventValue;
use crate::ca::conn::CaConnOpts;
use crate::ca::conn::ConnCommand;
use crate::ca::statemap::CaConnState;
use crate::ca::statemap::WithAddressState;
use crate::conf::CaIngestOpts;
use crate::daemon_common::Channel;
use crate::errconv::ErrConv;
use crate::rt::JoinHandle;
use crate::senderpolling::SenderPolling;
use crate::throttletrace::ThrottleTrace;
use async_channel::Receiver;
use async_channel::Sender;
use atomic::AtomicUsize;
use core::fmt;
use dbpg::seriesbychannel::BoxedSend;
use dbpg::seriesbychannel::CanSendChannelInfoResult;
use dbpg::seriesbychannel::ChannelInfoQuery;
use dbpg::seriesbychannel::ChannelInfoResult;
use err::Error;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use log::*;
use netpod::Database;
use scywr::iteminsertqueue::ChannelInfoItem;
use scywr::iteminsertqueue::ChannelStatus;
use scywr::iteminsertqueue::ChannelStatusItem;
use scywr::iteminsertqueue::QueryItem;
use serde::Serialize;
use series::ChannelStatusSeriesId;
use statemap::ActiveChannelState;
use statemap::CaConnStateValue;
use statemap::ChannelStateMap;
use statemap::ChannelStateValue;
use statemap::WithStatusSeriesIdState;
use statemap::WithStatusSeriesIdStateInner;
use statemap::CHANNEL_STATUS_DUMMY_SCALAR_TYPE;
use stats::CaConnSetStats;
use stats::CaConnStats;
use stats::CaProtoStats;
use stats::IocFinderStats;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::net::SocketAddrV4;
use std::pin::pin;
use std::pin::Pin;
use std::sync::atomic;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use taskrun::tokio;

#[allow(non_upper_case_globals)]
pub const trigger: [&'static str; 5] = [
    "S10-CMON-DIA1431:CURRENT-3-3",
    "S10-CMON-DIA1431:CURRENT-5",
    "S10-CMON-DIA1431:FAN-SPEED",
    "S10-CMON-DIA1431:POWER-TOT",
    "S10-CMON-MAG1721:TIN",
];

const CHECK_CHANS_PER_TICK: usize = 10000000;
pub const SEARCH_BATCH_MAX: usize = 64;
pub const CURRENT_SEARCH_PENDING_MAX: usize = SEARCH_BATCH_MAX * 2;
const UNKNOWN_ADDRESS_STAY: Duration = Duration::from_millis(15000);
const NO_ADDRESS_STAY: Duration = Duration::from_millis(20000);
const MAYBE_WRONG_ADDRESS_STAY: Duration = Duration::from_millis(4000);
const SEARCH_PENDING_TIMEOUT: Duration = Duration::from_millis(30000);
const CHANNEL_HEALTH_TIMEOUT: Duration = Duration::from_millis(30000);
const CHANNEL_UNASSIGNED_TIMEOUT: Duration = Duration::from_millis(0);
const CHANNEL_MAX_WITHOUT_HEALTH_UPDATE: usize = 10000;

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

#[derive(Debug, PartialEq, Eq)]
pub struct CmdId(SocketAddrV4, usize);

pub struct CaConnRes {
    state: CaConnState,
    sender: Pin<Box<SenderPolling<ConnCommand>>>,
    stats: Arc<CaConnStats>,
    cmd_queue: VecDeque<ConnCommand>,
    // TODO await on jh
    jh: JoinHandle<Result<(), Error>>,
}

impl CaConnRes {
    pub fn stats(&self) -> &Arc<CaConnStats> {
        &self.stats
    }
}

#[derive(Debug, Clone)]
pub struct ChannelAddWithAddr {
    backend: String,
    name: String,
    local_epics_hostname: String,
    cssid: ChannelStatusSeriesId,
    addr: SocketAddr,
}

#[derive(Debug, Clone)]
pub struct ChannelAddWithStatusId {
    backend: String,
    name: String,
    local_epics_hostname: String,
    cssid: ChannelStatusSeriesId,
}

#[derive(Debug, Clone)]
pub struct ChannelAdd {
    backend: String,
    name: String,
    local_epics_hostname: String,
}

#[derive(Debug, Clone)]
pub struct ChannelRemove {
    name: String,
}

pub struct ChannelStatusRequest {
    pub tx: Sender<ChannelStatusResponse>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChannelStatusResponse {
    pub channels_ca_conn: BTreeMap<String, ChannelStateInfo>,
    pub channels_ca_conn_set: BTreeMap<String, ChannelState>,
}

impl fmt::Debug for ChannelStatusRequest {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("ChannelStatusesRequest").finish()
    }
}

pub struct ChannelStatusesRequest {
    pub name: String,
    pub limit: u64,
    pub tx: Sender<ChannelStatusesResponse>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChannelStatusesResponse {
    pub channels_ca_conn_set: BTreeMap<String, ChannelState>,
}

impl fmt::Debug for ChannelStatusesRequest {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("ChannelStatusesRequest").finish()
    }
}

#[derive(Debug)]
pub enum ConnSetCmd {
    ChannelAdd(ChannelAdd),
    ChannelRemove(ChannelRemove),
    CheckHealth(Instant),
    Shutdown,
    ChannelStatuses(ChannelStatusesRequest),
}

#[derive(Debug)]
pub enum CaConnSetEvent {
    ConnSetCmd(ConnSetCmd),
}

impl CaConnSetEvent {
    // pub fn new_cmd_channel_statuses() -> (Self, Receiver) {}
}

#[derive(Debug, Clone)]
pub enum CaConnSetItem {
    Error(Error),
    Healthy(Instant, Instant),
}

pub struct CaConnSetCtrl {
    tx: Sender<CaConnSetEvent>,
    rx: Receiver<CaConnSetItem>,
    stats: Arc<CaConnSetStats>,
    ca_conn_stats: Arc<CaConnStats>,
    ca_proto_stats: Arc<CaProtoStats>,
    ioc_finder_stats: Arc<IocFinderStats>,
    jh: JoinHandle<Result<(), Error>>,
}

impl CaConnSetCtrl {
    pub fn sender(&self) -> Sender<CaConnSetEvent> {
        self.tx.clone()
    }

    pub fn receiver(&self) -> Receiver<CaConnSetItem> {
        self.rx.clone()
    }

    pub async fn add_channel(&self, backend: String, name: String, local_epics_hostname: String) -> Result<(), Error> {
        let cmd = ChannelAdd {
            backend,
            name,
            local_epics_hostname,
        };
        let cmd = ConnSetCmd::ChannelAdd(cmd);
        self.tx.send(CaConnSetEvent::ConnSetCmd(cmd)).await?;
        Ok(())
    }

    pub async fn remove_channel(&self, name: String) -> Result<(), Error> {
        let cmd = ChannelRemove { name };
        let cmd = ConnSetCmd::ChannelRemove(cmd);
        self.tx.send(CaConnSetEvent::ConnSetCmd(cmd)).await?;
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), Error> {
        let cmd = ConnSetCmd::Shutdown;
        self.tx.send(CaConnSetEvent::ConnSetCmd(cmd)).await?;
        Ok(())
    }

    pub async fn check_health(&self) -> Result<(), Error> {
        let cmd = ConnSetCmd::CheckHealth(Instant::now());
        let n = self.tx.len();
        if n > 0 {
            debug!("check_health  self.tx.len() {:?}", n);
        }
        let s = format!("{:?}", cmd);
        self.tx.send(CaConnSetEvent::ConnSetCmd(cmd)).await?;
        debug!("check_health  enqueued {s}");
        Ok(())
    }

    pub async fn join(self) -> Result<(), Error> {
        self.jh.await.map_err(|e| Error::with_msg_no_trace(e.to_string()))??;
        Ok(())
    }

    pub fn stats(&self) -> &Arc<CaConnSetStats> {
        &self.stats
    }

    pub fn ca_conn_stats(&self) -> &Arc<CaConnStats> {
        &self.ca_conn_stats
    }

    pub fn ca_proto_stats(&self) -> &Arc<CaProtoStats> {
        &self.ca_proto_stats
    }

    pub fn ioc_finder_stats(&self) -> &Arc<IocFinderStats> {
        &self.ioc_finder_stats
    }
}

#[derive(Debug)]
pub struct IocAddrQuery {
    name: String,
    use_cache: bool,
}

impl IocAddrQuery {
    pub fn cached(name: String) -> Self {
        Self { name, use_cache: true }
    }

    pub fn uncached(name: String) -> Self {
        Self { name, use_cache: false }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn name_string(&self) -> &String {
        &self.name
    }

    pub fn use_cache(&self) -> bool {
        self.use_cache
    }
}

struct SeriesLookupSender {
    tx: Sender<Result<ChannelInfoResult, Error>>,
}

impl CanSendChannelInfoResult for SeriesLookupSender {
    fn make_send(&self, item: Result<ChannelInfoResult, dbpg::seriesbychannel::Error>) -> BoxedSend {
        let tx = self.tx.clone();
        let fut = async move {
            tx.send(item.map_err(|e| Error::with_msg_no_trace(e.to_string())))
                .await
                .map_err(|_| ())
        };
        Box::pin(fut)
    }
}

pub struct CaConnSet {
    backend: String,
    local_epics_hostname: String,
    ca_conn_ress: BTreeMap<SocketAddr, CaConnRes>,
    channel_states: ChannelStateMap,
    connset_inp_rx: Pin<Box<Receiver<CaConnSetEvent>>>,
    channel_info_query_queue: VecDeque<ChannelInfoQuery>,
    channel_info_query_sender: Pin<Box<SenderPolling<ChannelInfoQuery>>>,
    channel_info_query_tx: Option<Sender<ChannelInfoQuery>>,
    channel_info_res_tx: Pin<Box<Sender<Result<ChannelInfoResult, Error>>>>,
    channel_info_res_rx: Pin<Box<Receiver<Result<ChannelInfoResult, Error>>>>,
    find_ioc_query_queue: VecDeque<IocAddrQuery>,
    find_ioc_query_sender: Pin<Box<SenderPolling<IocAddrQuery>>>,
    find_ioc_res_rx: Pin<Box<Receiver<VecDeque<FindIocRes>>>>,
    storage_insert_tx: Pin<Box<Sender<VecDeque<QueryItem>>>>,
    storage_insert_queue: VecDeque<VecDeque<QueryItem>>,
    storage_insert_sender: Pin<Box<SenderPolling<VecDeque<QueryItem>>>>,
    ca_conn_res_tx: Pin<Box<Sender<(SocketAddr, CaConnEvent)>>>,
    ca_conn_res_rx: Pin<Box<Receiver<(SocketAddr, CaConnEvent)>>>,
    connset_out_queue: VecDeque<CaConnSetItem>,
    connset_out_tx: Pin<Box<Sender<CaConnSetItem>>>,
    shutdown_stopping: bool,
    shutdown_done: bool,
    chan_check_next: Option<Channel>,
    stats: Arc<CaConnSetStats>,
    ca_conn_stats: Arc<CaConnStats>,
    ioc_finder_jh: JoinHandle<Result<(), Error>>,
    await_ca_conn_jhs: VecDeque<(SocketAddr, JoinHandle<Result<(), Error>>)>,
    thr_msg_poll_1: ThrottleTrace,
    thr_msg_storage_len: ThrottleTrace,
    ca_proto_stats: Arc<CaProtoStats>,
    rogue_channel_count: u64,
    connect_fail_count: usize,
}

impl CaConnSet {
    pub fn start(
        backend: String,
        local_epics_hostname: String,
        storage_insert_tx: Sender<VecDeque<QueryItem>>,
        channel_info_query_tx: Sender<ChannelInfoQuery>,
        ingest_opts: CaIngestOpts,
    ) -> CaConnSetCtrl {
        let (ca_conn_res_tx, ca_conn_res_rx) = async_channel::bounded(200);
        let (connset_inp_tx, connset_inp_rx) = async_channel::bounded(200);
        let (connset_out_tx, connset_out_rx) = async_channel::bounded(200);
        let (find_ioc_res_tx, find_ioc_res_rx) = async_channel::bounded(400);
        let ioc_finder_stats = Arc::new(IocFinderStats::new());
        let (find_ioc_query_tx, ioc_finder_jh) = super::finder::start_finder(
            find_ioc_res_tx.clone(),
            backend.clone(),
            ingest_opts,
            ioc_finder_stats.clone(),
        )
        .unwrap();
        let (channel_info_res_tx, channel_info_res_rx) = async_channel::bounded(400);
        let stats = Arc::new(CaConnSetStats::new());
        let ca_proto_stats = Arc::new(CaProtoStats::new());
        let ca_conn_stats = Arc::new(CaConnStats::new());
        let connset = Self {
            backend,
            local_epics_hostname,
            ca_conn_ress: BTreeMap::new(),
            channel_states: ChannelStateMap::new(),
            connset_inp_rx: Box::pin(connset_inp_rx),
            channel_info_query_queue: VecDeque::new(),
            channel_info_query_sender: Box::pin(SenderPolling::new(channel_info_query_tx.clone())),
            channel_info_query_tx: Some(channel_info_query_tx),
            channel_info_res_tx: Box::pin(channel_info_res_tx),
            channel_info_res_rx: Box::pin(channel_info_res_rx),
            find_ioc_query_queue: VecDeque::new(),
            find_ioc_query_sender: Box::pin(SenderPolling::new(find_ioc_query_tx)),
            find_ioc_res_rx: Box::pin(find_ioc_res_rx),
            storage_insert_tx: Box::pin(storage_insert_tx.clone()),
            storage_insert_queue: VecDeque::new(),
            storage_insert_sender: Box::pin(SenderPolling::new(storage_insert_tx)),
            ca_conn_res_tx: Box::pin(ca_conn_res_tx),
            ca_conn_res_rx: Box::pin(ca_conn_res_rx),
            shutdown_stopping: false,
            shutdown_done: false,
            chan_check_next: None,
            stats: stats.clone(),
            ca_conn_stats: ca_conn_stats.clone(),
            connset_out_tx: Box::pin(connset_out_tx),
            connset_out_queue: VecDeque::new(),
            // connset_out_sender: SenderPolling::new(connset_out_tx),
            ioc_finder_jh,
            await_ca_conn_jhs: VecDeque::new(),
            thr_msg_poll_1: ThrottleTrace::new(Duration::from_millis(2000)),
            thr_msg_storage_len: ThrottleTrace::new(Duration::from_millis(1000)),
            ca_proto_stats: ca_proto_stats.clone(),
            rogue_channel_count: 0,
            connect_fail_count: 0,
        };
        // TODO await on jh
        let jh = tokio::spawn(CaConnSet::run(connset));
        CaConnSetCtrl {
            tx: connset_inp_tx,
            rx: connset_out_rx,
            stats,
            ca_conn_stats,
            ca_proto_stats,
            ioc_finder_stats,
            jh,
        }
    }

    async fn run(mut this: CaConnSet) -> Result<(), Error> {
        debug!("CaConnSet  run begin");
        loop {
            let x = this.next().await;
            match x {
                Some(x) => this.connset_out_tx.send(x).await?,
                None => break,
            }
        }
        // debug!(
        //     "search_tx  sender {}  receiver {}",
        //     this.find_ioc_query_tx.sender_count(),
        //     this.find_ioc_query_tx.receiver_count()
        // );
        debug!("CaConnSet EndOfStream");
        debug!("join ioc_finder_jh A  {:?}", this.find_ioc_query_sender.len());
        this.find_ioc_query_sender.as_mut().drop();
        debug!("join ioc_finder_jh B  {:?}", this.find_ioc_query_sender.len());
        this.ioc_finder_jh
            .await
            .map_err(|e| Error::with_msg_no_trace(e.to_string()))??;
        debug!("joined ioc_finder_jh");
        this.connset_out_tx.close();
        this.connset_inp_rx.close();
        this.shutdown_done = true;
        Ok(())
    }

    fn handle_event(&mut self, ev: CaConnSetEvent) -> Result<(), Error> {
        // trace!("handle_event  {ev:?}");
        match ev {
            CaConnSetEvent::ConnSetCmd(cmd) => match cmd {
                ConnSetCmd::ChannelAdd(x) => self.handle_add_channel(x),
                // ConnSetCmd::ChannelAddWithStatusId(x) => self.handle_add_channel_with_status_id(x),
                // ConnSetCmd::ChannelAddWithAddr(x) => self.handle_add_channel_with_addr(x),
                ConnSetCmd::ChannelRemove(x) => self.handle_remove_channel(x),
                // ConnSetCmd::IocAddrQueryResult(x) => self.handle_ioc_query_result(x).await,
                // ConnSetCmd::SeriesLookupResult(x) => self.handle_series_lookup_result(x).await,
                ConnSetCmd::CheckHealth(ts1) => self.handle_check_health(ts1),
                ConnSetCmd::Shutdown => self.handle_shutdown(),
                ConnSetCmd::ChannelStatuses(x) => self.handle_channel_statuses_req(x),
            },
        }
    }

    fn handle_ca_conn_event(&mut self, addr: SocketAddr, ev: CaConnEvent) -> Result<(), Error> {
        match ev.value {
            CaConnEventValue::None => Ok(()),
            CaConnEventValue::EchoTimeout => Ok(()),
            CaConnEventValue::ConnCommandResult(x) => self.handle_conn_command_result(addr, x),
            CaConnEventValue::QueryItem(item) => {
                todo!("remove this insert case");
                // self.storage_insert_queue.push_back(item);
                Ok(())
            }
            CaConnEventValue::ChannelCreateFail(x) => self.handle_channel_create_fail(addr, x),
            CaConnEventValue::EndOfStream => self.handle_ca_conn_eos(addr),
            CaConnEventValue::ConnectFail => self.handle_connect_fail(addr),
        }
    }

    fn handle_series_lookup_result(&mut self, res: Result<ChannelInfoResult, Error>) -> Result<(), Error> {
        if self.shutdown_stopping {
            return Ok(());
        }
        trace3!("handle_series_lookup_result  {res:?}");
        match res {
            Ok(res) => {
                let add = ChannelAddWithStatusId {
                    backend: res.backend,
                    name: res.channel,
                    local_epics_hostname: self.local_epics_hostname.clone(),
                    cssid: ChannelStatusSeriesId::new(res.series.into_inner().id()),
                };
                self.handle_add_channel_with_status_id(add)?;
            }
            Err(e) => {
                warn!("TODO handle error {e}");
            }
        }
        Ok(())
    }

    fn handle_add_channel(&mut self, cmd: ChannelAdd) -> Result<(), Error> {
        if self.shutdown_stopping {
            trace3!("handle_add_channel but shutdown_stopping");
            return Ok(());
        }
        trace3!("handle_add_channel {}", cmd.name);
        if trigger.contains(&cmd.name.as_str()) {
            debug!("handle_add_channel  {cmd:?}");
        }
        self.stats.channel_add().inc();
        // TODO should I add the transition through ActiveChannelState::Init as well?
        let ch = Channel::new(cmd.name.clone());
        let _st = self.channel_states.inner().entry(ch).or_insert_with(|| ChannelState {
            value: ChannelStateValue::Active(ActiveChannelState::WaitForStatusSeriesId {
                since: SystemTime::now(),
            }),
            running_cmd_id: None,
            health_timeout_count: 0,
        });
        let tx = self.channel_info_res_tx.as_ref().get_ref().clone();
        let item = ChannelInfoQuery {
            backend: cmd.backend,
            channel: cmd.name,
            scalar_type: CHANNEL_STATUS_DUMMY_SCALAR_TYPE,
            shape_dims: Vec::new(),
            tx: Box::pin(SeriesLookupSender { tx }),
        };
        self.channel_info_query_queue.push_back(item);
        Ok(())
    }

    fn handle_add_channel_with_status_id(&mut self, cmd: ChannelAddWithStatusId) -> Result<(), Error> {
        trace3!("handle_add_channel_with_status_id {}", cmd.name);
        if self.shutdown_stopping {
            debug!("handle_add_channel but shutdown_stopping");
            return Ok(());
        }
        self.stats.channel_status_series_found().inc();
        if trigger.contains(&cmd.name.as_str()) {
            debug!("handle_add_channel_with_status_id  {cmd:?}");
        }
        let ch = Channel::new(cmd.name.clone());
        if let Some(chst) = self.channel_states.inner().get_mut(&ch) {
            if let ChannelStateValue::Active(chst2) = &mut chst.value {
                if let ActiveChannelState::WaitForStatusSeriesId { .. } = chst2 {
                    *chst2 = ActiveChannelState::WithStatusSeriesId {
                        status_series_id: cmd.cssid,
                        state: WithStatusSeriesIdState {
                            addr_find_backoff: 0,
                            inner: WithStatusSeriesIdStateInner::AddrSearchPending {
                                since: SystemTime::now(),
                            },
                        },
                    };
                    let qu = IocAddrQuery::cached(cmd.name);
                    self.find_ioc_query_queue.push_back(qu);
                    self.stats.ioc_search_start().inc();
                } else {
                    self.stats.logic_issue().inc();
                    trace!("TODO have a status series id but no more channel");
                }
            } else {
                self.stats.logic_issue().inc();
                trace!("TODO have a status series id but no more channel");
            }
        } else {
            self.stats.logic_issue().inc();
            trace!("TODO have a status series id but no more channel");
        }
        Ok(())
    }

    fn handle_add_channel_with_addr(&mut self, cmd: ChannelAddWithAddr) -> Result<(), Error> {
        if self.shutdown_stopping {
            trace3!("handle_add_channel but shutdown_stopping");
            return Ok(());
        }
        let addr_v4 = if let SocketAddr::V4(x) = cmd.addr {
            x
        } else {
            return Err(Error::with_msg_no_trace("ipv4 for epics"));
        };
        if trigger.contains(&cmd.name.as_str()) {
            debug!("handle_add_channel_with_addr  {cmd:?}");
        }
        let ch = Channel::new(cmd.name.clone());
        if let Some(chst) = self.channel_states.inner().get_mut(&ch) {
            if let ChannelStateValue::Active(ast) = &mut chst.value {
                if let ActiveChannelState::WithStatusSeriesId {
                    status_series_id: _,
                    state: st3,
                } = ast
                {
                    self.stats.handle_add_channel_with_addr().inc();
                    let tsnow = SystemTime::now();
                    *st3 = WithStatusSeriesIdState {
                        addr_find_backoff: 0,
                        inner: WithStatusSeriesIdStateInner::WithAddress {
                            addr: addr_v4,
                            state: WithAddressState::Assigned(ConnectionState {
                                updated: tsnow,
                                health_update_count: 0,
                                value: ConnectionStateValue::Unknown,
                            }),
                        },
                    };
                    if !self.ca_conn_ress.contains_key(&cmd.addr) {
                        let c = self.create_ca_conn(cmd.clone())?;
                        self.ca_conn_ress.insert(cmd.addr, c);
                    }
                    let conn_ress = self.ca_conn_ress.get_mut(&cmd.addr).unwrap();
                    let cmd = ConnCommand::channel_add(cmd.name, cmd.cssid);
                    conn_ress.cmd_queue.push_back(cmd);
                }
            }
        }
        Ok(())
    }

    fn handle_remove_channel(&mut self, cmd: ChannelRemove) -> Result<(), Error> {
        if self.shutdown_stopping {
            return Ok(());
        }
        let ch = Channel::new(cmd.name);
        if let Some(k) = self.channel_states.inner().get_mut(&ch) {
            match &k.value {
                ChannelStateValue::Active(j) => match j {
                    ActiveChannelState::Init { .. } => {
                        k.value = ChannelStateValue::ToRemove { addr: None };
                    }
                    ActiveChannelState::WaitForStatusSeriesId { .. } => {
                        k.value = ChannelStateValue::ToRemove { addr: None };
                    }
                    ActiveChannelState::WithStatusSeriesId {
                        status_series_id: _,
                        state,
                    } => match &state.inner {
                        WithStatusSeriesIdStateInner::UnknownAddress { .. } => {
                            k.value = ChannelStateValue::ToRemove { addr: None };
                        }
                        WithStatusSeriesIdStateInner::AddrSearchPending { .. } => {
                            k.value = ChannelStateValue::ToRemove { addr: None };
                        }
                        WithStatusSeriesIdStateInner::WithAddress { addr, state: _ } => {
                            k.value = ChannelStateValue::ToRemove {
                                addr: Some(addr.clone()),
                            };
                        }
                        WithStatusSeriesIdStateInner::NoAddress { .. } => {
                            k.value = ChannelStateValue::ToRemove { addr: None };
                        }
                        WithStatusSeriesIdStateInner::MaybeWrongAddress { .. } => {
                            k.value = ChannelStateValue::ToRemove { addr: None };
                        }
                    },
                },
                ChannelStateValue::ToRemove { .. } => {}
            }
        }
        Ok(())
    }

    fn handle_ioc_query_result(&mut self, results: VecDeque<FindIocRes>) -> Result<(), Error> {
        if self.shutdown_stopping {
            return Ok(());
        }
        trace3!("handle_ioc_query_result");
        for res in results {
            let ch = Channel::new(res.channel.clone());
            if trigger.contains(&ch.id()) {
                debug!("handle_ioc_query_result  {res:?}");
            }
            if let Some(chst) = self.channel_states.inner().get_mut(&ch) {
                if let ChannelStateValue::Active(ast) = &mut chst.value {
                    if let ActiveChannelState::WithStatusSeriesId {
                        status_series_id,
                        state,
                    } = ast
                    {
                        if let Some(addr) = res.addr {
                            self.stats.ioc_addr_found().inc();
                            trace3!("ioc found {res:?}");
                            let since = SystemTime::now();
                            state.addr_find_backoff = 0;
                            state.inner = WithStatusSeriesIdStateInner::WithAddress {
                                addr,
                                state: WithAddressState::Unassigned { since },
                            };
                            if false {
                                let cmd = ChannelAddWithAddr {
                                    backend: self.backend.clone(),
                                    name: res.channel,
                                    addr: SocketAddr::V4(addr),
                                    cssid: status_series_id.clone(),
                                    local_epics_hostname: self.local_epics_hostname.clone(),
                                };
                                self.handle_add_channel_with_addr(cmd)?;
                            }
                        } else {
                            self.stats.ioc_addr_not_found().inc();
                            trace3!("ioc not found {res:?}");
                            let since = SystemTime::now();
                            state.inner = WithStatusSeriesIdStateInner::UnknownAddress { since };
                        }
                    } else {
                        self.stats.ioc_addr_result_for_unknown_channel().inc();
                        warn!("TODO got address but no longer active");
                    }
                } else {
                    self.stats.ioc_addr_result_for_unknown_channel().inc();
                    warn!("TODO got address but no longer active");
                }
            } else {
                self.stats.ioc_addr_result_for_unknown_channel().inc();
                warn!("ioc addr lookup done but channel no longer here");
            }
        }
        Ok(())
    }

    fn handle_check_health(&mut self, ts1: Instant) -> Result<(), Error> {
        trace2!("handle_check_health");
        if self.shutdown_stopping {
            return Ok(());
        }
        if false {
            self.thr_msg_storage_len
                .trigger("connset handle_check_health", &[&self.storage_insert_sender.len()]);
        }
        self.check_channel_states()?;

        // Trigger already the next health check, but use the current data that we have.
        // TODO do the full check before sending the reply to daemon.
        for (_, res) in self.ca_conn_ress.iter_mut() {
            let item = ConnCommand::check_health();
            res.cmd_queue.push_back(item);
            trace2!(
                "handle_check_health pushed check command  {:?}  {:?}",
                res.cmd_queue.len(),
                res.sender.len()
            );
        }

        let ts2 = Instant::now();
        let item = CaConnSetItem::Healthy(ts1, ts2);
        self.connset_out_queue.push_back(item);
        Ok(())
    }

    fn handle_channel_statuses_req(&mut self, req: ChannelStatusesRequest) -> Result<(), Error> {
        if self.shutdown_stopping {
            return Ok(());
        }
        debug!("handle_channel_statuses_req");
        let reg1 = regex::Regex::new(&req.name)?;
        let channels_ca_conn_set = self
            .channel_states
            .inner()
            .iter()
            .filter(|(k, _)| reg1.is_match(k.id()))
            .map(|(k, v)| (k.id().to_string(), v.clone()))
            .collect();
        let item = ChannelStatusesResponse { channels_ca_conn_set };
        if req.tx.try_send(item).is_err() {
            self.stats.response_tx_fail.inc();
        }
        Ok(())
    }

    fn handle_shutdown(&mut self) -> Result<(), Error> {
        if self.shutdown_stopping {
            return Ok(());
        }
        debug!("handle_shutdown");
        self.shutdown_stopping = true;
        self.find_ioc_res_rx.close();
        self.channel_info_query_sender.as_mut().drop();
        self.channel_info_query_tx = None;
        self.find_ioc_query_sender.as_mut().drop();
        for (_addr, res) in self.ca_conn_ress.iter() {
            let item = ConnCommand::shutdown();
            // TODO not the nicest
            let mut tx = res.sender.clone();
            tokio::spawn(async move { tx.as_mut().send_async_pin(item).await });
        }
        Ok(())
    }

    fn handle_conn_command_result(&mut self, addr: SocketAddr, res: ConnCommandResult) -> Result<(), Error> {
        use crate::ca::conn::ConnCommandResultKind::*;
        match res.kind {
            CheckHealth(res) => self.apply_ca_conn_health_update(addr, res),
        }
    }

    fn apply_ca_conn_health_update(&mut self, addr: SocketAddr, res: CheckHealthResult) -> Result<(), Error> {
        trace2!("apply_ca_conn_health_update  {addr}");
        let tsnow = SystemTime::now();
        self.rogue_channel_count = 0;
        for (k, v) in res.channel_statuses {
            let ch = Channel::new(k);
            if let Some(st1) = self.channel_states.inner().get_mut(&ch) {
                if let ChannelStateValue::Active(st2) = &mut st1.value {
                    if let ActiveChannelState::WithStatusSeriesId {
                        status_series_id: _,
                        state: st3,
                    } = st2
                    {
                        if let WithStatusSeriesIdStateInner::WithAddress {
                            addr: conn_addr,
                            state: st4,
                        } = &mut st3.inner
                        {
                            if SocketAddr::V4(*conn_addr) != addr {
                                self.rogue_channel_count += 1;
                            }
                            if let WithAddressState::Assigned(st5) = st4 {
                                st5.updated = tsnow;
                                st5.health_update_count += 1;
                                st5.value = ConnectionStateValue::ChannelStateInfo(v);
                            } else {
                                self.rogue_channel_count += 1;
                            }
                        } else {
                            self.rogue_channel_count += 1;
                        }
                    } else {
                        self.rogue_channel_count += 1;
                    }
                } else {
                    self.rogue_channel_count += 1;
                }
            } else {
                self.rogue_channel_count += 1;
            }
        }
        self.stats.channel_rogue.set(self.rogue_channel_count);
        Ok(())
    }

    fn handle_channel_create_fail(&mut self, addr: SocketAddr, name: String) -> Result<(), Error> {
        trace!("handle_channel_create_fail {addr} {name}");
        let tsnow = SystemTime::now();
        let ch = Channel::new(name);
        if let Some(st1) = self.channel_states.inner().get_mut(&ch) {
            if let ChannelStateValue::Active(st2) = &mut st1.value {
                if let ActiveChannelState::WithStatusSeriesId {
                    status_series_id: _,
                    state: st3,
                } = st2
                {
                    trace!("handle_channel_create_fail {addr} {ch:?}  set to MaybeWrongAddress");
                    st3.addr_find_backoff += 1;
                    st3.inner = WithStatusSeriesIdStateInner::MaybeWrongAddress { since: tsnow };
                }
            }
        }
        Ok(())
    }

    fn handle_ca_conn_eos(&mut self, addr: SocketAddr) -> Result<(), Error> {
        trace2!("handle_ca_conn_eos {addr}");
        if let Some(e) = self.ca_conn_ress.remove(&addr) {
            self.stats.ca_conn_eos_ok().inc();
            self.await_ca_conn_jhs.push_back((addr, e.jh));
        } else {
            self.stats.ca_conn_eos_unexpected().inc();
            warn!("end-of-stream received for non-existent CaConn {addr}");
        }
        self.remove_status_for_addr(addr)?;
        trace2!("still CaConn left  {}", self.ca_conn_ress.len());
        Ok(())
    }

    fn handle_connect_fail(&mut self, addr: SocketAddr) -> Result<(), Error> {
        trace2!("handle_connect_fail {addr}");
        let tsnow = SystemTime::now();
        for (ch, st1) in self.channel_states.inner().iter_mut() {
            match &mut st1.value {
                ChannelStateValue::Active(st2) => match st2 {
                    ActiveChannelState::Init { since: _ } => {}
                    ActiveChannelState::WaitForStatusSeriesId { since: _ } => {}
                    ActiveChannelState::WithStatusSeriesId {
                        status_series_id: _,
                        state: st3,
                    } => {
                        if let WithStatusSeriesIdStateInner::WithAddress {
                            addr: addr_ch,
                            state: _st4,
                        } = &mut st3.inner
                        {
                            if SocketAddr::V4(*addr_ch) == addr {
                                if trigger.contains(&ch.id()) {
                                    self.connect_fail_count += 1;
                                    debug!(" connect fail, maybe wrong address for {} {}", addr, ch.id());
                                }
                                if self.connect_fail_count > 400 {
                                    std::process::exit(1);
                                }
                                st3.addr_find_backoff += 1;
                                st3.inner = WithStatusSeriesIdStateInner::MaybeWrongAddress { since: tsnow };
                            }
                        }
                    }
                },
                ChannelStateValue::ToRemove { addr: _ } => {}
            }
        }
        Ok(())
    }

    fn remove_status_for_addr(&mut self, addr: SocketAddr) -> Result<(), Error> {
        Ok(())
    }

    fn ready_for_end_of_stream(&self) -> bool {
        if !self.shutdown_stopping {
            false
        } else if self.ca_conn_ress.len() > 0 {
            false
        } else if self.await_ca_conn_jhs.len() > 0 {
            false
        } else {
            true
        }
    }

    fn create_ca_conn(&self, add: ChannelAddWithAddr) -> Result<CaConnRes, Error> {
        // TODO should we save this as event?
        let opts = CaConnOpts::default();
        let addr = add.addr;
        let addr_v4 = if let SocketAddr::V4(x) = add.addr {
            x
        } else {
            return Err(Error::with_msg_no_trace("only ipv4 for epics"));
        };
        self.stats.create_ca_conn().inc();
        let conn = CaConn::new(
            opts,
            add.backend.clone(),
            addr_v4,
            add.local_epics_hostname,
            self.storage_insert_tx.as_ref().get_ref().clone(),
            self.channel_info_query_tx
                .clone()
                .ok_or_else(|| Error::with_msg_no_trace("no more channel_info_query_tx available"))?,
            self.ca_conn_stats.clone(),
            self.ca_proto_stats.clone(),
        );
        let conn_tx = conn.conn_command_tx();
        let conn_stats = conn.stats();
        let tx1 = self.ca_conn_res_tx.as_ref().get_ref().clone();
        let tx2 = self.storage_insert_tx.as_ref().get_ref().clone();
        let jh = tokio::spawn(Self::ca_conn_item_merge(conn, tx1, tx2, addr, self.stats.clone()));
        let ca_conn_res = CaConnRes {
            state: CaConnState::new(CaConnStateValue::Fresh),
            sender: Box::pin(conn_tx.into()),
            stats: conn_stats,
            cmd_queue: VecDeque::new(),
            jh,
        };
        Ok(ca_conn_res)
    }

    async fn ca_conn_item_merge(
        conn: CaConn,
        tx1: Sender<(SocketAddr, CaConnEvent)>,
        tx2: Sender<VecDeque<QueryItem>>,
        addr: SocketAddr,
        stats: Arc<CaConnSetStats>,
    ) -> Result<(), Error> {
        stats.ca_conn_task_begin().inc();
        trace2!("ca_conn_consumer  begin  {}", addr);
        let connstats = conn.stats();
        let mut conn = Box::pin(conn);
        let mut ret = Ok(());
        while let Some(item) = conn.next().await {
            match item {
                Ok(item) => {
                    connstats.item_count.inc();
                    match item.value {
                        CaConnEventValue::QueryItem(x) => {
                            warn!("ca_conn_item_merge should not go here often");
                            let mut v = VecDeque::new();
                            v.push_back(x);
                            if let Err(_) = tx2.send(v).await {
                                break;
                            }
                        }
                        CaConnEventValue::None => {
                            if let Err(_) = tx1.send((addr, item)).await {
                                break;
                            }
                        }
                        CaConnEventValue::EchoTimeout => {
                            if let Err(_) = tx1.send((addr, item)).await {
                                break;
                            }
                        }
                        CaConnEventValue::ConnCommandResult(_) => {
                            if let Err(_) = tx1.send((addr, item)).await {
                                break;
                            }
                        }
                        CaConnEventValue::ChannelCreateFail(_) => {
                            if let Err(_) = tx1.send((addr, item)).await {
                                break;
                            }
                        }
                        CaConnEventValue::EndOfStream => {
                            if let Err(_) = tx1.send((addr, item)).await {
                                break;
                            }
                        }
                        CaConnEventValue::ConnectFail => {
                            if let Err(_) = tx1.send((addr, item)).await {
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("CaConn gives error: {e:?}");
                    ret = Err(e);
                    break;
                }
            }
        }
        trace2!("ca_conn_consumer  ended {}", addr);
        tx1.send((
            addr,
            CaConnEvent {
                ts: Instant::now(),
                value: CaConnEventValue::EndOfStream,
            },
        ))
        .await?;
        trace2!("ca_conn_consumer  signaled {}", addr);
        stats.ca_conn_task_done().inc();
        ret
    }

    fn push_channel_status(&mut self, item: ChannelStatusItem) -> Result<(), Error> {
        if false {
            let _ = ChannelInfoItem {
                ts_msp: todo!(),
                series: todo!(),
                ivl: todo!(),
                interest: todo!(),
                evsize: todo!(),
            };
        }
        let item = QueryItem::ChannelStatus(item);
        let mut v = VecDeque::new();
        v.push_back(item);
        self.storage_insert_queue.push_back(v);
        Ok(())
    }

    #[allow(unused)]
    async fn __enqueue_command_to_all<F>(&self, cmdgen: F) -> Result<Vec<CmdId>, Error>
    where
        F: Fn() -> ConnCommand,
    {
        let mut senders: Vec<(SocketAddrV4, Sender<ConnCommand>)> = err::todoval();
        let mut cmdids = Vec::new();
        for (addr, sender) in senders {
            let cmd = cmdgen();
            let cmdid = cmd.id();
            match sender.send(cmd).await {
                Ok(()) => {
                    cmdids.push(CmdId(addr, cmdid));
                }
                Err(e) => {
                    error!("enqueue_command_to_all  can not send command {e:?}  {:?}", e.0);
                }
            }
        }
        Ok(cmdids)
    }

    #[allow(unused)]
    async fn __send_command_to_addr_disabled<F, R>(&self, addr: &SocketAddrV4, cmdgen: F) -> Result<R, Error>
    where
        F: Fn() -> (ConnCommand, async_channel::Receiver<R>),
    {
        let tx: Sender<ConnCommand> = err::todoval();
        let (cmd, rx) = cmdgen();
        tx.send(cmd).await.err_conv()?;
        let ret = rx.recv().await.err_conv()?;
        Ok(ret)
    }

    #[allow(unused)]
    async fn __send_command_inner_disabled<'a, IT, F, R>(it: &mut IT, cmdgen: F) -> Vec<async_channel::Receiver<R>>
    where
        IT: Iterator<Item = (&'a SocketAddrV4, &'a async_channel::Sender<ConnCommand>)>,
        F: Fn() -> (ConnCommand, async_channel::Receiver<R>),
    {
        let mut rxs = Vec::new();
        for (_, tx) in it {
            let (cmd, rx) = cmdgen();
            match tx.send(cmd).await {
                Ok(()) => {
                    rxs.push(rx);
                }
                Err(e) => {
                    error!("can not send command {e:?}");
                }
            }
        }
        rxs
    }

    async fn wait_stopped(&self) -> Result<(), Error> {
        warn!("Lock for wait_stopped");
        // let mut g = self.ca_conn_ress.lock().await;
        // let mm = std::mem::replace(&mut *g, BTreeMap::new());
        let mm: BTreeMap<SocketAddrV4, JoinHandle<Result<(), Error>>> = BTreeMap::new();
        let mut jhs: VecDeque<_> = VecDeque::new();
        for t in mm {
            jhs.push_back(t.1.fuse());
        }
        loop {
            let mut jh = if let Some(x) = jhs.pop_front() {
                x
            } else {
                break;
            };
            futures_util::select! {
                a = jh => match a {
                    Ok(k) => match k {
                        Ok(_) => {}
                        Err(e) => {
                            error!("{e:?}");
                        }
                    },
                    Err(e) => {
                        error!("{e:?}");
                    }
                },
                _b = crate::rt::sleep(Duration::from_millis(1000)).fuse() => {
                    jhs.push_back(jh);
                    info!("waiting for {} connections", jhs.len());
                }
            };
        }
        Ok(())
    }

    fn check_connection_states(&mut self) -> Result<(), Error> {
        let tsnow = Instant::now();
        for (addr, val) in &mut self.ca_conn_ress {
            let state = &mut val.state;
            let v = &mut state.value;
            match v {
                CaConnStateValue::Fresh => {
                    // TODO check for delta t since last issued status command.
                    if tsnow.duration_since(state.last_feedback) > Duration::from_millis(20000) {
                        error!("TODO Fresh timeout send connection-close for {addr}");
                        // TODO collect in metrics
                        // self.stats.ca_conn_status_feedback_timeout.inc();
                        // TODO send shutdown to this CaConn, check that we've received
                        // a 'shutdown' state from it. (see below)
                        *v = CaConnStateValue::Shutdown { since: tsnow };
                    }
                }
                CaConnStateValue::HadFeedback => {
                    // TODO check for delta t since last issued status command.
                    if tsnow.duration_since(state.last_feedback) > Duration::from_millis(20000) {
                        error!("TODO HadFeedback timeout send connection-close for {addr}");
                        // TODO collect in metrics
                        // self.stats.ca_conn_status_feedback_timeout.inc();
                        *v = CaConnStateValue::Shutdown { since: tsnow };
                    }
                }
                CaConnStateValue::Shutdown { since } => {
                    if tsnow.saturating_duration_since(*since) > Duration::from_millis(10000) {
                        // TODO collect in metrics as severe error, this would be a bug.
                        // self.stats.critical_error.inc();
                        error!("Shutdown of CaConn failed for {addr}");
                    }
                }
            }
        }
        Ok(())
    }

    fn check_channel_states(&mut self) -> Result<(), Error> {
        let (mut search_pending_count, mut assigned_without_health_update) = self.update_channel_state_counts();
        let mut cmd_remove_channel = Vec::new();
        let mut cmd_add_channel = Vec::new();
        let mut channel_status_items = Vec::new();
        let k = self.chan_check_next.take();
        let it = if let Some(last) = k {
            trace!("check_chans  start at {:?}", last);
            self.channel_states.inner().range_mut(last..)
        } else {
            self.channel_states.inner().range_mut(..)
        };

        let tsnow = SystemTime::now();
        for (i, (ch, st)) in it.enumerate() {
            match &mut st.value {
                ChannelStateValue::Active(st2) => match st2 {
                    ActiveChannelState::Init { since: _ } => {
                        // TODO no longer used? remove?
                        self.stats.logic_error().inc();
                    }
                    ActiveChannelState::WaitForStatusSeriesId { since } => {
                        let dt = tsnow.duration_since(*since).unwrap_or(Duration::ZERO);
                        if dt > Duration::from_millis(5000) {
                            warn!("timeout can not get status series id for {ch:?}");
                            *st2 = ActiveChannelState::Init { since: tsnow };
                        } else {
                            // TODO
                        }
                    }
                    ActiveChannelState::WithStatusSeriesId {
                        status_series_id,
                        state,
                    } => match &mut state.inner {
                        WithStatusSeriesIdStateInner::UnknownAddress { since } => {
                            if search_pending_count < CURRENT_SEARCH_PENDING_MAX as _ {
                                if since.checked_add(UNKNOWN_ADDRESS_STAY).unwrap() < tsnow {
                                    if false {
                                        // TODO
                                        search_pending_count += 1;
                                        state.inner = WithStatusSeriesIdStateInner::AddrSearchPending { since: tsnow };
                                    }
                                }
                            }
                        }
                        WithStatusSeriesIdStateInner::AddrSearchPending { since } => {
                            let dt = tsnow.duration_since(*since).unwrap_or(Duration::ZERO);
                            if dt > SEARCH_PENDING_TIMEOUT {
                                debug!("TODO should receive some error indication instead of timeout for {ch:?}");
                                state.inner = WithStatusSeriesIdStateInner::NoAddress { since: tsnow };
                                search_pending_count -= 1;
                            }
                        }
                        WithStatusSeriesIdStateInner::WithAddress {
                            addr: addr_v4,
                            state: st3,
                        } => {
                            use WithAddressState::*;
                            match st3 {
                                Unassigned { since } => {
                                    if assigned_without_health_update < CHANNEL_MAX_WITHOUT_HEALTH_UPDATE as _ {
                                        if *since + CHANNEL_UNASSIGNED_TIMEOUT < tsnow {
                                            assigned_without_health_update += 1;
                                            let cmd = ChannelAddWithAddr {
                                                backend: self.backend.clone(),
                                                name: ch.id().into(),
                                                local_epics_hostname: self.local_epics_hostname.clone(),
                                                cssid: status_series_id.clone(),
                                                addr: SocketAddr::V4(*addr_v4),
                                            };
                                            cmd_add_channel.push(cmd);
                                        }
                                    }
                                }
                                Assigned(st4) => {
                                    if st4.updated + CHANNEL_HEALTH_TIMEOUT < tsnow {
                                        self.stats.channel_health_timeout().inc();
                                        trace!("health timeout  channel {ch:?}  ~~~~~~~~~~~~~~~~~~~");
                                        // TODO
                                        error!("health timeout  channel {ch:?}  ~~~~~~~~~~~~~~~~~~~");
                                        std::process::exit(1);
                                        let addr = SocketAddr::V4(*addr_v4);
                                        cmd_remove_channel.push((addr, ch.clone()));
                                        if st.health_timeout_count < 3 {
                                            state.addr_find_backoff += 1;
                                            state.inner =
                                                WithStatusSeriesIdStateInner::MaybeWrongAddress { since: tsnow };
                                            let item = ChannelStatusItem::new_closed_conn_timeout(
                                                tsnow,
                                                status_series_id.clone(),
                                            );
                                            channel_status_items.push(item);
                                        }
                                    }
                                }
                            }
                        }
                        WithStatusSeriesIdStateInner::NoAddress { since } => {
                            if *since + NO_ADDRESS_STAY < tsnow {
                                state.inner = WithStatusSeriesIdStateInner::UnknownAddress { since: tsnow };
                            }
                        }
                        WithStatusSeriesIdStateInner::MaybeWrongAddress { since } => {
                            if *since + (MAYBE_WRONG_ADDRESS_STAY * state.addr_find_backoff.min(10).max(1)) < tsnow {
                                if search_pending_count < CURRENT_SEARCH_PENDING_MAX as _ {
                                    if trigger.contains(&ch.id()) {
                                        debug!("issue ioc search for {}", ch.id());
                                    }
                                    search_pending_count += 1;
                                    state.inner = WithStatusSeriesIdStateInner::AddrSearchPending { since: tsnow };
                                    let qu = IocAddrQuery::uncached(ch.id().into());
                                    self.find_ioc_query_queue.push_back(qu);
                                    self.stats.ioc_search_start().inc();
                                }
                            }
                        }
                    },
                },
                ChannelStateValue::ToRemove { .. } => {
                    // TODO if assigned to some address,
                }
            }
            if i >= CHECK_CHANS_PER_TICK {
                self.chan_check_next = Some(ch.clone());
                break;
            }
        }
        for item in channel_status_items {
            self.push_channel_status(item)?;
        }
        for (addr, ch) in cmd_remove_channel {
            if let Some(g) = self.ca_conn_ress.get_mut(&addr) {
                let cmd = ConnCommand::channel_remove(ch.id().into());
                g.cmd_queue.push_back(cmd);
            }
            let cmd = ChannelRemove { name: ch.id().into() };
            self.handle_remove_channel(cmd)?;
        }
        for cmd in cmd_add_channel {
            self.handle_add_channel_with_addr(cmd)?;
        }
        Ok(())
    }

    // TODO should use both counters and values
    fn update_channel_state_counts(&mut self) -> (u64, u64) {
        let mut unknown_address = 0;
        let mut search_pending = 0;
        let mut no_address = 0;
        let mut unassigned = 0;
        let mut assigned = 0;
        let mut connected = 0;
        let mut maybe_wrong_address = 0;
        let mut assigned_without_health_update = 0;
        for (_ch, st) in self.channel_states.inner().iter() {
            match &st.value {
                ChannelStateValue::Active(st2) => match st2 {
                    ActiveChannelState::Init { .. } => {
                        unknown_address += 1;
                    }
                    ActiveChannelState::WaitForStatusSeriesId { .. } => {
                        unknown_address += 1;
                    }
                    ActiveChannelState::WithStatusSeriesId { state, .. } => match &state.inner {
                        WithStatusSeriesIdStateInner::UnknownAddress { .. } => {
                            unknown_address += 1;
                        }
                        WithStatusSeriesIdStateInner::AddrSearchPending { .. } => {
                            search_pending += 1;
                        }
                        WithStatusSeriesIdStateInner::WithAddress { state, .. } => match state {
                            WithAddressState::Unassigned { .. } => {
                                unassigned += 1;
                            }
                            WithAddressState::Assigned(st3) => {
                                if st3.health_update_count == 0 {
                                    assigned_without_health_update += 1;
                                }
                                match &st3.value {
                                    ConnectionStateValue::Unknown => {
                                        assigned += 1;
                                    }
                                    ConnectionStateValue::ChannelStateInfo(_) => {
                                        connected += 1;
                                    }
                                }
                            }
                        },
                        WithStatusSeriesIdStateInner::NoAddress { .. } => {
                            no_address += 1;
                        }
                        WithStatusSeriesIdStateInner::MaybeWrongAddress { .. } => {
                            maybe_wrong_address += 1;
                        }
                    },
                },
                ChannelStateValue::ToRemove { .. } => {
                    unassigned += 1;
                }
            }
        }
        self.stats.channel_unknown_address.set(unknown_address);
        self.stats.channel_search_pending.set(search_pending);
        self.stats.channel_no_address.set(no_address);
        self.stats.channel_unassigned.set(unassigned);
        self.stats.channel_assigned.set(assigned);
        self.stats.channel_connected.set(connected);
        self.stats.channel_maybe_wrong_address.set(maybe_wrong_address);
        self.stats
            .channel_assigned_without_health_update
            .set(assigned_without_health_update);
        (search_pending, assigned_without_health_update)
    }

    fn try_push_ca_conn_cmds(&mut self, cx: &mut Context) -> Result<(), Error> {
        use Poll::*;
        for (_, v) in self.ca_conn_ress.iter_mut() {
            let tx = &mut v.sender;
            loop {
                if false {
                    if v.cmd_queue.len() != 0 || tx.is_sending() {
                        debug!("try_push_ca_conn_cmds  {:?}  {:?}", v.cmd_queue.len(), tx.len());
                    }
                }
                break if tx.is_sending() {
                    match tx.poll_unpin(cx) {
                        Ready(Ok(())) => {
                            self.stats.try_push_ca_conn_cmds_sent.inc();
                            continue;
                        }
                        Ready(Err(e)) => {
                            error!("try_push_ca_conn_cmds {e}");
                            return Err(Error::with_msg_no_trace(format!("{e}")));
                        }
                        Pending => (),
                    }
                } else if let Some(item) = v.cmd_queue.pop_front() {
                    tx.as_mut().send_pin(item);
                    continue;
                } else {
                    ()
                };
            }
        }
        Ok(())
    }
}

impl Stream for CaConnSet {
    type Item = CaConnSetItem;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        trace4!("CaConnSet  poll  begin");
        use Poll::*;
        self.stats.poll_fn_begin().inc();
        let res = loop {
            trace4!("CaConnSet  poll  loop");
            self.stats.poll_loop_begin().inc();

            self.stats.storage_insert_tx_len.set(self.storage_insert_tx.len() as _);
            self.stats
                .storage_insert_queue_len
                .set(self.storage_insert_queue.len() as _);
            self.stats
                .channel_info_query_queue_len
                .set(self.channel_info_query_queue.len() as _);
            self.stats
                .channel_info_query_sender_len
                .set(self.channel_info_query_sender.len().unwrap_or(0) as _);
            self.stats
                .channel_info_res_tx_len
                .set(self.channel_info_res_tx.len() as _);
            self.stats
                .find_ioc_query_sender_len
                .set(self.find_ioc_query_sender.len().unwrap_or(0) as _);
            self.stats.ca_conn_res_tx_len.set(self.ca_conn_res_tx.len() as _);

            let mut have_pending = false;
            let mut have_progress = false;

            if let Err(e) = self.try_push_ca_conn_cmds(cx) {
                break Ready(Some(CaConnSetItem::Error(e)));
            }

            if let Some(item) = self.connset_out_queue.pop_front() {
                break Ready(Some(item));
            }

            if let Some((addr, jh)) = self.await_ca_conn_jhs.front_mut() {
                match jh.poll_unpin(cx) {
                    Ready(x) => {
                        let addr = *addr;
                        self.await_ca_conn_jhs.pop_front();
                        let left = self.await_ca_conn_jhs.len();
                        match x {
                            Ok(Ok(())) => {
                                self.stats.ca_conn_task_join_done_ok.inc();
                                debug!("CaConn {addr} finished well  left {left}");
                            }
                            Ok(Err(e)) => {
                                self.stats.ca_conn_task_join_done_err.inc();
                                error!("CaConn {addr} task error: {e}  left {left}");
                            }
                            Err(e) => {
                                self.stats.ca_conn_task_join_err.inc();
                                error!("CaConn {addr} join error: {e}  left {left}");
                            }
                        }
                        have_progress = true;
                    }
                    Pending => {
                        have_pending = true;
                    }
                }
            }

            if self.storage_insert_sender.is_idle() {
                if let Some(item) = self.storage_insert_queue.pop_front() {
                    self.stats.logic_error().inc();
                    self.storage_insert_sender.as_mut().send_pin(item);
                }
            }
            if self.storage_insert_sender.is_sending() {
                match self.storage_insert_sender.poll_unpin(cx) {
                    Ready(Ok(())) => {
                        have_progress = true;
                    }
                    Ready(Err(_)) => {
                        let e = Error::with_msg_no_trace("can not send into channel");
                        error!("{e}");
                        break Ready(Some(CaConnSetItem::Error(e)));
                    }
                    Pending => {
                        have_pending = true;
                    }
                }
            }

            if self.find_ioc_query_sender.is_idle() {
                if let Some(item) = self.find_ioc_query_queue.pop_front() {
                    self.find_ioc_query_sender.as_mut().send_pin(item);
                }
            }
            if self.find_ioc_query_sender.is_sending() {
                match self.find_ioc_query_sender.poll_unpin(cx) {
                    Ready(Ok(())) => {
                        have_progress = true;
                    }
                    Ready(Err(_)) => {
                        let e = Error::with_msg_no_trace("can not send into channel");
                        error!("{e}");
                        break Ready(Some(CaConnSetItem::Error(e)));
                    }
                    Pending => {
                        have_pending = true;
                    }
                }
            }

            if self.channel_info_query_sender.is_idle() {
                if let Some(item) = self.channel_info_query_queue.pop_front() {
                    self.channel_info_query_sender.as_mut().send_pin(item);
                }
            }
            if self.channel_info_query_sender.is_sending() {
                match self.channel_info_query_sender.poll_unpin(cx) {
                    Ready(Ok(())) => {
                        have_progress = true;
                    }
                    Ready(Err(_)) => {
                        let e = Error::with_msg_no_trace("can not send into channel");
                        error!("{e}");
                        break Ready(Some(CaConnSetItem::Error(e)));
                    }
                    Pending => {
                        have_pending = true;
                    }
                }
            }

            match self.find_ioc_res_rx.as_mut().poll_next(cx) {
                Ready(Some(x)) => match self.handle_ioc_query_result(x) {
                    Ok(()) => {
                        have_progress = true;
                    }
                    Err(e) => break Ready(Some(CaConnSetItem::Error(e))),
                },
                Ready(None) => {}
                Pending => {
                    have_pending = true;
                }
            }

            match self.ca_conn_res_rx.as_mut().poll_next(cx) {
                Ready(Some((addr, ev))) => match self.handle_ca_conn_event(addr, ev) {
                    Ok(()) => {
                        have_progress = true;
                    }
                    Err(e) => break Ready(Some(CaConnSetItem::Error(e))),
                },
                Ready(None) => {}
                Pending => {
                    have_pending = true;
                }
            }

            match self.channel_info_res_rx.as_mut().poll_next(cx) {
                Ready(Some(x)) => match self.handle_series_lookup_result(x) {
                    Ok(()) => {
                        have_progress = true;
                    }
                    Err(e) => break Ready(Some(CaConnSetItem::Error(e))),
                },
                Ready(None) => {}
                Pending => {
                    have_pending = true;
                }
            }

            match self.connset_inp_rx.as_mut().poll_next(cx) {
                Ready(Some(x)) => match self.handle_event(x) {
                    Ok(()) => {
                        have_progress = true;
                    }
                    Err(e) => break Ready(Some(CaConnSetItem::Error(e))),
                },
                Ready(None) => {
                    warn!("connset_inp_rx broken?")
                }
                Pending => {
                    have_pending = true;
                }
            }

            break if self.ready_for_end_of_stream() {
                self.stats.ready_for_end_of_stream().inc();
                if have_progress {
                    self.stats.ready_for_end_of_stream_with_progress().inc();
                    continue;
                } else {
                    Ready(None)
                }
            } else {
                if have_progress {
                    self.stats.poll_reloop().inc();
                    continue;
                } else {
                    if have_pending {
                        self.stats.poll_pending().inc();
                        Pending
                    } else {
                        self.stats.poll_no_progress_no_pending().inc();
                        let e = Error::with_msg_no_trace("no progress no pending");
                        Ready(Some(CaConnSetItem::Error(e)))
                    }
                }
            };
        };
        trace4!("CaConnSet  poll  done");
        res
    }
}
