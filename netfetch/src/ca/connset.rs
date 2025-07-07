use super::conn::EndOfStreamReason;
use super::findioc::FindIocRes;
use crate::ca::conn;
use crate::ca::statemap;
use crate::ca::statemap::MaybeWrongAddressState;
use crate::ca::statemap::WithAddressState;
use crate::conf::CaIngestOpts;
use crate::conf::ChannelConfig;
use crate::daemon_common::ChannelName;
use crate::rt::JoinHandle;
use crate::throttletrace::ThrottleTrace;
use async_channel::Receiver;
use async_channel::Sender;
use conn::CaConn;
use conn::CaConnEvent;
use conn::CaConnEventValue;
use conn::CaConnOpts;
use conn::ChannelStateInfo;
use conn::ChannelStatusPartial;
use conn::ConnCommand;
use conn::ConnCommandResult;
use dbpg::seriesbychannel::BoxedSend;
use dbpg::seriesbychannel::CanSendChannelInfoResult;
use dbpg::seriesbychannel::ChannelInfoQuery;
use dbpg::seriesbychannel::ChannelInfoResult;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use hashbrown::HashMap;
use log::*;
use netpod::ScalarType;
use netpod::SeriesKind;
use netpod::Shape;
use scywr::iteminsertqueue::ChannelStatusItem;
use scywr::iteminsertqueue::QueryItem;
use scywr::senderpolling::SenderPolling;
use serde::Serialize;
use series::ChannelStatusSeriesId;
use statemap::ActiveChannelState;
use statemap::ChannelState;
use statemap::ChannelStateMap;
use statemap::ChannelStateValue;
use statemap::ConnectionState;
use statemap::ConnectionStateValue;
use statemap::WithStatusSeriesIdState;
use statemap::WithStatusSeriesIdStateInner;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::fmt;
use std::net::SocketAddr;
use std::net::SocketAddrV4;
use std::pin::Pin;

use crate::queueset::QueueSet;
use netpod::OnDrop;
use netpod::TsNano;
use scywr::insertqueues::InsertQueuesTx;
use series::SeriesId;
use serieswriter::msptool::fixgrid::MspSplitFixGrid;
use stats::mett::CaConnSetMetrics;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use taskrun::tokio;
use tracing::Instrument;

const CHECK_CHANS_PER_TICK: usize = 10000000;
pub const SEARCH_BATCH_MAX: usize = 64;
pub const CURRENT_SEARCH_PENDING_MAX: usize = SEARCH_BATCH_MAX * 4;
const NO_ADDRESS_STAY: Duration = Duration::from_millis(20000);
const SEARCH_PENDING_TIMEOUT: Duration = Duration::from_millis(30000);
const CHANNEL_HEALTH_TIMEOUT: Duration = Duration::from_millis(30000);
const CHANNEL_UNASSIGNED_TIMEOUT: Duration = Duration::from_millis(0);
const UNASSIGN_FOR_CONFIG_CHANGE_TIMEOUT: Duration = Duration::from_millis(1000 * 10);
const CHANNEL_MAX_WITHOUT_HEALTH_UPDATE: usize = 3000000;

macro_rules! trace2 { ($($arg:expr),*) => ( if false { trace!($($arg),*); } ); }

macro_rules! trace3 { ($($arg:expr),*) => ( if false { trace!($($arg),*); } ); }

macro_rules! trace4 { ($($arg:tt)*) => { if false { trace!($($arg)*); } }; }

macro_rules! trace_health_update { ($($arg:tt)*) => { if false { trace!($($arg)*); } }; }

macro_rules! trace_channel_state { ($($arg:tt)*) => { if false { trace!($($arg)*); } }; }

autoerr::create_error_v1!(
    name(Error, "CaConnSet"),
    enum variants {
        ChannelSend,
        TaskJoin(#[from] tokio::task::JoinError),
        SeriesLookup(#[from] dbpg::seriesbychannel::Error),
        Beacons(#[from] crate::ca::beacons::Error),
        SeriesWriter(#[from] serieswriter::writer::Error),
        ExpectIpv4,
        UnknownCssid,
        Regex(#[from] regex::Error),
        MissingChannelInfoChannelTx,
        UnexpectedChannelDummyState,
        CaConnEndWithoutReason,
        PushCmdsNoSendInProgress(SocketAddr),
        SenderPollingSend,
        NoProgressNoPending,
        IocFinder(#[from] crate::ca::finder::Error),
        ChannelAssignedWithoutConnRess,
    },
);

impl<T> From<async_channel::SendError<T>> for Error {
    fn from(_value: async_channel::SendError<T>) -> Self {
        Self::ChannelSend
    }
}

impl<T> From<scywr::senderpolling::Error<T>> for Error {
    fn from(_value: scywr::senderpolling::Error<T>) -> Self {
        Self::SenderPollingSend
    }
}

impl From<Error> for ::err::Error {
    fn from(value: Error) -> Self {
        Self::from_string(value)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct CmdId(SocketAddrV4, usize);

pub struct CaConnRes {
    sender: Pin<Box<SenderPolling<ConnCommand>>>,
    cmd_queue: VecDeque<ConnCommand>,
    // TODO await on jh
    jh: JoinHandle<Result<(), Error>>,
}

#[derive(Debug, Clone)]
pub struct ChannelAddWithAddr {
    ch_cfg: ChannelConfig,
    cssid: ChannelStatusSeriesId,
    addr: SocketAddr,
}

#[derive(Debug, Clone)]
pub struct ChannelAddWithStatusId {
    ch_cfg: ChannelConfig,
    cssid: ChannelStatusSeriesId,
}

#[derive(Debug, Clone)]
pub struct ChannelConfigFlagReset {
    restx: crate::ca::conn::CmdResTx,
}

#[derive(Debug, Clone)]
pub struct ChannelConfigRemoveUnflagged {
    restx: crate::ca::conn::CmdResTx,
}

#[derive(Debug, Clone)]
pub struct ChannelAdd {
    ch_cfg: ChannelConfig,
    restx: crate::ca::conn::CmdResTx,
}

impl ChannelAdd {
    pub fn name(&self) -> &str {
        self.ch_cfg.name()
    }
}

#[derive(Debug, Clone)]
pub struct ChannelRemove {
    name: String,
}

pub struct ChannelStatusRequest {
    pub tx: Sender<ChannelStatusResponse>,
}

#[derive(Debug, Serialize)]
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

impl fmt::Debug for ChannelStatusesRequest {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("ChannelStatusesRequest").finish()
    }
}

#[derive(Debug, Serialize)]
pub struct ChannelStatusesResponse {
    pub channels_ca_conn_set: BTreeMap<String, ChannelState>,
}

pub struct ChannelStatusesPrivateRequest {
    pub name: String,
    pub limit: u64,
    pub tx: Sender<ChannelStatusesPrivateResponse>,
}

impl fmt::Debug for ChannelStatusesPrivateRequest {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("ChannelStatusesPrivateRequest").finish()
    }
}

#[derive(Debug, Serialize)]
pub struct ChannelStatusesPrivateResponse {
    pub channels_ca_conn_set: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug)]
pub struct ChannelCommand {
    pub channel: String,
    pub conn_command: ConnCommand,
}

#[derive(Debug)]
pub enum ConnSetCmd {
    ChannelConfigFlagReset(ChannelConfigFlagReset),
    ChannelConfigRemoveUnflagged(ChannelConfigRemoveUnflagged),
    ChannelAdd(ChannelAdd),
    ChannelRemove(ChannelRemove),
    Shutdown,
    ChannelStatuses(ChannelStatusesRequest),
    ChannelStatusesPrivate(ChannelStatusesPrivateRequest),
    // TODO rename to ConnCommand because it must be handled by some specific Conn
    ChannelCommand(ChannelCommand),
}

#[derive(Debug)]
pub enum CaConnSetEvent {
    ConnSetCmd(ConnSetCmd),
}

impl CaConnSetEvent {
    // pub fn new_cmd_channel_statuses() -> (Self, Receiver) {}
}

#[derive(Debug)]
pub enum CaConnSetItem {
    Error(Error),
    Healthy,
    Metrics(stats::mett::CaConnSetMetrics),
}

pub struct CaConnSetCtrl {
    tx: Sender<CaConnSetEvent>,
    rx: Receiver<CaConnSetItem>,
    jh: JoinHandle<Result<(), Error>>,
}

impl CaConnSetCtrl {
    pub fn sender(&self) -> Sender<CaConnSetEvent> {
        self.tx.clone()
    }

    pub fn receiver(&self) -> Receiver<CaConnSetItem> {
        self.rx.clone()
    }

    pub async fn channel_config_flag_reset(&self, restx: crate::ca::conn::CmdResTx) -> Result<(), Error> {
        let cmd = ChannelConfigFlagReset { restx };
        let cmd = ConnSetCmd::ChannelConfigFlagReset(cmd);
        self.tx.send(CaConnSetEvent::ConnSetCmd(cmd)).await?;
        Ok(())
    }

    pub async fn channel_config_remove_unflagged(&self, restx: crate::ca::conn::CmdResTx) -> Result<(), Error> {
        let cmd = ChannelConfigRemoveUnflagged { restx };
        let cmd = ConnSetCmd::ChannelConfigRemoveUnflagged(cmd);
        self.tx.send(CaConnSetEvent::ConnSetCmd(cmd)).await?;
        Ok(())
    }

    pub async fn add_channel(&self, ch_cfg: ChannelConfig, restx: crate::ca::conn::CmdResTx) -> Result<(), Error> {
        let cmd = ChannelAdd { ch_cfg, restx };
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

    pub async fn send_channel_command(&self, cmd: ChannelCommand) -> Result<(), Error> {
        self.tx
            .send(CaConnSetEvent::ConnSetCmd(ConnSetCmd::ChannelCommand(cmd)))
            .await?;
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), Error> {
        let cmd = ConnSetCmd::Shutdown;
        self.tx.send(CaConnSetEvent::ConnSetCmd(cmd)).await?;
        Ok(())
    }

    pub async fn join(self) -> Result<(), Error> {
        self.jh.await??;
        Ok(())
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

fn bump_backoff(x: &mut u32) {
    *x = (1 + *x).min(20);
}

struct SeriesLookupSender {
    tx: Sender<Result<ChannelInfoResult, Error>>,
}

impl CanSendChannelInfoResult for SeriesLookupSender {
    fn make_send(&self, item: Result<ChannelInfoResult, dbpg::seriesbychannel::Error>) -> BoxedSend {
        let tx = self.tx.clone();
        let fut = async move { tx.send(item.map_err(Into::into)).await.map_err(|_| ()) };
        Box::pin(fut)
    }
}

struct StateTransRes<'a> {
    backend: &'a str,
    ca_conn_ress: &'a mut HashMap<SocketAddr, CaConnRes>,
    channel_info_query_qu: &'a mut VecDeque<ChannelInfoQuery>,
    channel_info_res_tx: Pin<&'a mut Sender<Result<ChannelInfoResult, Error>>>,
    chst: &'a mut ChannelState,
    mett: &'a mut CaConnSetMetrics,
}

impl<'a> StateTransRes<'a> {
    fn init(value: &'a mut CaConnSet, chname: &ChannelName) -> Self {
        let chst = value.channel_states.get_mut_or_dummy_init(&chname);
        Self {
            backend: &value.backend,
            ca_conn_ress: &mut value.ca_conn_ress,
            channel_info_query_qu: &mut value.channel_info_query_qu,
            channel_info_res_tx: value.channel_info_res_tx.as_mut(),
            chst,
            mett: &mut value.mett,
        }
    }
}

pub struct CaConnSet {
    ticker: Pin<Box<tokio::time::Sleep>>,
    backend: String,
    local_epics_hostname: String,
    ca_conn_ress: HashMap<SocketAddr, CaConnRes>,
    channel_states: ChannelStateMap,
    channel_by_cssid: HashMap<ChannelStatusSeriesId, ChannelName>,
    connset_inp_rx: Pin<Box<Receiver<CaConnSetEvent>>>,
    channel_info_query_qu: VecDeque<ChannelInfoQuery>,
    channel_info_query_sender: Pin<Box<SenderPolling<ChannelInfoQuery>>>,
    channel_info_query_tx: Option<Sender<ChannelInfoQuery>>,
    channel_info_res_tx: Pin<Box<Sender<Result<ChannelInfoResult, Error>>>>,
    channel_info_res_rx: Pin<Box<Receiver<Result<ChannelInfoResult, Error>>>>,
    find_ioc_query_queue: VecDeque<IocAddrQuery>,
    find_ioc_query_sender: Pin<Box<SenderPolling<IocAddrQuery>>>,
    find_ioc_res_rx: Pin<Box<Receiver<VecDeque<FindIocRes>>>>,
    find_ioc_queue_set: QueueSet<ChannelName>,
    iqtx: Pin<Box<InsertQueuesTx>>,
    storage_insert_st_qu: VecDeque<VecDeque<QueryItem>>,
    storage_insert_st_qu_l1: VecDeque<QueryItem>,
    storage_insert_st_tx: Pin<Box<SenderPolling<VecDeque<QueryItem>>>>,
    storage_insert_lt_qu: VecDeque<VecDeque<QueryItem>>,
    storage_insert_lt_qu_l1: VecDeque<QueryItem>,
    storage_insert_lt_tx: Pin<Box<SenderPolling<VecDeque<QueryItem>>>>,
    ca_conn_res_tx: Pin<Box<Sender<(SocketAddr, CaConnEvent)>>>,
    ca_conn_res_rx: Pin<Box<Receiver<(SocketAddr, CaConnEvent)>>>,
    connset_out_queue: VecDeque<CaConnSetItem>,
    connset_out_tx: Pin<Box<Sender<CaConnSetItem>>>,
    shutdown_stopping: bool,
    shutdown_done: bool,
    chan_check_next: Option<ChannelName>,
    ioc_finder_jh: JoinHandle<Result<(), crate::ca::finder::Error>>,
    await_ca_conn_jhs: VecDeque<(SocketAddr, JoinHandle<Result<(), Error>>)>,
    thr_msg_storage_len: ThrottleTrace,
    cssid_latency_max: Duration,
    mett: stats::mett::CaConnSetMetrics,
    use_binwriter: bool,
}

impl CaConnSet {
    pub fn self_name() -> &'static str {
        std::any::type_name::<Self>()
    }

    pub fn start(
        backend: String,
        local_epics_hostname: String,
        iqtx: InsertQueuesTx,
        channel_info_query_tx: Sender<ChannelInfoQuery>,
        ingest_opts: CaIngestOpts,
    ) -> CaConnSetCtrl {
        let use_binwriter = ingest_opts.binwriter_enable();
        let (ca_conn_res_tx, ca_conn_res_rx) = async_channel::bounded(200);
        let (connset_inp_tx, connset_inp_rx) = async_channel::bounded(200);
        let (connset_out_tx, connset_out_rx) = async_channel::bounded(200);
        let (find_ioc_res_tx, find_ioc_res_rx) = async_channel::bounded(400);
        let (find_ioc_query_tx, ioc_finder_jh) =
            super::finder::start_finder(find_ioc_res_tx.clone(), backend.clone(), ingest_opts).unwrap();
        let (channel_info_res_tx, channel_info_res_rx) = async_channel::bounded(400);
        let connset = Self {
            ticker: Self::new_self_ticker(),
            backend,
            local_epics_hostname,
            ca_conn_ress: HashMap::new(),
            channel_states: ChannelStateMap::new(),
            channel_by_cssid: HashMap::new(),
            connset_inp_rx: Box::pin(connset_inp_rx),
            channel_info_query_qu: VecDeque::new(),
            channel_info_query_sender: Box::pin(SenderPolling::new(channel_info_query_tx.clone())),
            channel_info_query_tx: Some(channel_info_query_tx),
            channel_info_res_tx: Box::pin(channel_info_res_tx),
            channel_info_res_rx: Box::pin(channel_info_res_rx),
            find_ioc_query_queue: VecDeque::new(),
            find_ioc_query_sender: Box::pin(SenderPolling::new(find_ioc_query_tx)),
            find_ioc_res_rx: Box::pin(find_ioc_res_rx),
            find_ioc_queue_set: QueueSet::new(),
            iqtx: Box::pin(iqtx.clone()),
            storage_insert_st_qu: VecDeque::new(),
            storage_insert_st_qu_l1: VecDeque::new(),
            storage_insert_st_tx: Box::pin(SenderPolling::new(iqtx.st_rf3_tx.clone())),
            // TODO simplify for all combinations
            storage_insert_lt_qu: VecDeque::new(),
            storage_insert_lt_qu_l1: VecDeque::new(),
            storage_insert_lt_tx: Box::pin(SenderPolling::new(iqtx.lt_rf3_tx.clone())),
            ca_conn_res_tx: Box::pin(ca_conn_res_tx),
            ca_conn_res_rx: Box::pin(ca_conn_res_rx),
            shutdown_stopping: false,
            shutdown_done: false,
            chan_check_next: None,
            connset_out_tx: Box::pin(connset_out_tx),
            connset_out_queue: VecDeque::new(),
            // connset_out_sender: SenderPolling::new(connset_out_tx),
            ioc_finder_jh,
            await_ca_conn_jhs: VecDeque::new(),
            thr_msg_storage_len: ThrottleTrace::new(Duration::from_millis(1000)),
            cssid_latency_max: Duration::from_millis(2000),
            mett: stats::mett::CaConnSetMetrics::new(),
            use_binwriter,
        };
        // TODO await on jh
        let jh = tokio::spawn(CaConnSet::run(connset));
        CaConnSetCtrl {
            tx: connset_inp_tx,
            rx: connset_out_rx,
            jh,
        }
    }

    fn new_self_ticker() -> Pin<Box<tokio::time::Sleep>> {
        Box::pin(tokio::time::sleep(Duration::from_millis(500)))
    }

    async fn run(mut this: CaConnSet) -> Result<(), Error> {
        trace!("CaConnSet  run begin");
        let (beacons_cancel_guard_tx, rx) = taskrun::tokio::sync::mpsc::channel(12);
        let beacons_jh = {
            let tx2 = this.channel_info_query_tx.clone().unwrap();
            let backend = this.backend.clone();
            tokio::spawn(async move {
                if false {
                    crate::ca::beacons::listen_beacons(rx, tx2, backend).await
                } else {
                    Ok(())
                }
            })
        };
        let _g_beacon = OnDrop::new(move || {});
        loop {
            let x = this.next().await;
            match x {
                Some(x) => this.connset_out_tx.send(x).await?,
                None => break,
            }
        }
        trace!("CaConnSet EndOfStream");
        beacons_cancel_guard_tx.send(1).await.ok();
        trace!("CaConnSet beacon cancelled");
        beacons_jh.await??;
        trace!("CaConnSet beacon joined");
        trace!("join ioc_finder_jh A  {:?}", this.find_ioc_query_sender.len());
        this.find_ioc_query_sender.as_mut().drop();
        trace!("join ioc_finder_jh B  {:?}", this.find_ioc_query_sender.len());
        this.ioc_finder_jh.await??;
        trace!("joined ioc_finder_jh");
        this.connset_out_tx.close();
        this.connset_inp_rx.close();
        this.shutdown_done = true;
        trace!("CaConnSet run done");
        Ok(())
    }

    fn handle_event(&mut self, ev: CaConnSetEvent) -> Result<(), Error> {
        match ev {
            CaConnSetEvent::ConnSetCmd(cmd) => match cmd {
                ConnSetCmd::ChannelConfigFlagReset(x) => self.handle_channel_config_flag_reset(x),
                ConnSetCmd::ChannelConfigRemoveUnflagged(x) => self.handle_channel_config_remove_unflagged(x),
                ConnSetCmd::ChannelAdd(x) => self.handle_add_channel(x),
                ConnSetCmd::ChannelRemove(x) => self.handle_remove_channel(x),
                ConnSetCmd::Shutdown => self.handle_shutdown(),
                ConnSetCmd::ChannelStatuses(x) => self.handle_channel_statuses_req(x),
                ConnSetCmd::ChannelStatusesPrivate(x) => self.handle_channel_statuses_private_req(x),
                ConnSetCmd::ChannelCommand(x) => self.handle_channel_command(x),
            },
        }
    }

    fn handle_add_channel_new(cmd: ChannelAdd, ress: StateTransRes) -> Result<(), Error> {
        {
            let item = ChannelState::new_wait_for_cssid(&cmd.ch_cfg);
            *ress.chst = item;
        }
        {
            let channel_name = cmd.name().into();
            let tx = ress.channel_info_res_tx.as_ref().get_ref().clone();
            let item = ChannelInfoQuery {
                backend: ress.backend.into(),
                channel: channel_name,
                kind: SeriesKind::ChannelStatus,
                scalar_type: ScalarType::U64,
                shape: Shape::Scalar,
                tx: Box::pin(SeriesLookupSender { tx }),
            };
            ress.channel_info_query_qu.push_back(item);
        }
        if let Err(_) = cmd.restx.try_send(Ok(())) {
            ress.mett.cmd_res_send_err().inc();
        }
        Ok(())
    }

    fn handle_add_channel_existing(cmd: ChannelAdd, ress: StateTransRes) -> Result<(), Error> {
        let tsnow = Instant::now();
        ress.chst.touched = 1;
        if cmd.ch_cfg == ress.chst.config {
            debug!("handle_add_channel_existing  config same  {}", cmd.name());
            if let Err(_) = cmd.restx.try_send(Ok(())) {
                ress.mett.cmd_res_send_err().inc();
            }
            Ok(())
        } else {
            debug!("handle_add_channel_existing  config changed  {}", cmd.name());
            // TODO
            match &mut ress.chst.value {
                ChannelStateValue::Active(st2) => match st2 {
                    ActiveChannelState::Init { .. } => {
                        ress.chst.config = cmd.ch_cfg;
                    }
                    ActiveChannelState::WaitForStatusSeriesId { .. } => {
                        ress.chst.config = cmd.ch_cfg;
                    }
                    ActiveChannelState::WithStatusSeriesId(st3) => match &mut st3.inner {
                        WithStatusSeriesIdStateInner::AddrSearchPending { .. } => {
                            ress.chst.config = cmd.ch_cfg;
                        }
                        WithStatusSeriesIdStateInner::WithAddress { addr, state: st4 } => match &st4 {
                            WithAddressState::Unassigned { .. } => {
                                ress.chst.config = cmd.ch_cfg;
                            }
                            WithAddressState::Assigned(_) => {
                                debug!("unassign for config change  {:?}  {}", cmd, addr);
                                let conn_ress = ress
                                    .ca_conn_ress
                                    .get_mut(&SocketAddr::V4(addr.clone()))
                                    .ok_or_else(|| Error::ChannelAssignedWithoutConnRess)?;
                                let item = ConnCommand::channel_close_reconf(cmd.name().into());
                                conn_ress.cmd_queue.push_back(item);
                                st3.inner = WithStatusSeriesIdStateInner::UnassigningForConfigChange(
                                    statemap::UnassigningForConfigChangeState {
                                        config_new: cmd.ch_cfg,
                                        addr: SocketAddr::V4(addr.clone()),
                                        since: tsnow,
                                    },
                                );
                            }
                        },
                        WithStatusSeriesIdStateInner::UnknownAddress { .. } => {
                            ress.chst.config = cmd.ch_cfg;
                        }
                        WithStatusSeriesIdStateInner::NoAddress { .. } => {
                            ress.chst.config = cmd.ch_cfg;
                        }
                        WithStatusSeriesIdStateInner::MaybeWrongAddress(..) => {
                            ress.chst.config = cmd.ch_cfg;
                        }
                        WithStatusSeriesIdStateInner::UnassigningForConfigChange(st4) => {
                            st4.config_new = cmd.ch_cfg;
                        }
                        WithStatusSeriesIdStateInner::AddrSearchPlanned { .. } => {
                            ress.chst.config = cmd.ch_cfg;
                        }
                    },
                },
                ChannelStateValue::ToRemove { .. } => {
                    ress.chst.config = cmd.ch_cfg;
                }
                ChannelStateValue::InitDummy => {
                    return Err(Error::UnexpectedChannelDummyState);
                }
            }
            if let Err(_) = cmd.restx.try_send(Ok(())) {
                ress.mett.cmd_res_send_err().inc();
            }
            Ok(())
        }
    }

    fn handle_channel_config_flag_reset(&mut self, cmd: ChannelConfigFlagReset) -> Result<(), Error> {
        for chst in self.channel_states.iter_mut() {
            chst.1.touched = 0;
        }
        if let Err(_) = cmd.restx.try_send(Ok(())) {
            self.mett.cmd_res_send_err().inc();
        }
        Ok(())
    }

    fn handle_channel_config_remove_unflagged(&mut self, cmd: ChannelConfigRemoveUnflagged) -> Result<(), Error> {
        let mut cmds = VecDeque::new();
        for chst in self.channel_states.iter_mut() {
            if chst.1.touched == 0 {
                let cmd = ChannelRemove {
                    name: chst.0.name().into(),
                };
                cmds.push_back(cmd);
            }
        }
        for cmd in cmds {
            debug!("call handle_remove_channel {:?}", cmd);
            self.handle_remove_channel(cmd)?;
        }
        if let Err(_) = cmd.restx.try_send(Ok(())) {
            self.mett.cmd_res_send_err().inc();
        }
        Ok(())
    }

    fn handle_add_channel(&mut self, cmd: ChannelAdd) -> Result<(), Error> {
        if self.shutdown_stopping {
            trace3!("handle_add_channel but shutdown_stopping");
            return Ok(());
        }
        if series::dbg::dbg_chn(cmd.name()) {
            info!("handle_add_channel  {:?}", cmd);
        }
        trace_channel_state!("handle_add_channel {:?}", cmd);
        // TODO should I add the transition through ActiveChannelState::Init as well?
        let chname = ChannelName::new(cmd.name().into());
        let ress = StateTransRes::init(self, &chname);
        if ress.chst.is_dummy() {
            // Directly overwrites this dummy state:
            Self::handle_add_channel_new(cmd, ress)?;
        } else {
            Self::handle_add_channel_existing(cmd, ress)?;
        }
        Ok(())
    }

    fn handle_ca_conn_event(&mut self, addr: SocketAddr, ev: CaConnEvent) -> Result<(), Error> {
        match ev.value {
            CaConnEventValue::None => Ok(()),
            CaConnEventValue::EchoTimeout => Ok(()),
            CaConnEventValue::ConnCommandResult(x) => self.handle_conn_command_result(addr, x),
            CaConnEventValue::ChannelCreateFail(x) => self.handle_channel_create_fail(addr, x),
            CaConnEventValue::ChannelStatus(st) => self.apply_ca_conn_health_update(addr, st),
            CaConnEventValue::EndOfStream(reason) => self.handle_ca_conn_eos(addr, reason),
            CaConnEventValue::ChannelRemoved(name) => self.handle_ca_conn_channel_removed(addr, name),
            CaConnEventValue::Metrics(v) => {
                self.mett.ca_conn().ingest(v);
                Ok(())
            }
        }
    }

    fn handle_series_lookup_result(&mut self, res: Result<ChannelInfoResult, Error>) -> Result<(), Error> {
        trace!("handle_series_lookup_result {:?}", res);
        if self.shutdown_stopping {
            Ok(())
        } else {
            match res {
                Ok(res) => {
                    let channel = ChannelName::new(res.channel.clone());
                    // TODO must not depend on purely informative `self.channel_state`
                    if let Some(st) = self.channel_states.get_mut(&channel) {
                        let cssid = ChannelStatusSeriesId::new(res.series.to_series().id());
                        self.channel_by_cssid
                            .insert(cssid.clone(), ChannelName::new(res.channel.clone()));
                        let add = ChannelAddWithStatusId {
                            ch_cfg: st.config.clone(),
                            cssid,
                        };
                        self.handle_add_channel_with_status_id(add)?;
                        Ok(())
                    } else {
                        // TODO count for metrics
                        warn!("received series id for unknown channel");
                        Ok(())
                    }
                }
                Err(e) => {
                    warn!("TODO handle error {}", e);
                    Ok(())
                }
            }
        }
    }

    fn handle_add_channel_with_status_id(&mut self, cmd: ChannelAddWithStatusId) -> Result<(), Error> {
        let name = cmd.ch_cfg.name();
        trace3!("handle_add_channel_with_status_id {}", name);
        if self.shutdown_stopping {
            debug!("handle_add_channel but shutdown_stopping");
            return Ok(());
        }
        self.mett.channel_status_series_found().inc();
        if series::dbg::dbg_chn(&name) {
            info!("handle_add_channel_with_status_id  {:?}", cmd);
        }
        let ch = ChannelName::new(name.into());
        if let Some(chst) = self.channel_states.get_mut(&ch) {
            if let ChannelStateValue::Active(chst2) = &mut chst.value {
                if let ActiveChannelState::WaitForStatusSeriesId { since } = chst2 {
                    let dt = since.elapsed().unwrap();
                    if dt > self.cssid_latency_max {
                        self.cssid_latency_max = dt + Duration::from_millis(2000);
                        debug!("slow cssid fetch  dt {:.0} ms  {:?}", 1e3 * dt.as_secs_f32(), cmd);
                    }
                    let mut writer_status = serieswriter::writer::SeriesWriter::new(
                        SeriesId::new(cmd.cssid.id()),
                        MspSplitFixGrid::for_channel_status(),
                    )?;
                    {
                        let status = netpod::channelstatus::ChannelStatus::HaveStatusId;
                        let stnow = SystemTime::now();
                        let ts = TsNano::from_system_time(stnow);
                        let item = serieswriter::fixgridwriter::ChannelStatusWriteValue::new(ts, status.to_u64());
                        let ts_net = Instant::now();
                        let deque = &mut self.storage_insert_lt_qu_l1;
                        writer_status.write(item, &mut (), ts_net, ts, deque)?;
                    }
                    *chst2 = ActiveChannelState::WithStatusSeriesId(WithStatusSeriesIdState {
                        cssid: cmd.cssid,
                        addr_find_backoff: 0,
                        inner: WithStatusSeriesIdStateInner::AddrSearchPending {
                            since: SystemTime::now(),
                        },
                        writer_status: Some(writer_status),
                    });
                    let qu = IocAddrQuery::cached(name.into());
                    self.find_ioc_query_queue.push_back(qu);
                    self.mett.ioc_search_start().inc();
                } else {
                    self.mett.logic_err().inc();
                    trace!("TODO have a status series id but no more channel");
                }
            } else {
                self.mett.logic_err().inc();
                trace!("TODO have a status series id but no more channel");
            }
        } else {
            self.mett.logic_err().inc();
            trace!("TODO have a status series id but no more channel");
        }
        Ok(())
    }

    fn handle_add_channel_with_addr(&mut self, cmd: ChannelAddWithAddr) -> Result<(), Error> {
        let name = cmd.ch_cfg.name();
        if self.shutdown_stopping {
            trace3!("handle_add_channel but shutdown_stopping");
            return Ok(());
        }
        let addr_v4 = if let SocketAddr::V4(x) = cmd.addr {
            x
        } else {
            return Err(Error::ExpectIpv4);
        };
        if series::dbg::dbg_chn(&name) {
            info!("handle_add_channel_with_addr  {:?}", cmd);
        }
        let ch = ChannelName::new(name.into());
        if let Some(chst) = self.channel_states.get_mut(&ch) {
            // TODO should not have some already stored config.
            chst.config = cmd.ch_cfg.clone();
            if let ChannelStateValue::Active(ast) = &mut chst.value {
                if let ActiveChannelState::WithStatusSeriesId(st3) = ast {
                    trace!("handle_add_channel_with_addr  INNER  {:?}", cmd);
                    self.mett.handle_add_channel_with_addr().inc();
                    let tsnow = SystemTime::now();
                    {
                        let status = netpod::channelstatus::ChannelStatus::HaveAddress;
                        let stnow = SystemTime::now();
                        let ts = TsNano::from_system_time(stnow);
                        let item = serieswriter::fixgridwriter::ChannelStatusWriteValue::new(ts, status.to_u64());
                        let ts_net = Instant::now();
                        let deque = &mut self.storage_insert_lt_qu_l1;
                        st3.writer_status
                            .as_mut()
                            .unwrap()
                            .write(item, &mut (), ts_net, ts, deque)?;
                    }
                    st3.cssid = cmd.cssid.clone();
                    st3.addr_find_backoff = 0;
                    st3.inner = WithStatusSeriesIdStateInner::WithAddress {
                        addr: addr_v4,
                        state: WithAddressState::Assigned(ConnectionState {
                            updated: tsnow,
                            health_update_count: 0,
                            value: ConnectionStateValue::Unknown,
                        }),
                    };
                    if false {
                        let _ = WithStatusSeriesIdState {
                            cssid: todo!(),
                            addr_find_backoff: todo!(),
                            inner: todo!(),
                            writer_status: todo!(),
                        };
                    }
                    let addr = cmd.addr;
                    if self.ca_conn_ress.contains_key(&addr) {
                        trace!("ca_conn_ress has already {:?}", addr_v4);
                    } else {
                        trace!("ca_conn_ress  NEW  {:?}", addr);
                        let c = self.create_ca_conn(cmd.clone())?;
                        self.ca_conn_ress.insert(addr, c);
                    }
                    let conn_ress = self.ca_conn_ress.get_mut(&addr).unwrap();
                    let cmd = ConnCommand::channel_add(cmd.ch_cfg, cmd.cssid);
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
        let ch = ChannelName::new(cmd.name);
        if let Some(k) = self.channel_states.get_mut(&ch) {
            match &k.value {
                ChannelStateValue::Active(j) => match j {
                    ActiveChannelState::Init { .. } => {
                        k.value = ChannelStateValue::ToRemove { addr: None };
                    }
                    ActiveChannelState::WaitForStatusSeriesId { .. } => {
                        k.value = ChannelStateValue::ToRemove { addr: None };
                    }
                    ActiveChannelState::WithStatusSeriesId(state) => match &state.inner {
                        WithStatusSeriesIdStateInner::UnknownAddress { .. } => {
                            k.value = ChannelStateValue::ToRemove { addr: None };
                        }
                        WithStatusSeriesIdStateInner::AddrSearchPending { .. } => {
                            k.value = ChannelStateValue::ToRemove { addr: None };
                        }
                        WithStatusSeriesIdStateInner::WithAddress { addr, state: _ } => {
                            debug!("send remove  {:?}  to {}", ch, addr);
                            let conn_ress = self
                                .ca_conn_ress
                                .get_mut(&SocketAddr::V4(addr.clone()))
                                .ok_or_else(|| Error::ChannelAssignedWithoutConnRess)?;
                            let item = ConnCommand::channel_close(ch.name().into());
                            conn_ress.cmd_queue.push_back(item);
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
                        WithStatusSeriesIdStateInner::UnassigningForConfigChange(..) => {
                            k.value = ChannelStateValue::ToRemove { addr: None };
                        }
                        WithStatusSeriesIdStateInner::AddrSearchPlanned { .. } => {
                            k.value = ChannelStateValue::ToRemove { addr: None };
                        }
                    },
                },
                ChannelStateValue::ToRemove { .. } => {}
                ChannelStateValue::InitDummy { .. } => {}
            }
        }
        Ok(())
    }

    fn handle_ioc_query_result(&mut self, results: VecDeque<FindIocRes>) -> Result<(), Error> {
        trace!("handle_ioc_query_result  {:?}", results);
        if self.shutdown_stopping {
            return Ok(());
        }
        for res in results {
            let ch = ChannelName::new(res.channel.clone());
            if series::dbg::dbg_chn(&ch.name()) {
                info!("handle_ioc_query_result  {:?}", res);
            }
            if let Some(chst) = self.channel_states.get_mut(&ch) {
                if let ChannelStateValue::Active(ast) = &mut chst.value {
                    if let ActiveChannelState::WithStatusSeriesId(st2) = ast {
                        if let Some(addr) = res.addr {
                            self.mett.ioc_addr_found().inc();
                            trace!("ioc found {:?}", res);
                            let cmd = ChannelAddWithAddr {
                                ch_cfg: chst.config.clone(),
                                addr: SocketAddr::V4(addr),
                                cssid: st2.cssid.clone(),
                            };
                            self.handle_add_channel_with_addr(cmd)?;
                        } else {
                            self.mett.ioc_addr_not_found().inc();
                            trace!("ioc not found {:?}", res);
                            let since = SystemTime::now();
                            st2.inner = WithStatusSeriesIdStateInner::UnknownAddress { since };
                        }
                    } else {
                        self.mett.ioc_addr_result_for_unknown_channel().inc();
                        warn!("TODO got address but no longer active");
                    }
                } else {
                    self.mett.ioc_addr_result_for_unknown_channel().inc();
                    warn!("TODO got address but no longer active");
                }
            } else {
                self.mett.ioc_addr_result_for_unknown_channel().inc();
                warn!("ioc addr lookup done but channel no longer here");
            }
        }
        Ok(())
    }

    fn handle_check_health(&mut self) -> Result<(), Error> {
        let tsnow = Instant::now();
        let stnow = SystemTime::now();
        trace2!("handle_check_health");
        if self.shutdown_stopping {
            Ok(())
        } else {
            if false {
                // TODO
                self.thr_msg_storage_len
                    .trigger("connset handle_check_health", &[&self.storage_insert_st_tx.len()]);
            }
            self.check_channel_states(tsnow, stnow)?;
            let item = CaConnSetItem::Healthy;
            self.connset_out_queue.push_back(item);
            Ok(())
        }
    }

    fn handle_channel_statuses_req(&mut self, req: ChannelStatusesRequest) -> Result<(), Error> {
        if self.shutdown_stopping {
            return Ok(());
        }
        debug!("handle_channel_statuses_req");
        let reg1 = regex::Regex::new(&req.name)?;
        let channels_ca_conn_set = self
            .channel_states
            .iter()
            .filter(|(k, _)| reg1.is_match(k.name()))
            .map(|(k, v)| (k.name().to_string(), v.clone()))
            .collect();
        let item = ChannelStatusesResponse { channels_ca_conn_set };
        if req.tx.try_send(item).is_err() {
            self.mett.chan_send_err().inc();
        }
        Ok(())
    }

    fn handle_channel_statuses_private_req(&mut self, req: ChannelStatusesPrivateRequest) -> Result<(), Error> {
        if self.shutdown_stopping {
            return Ok(());
        }
        for (addr, ca_conn) in self.ca_conn_ress.iter() {
            // let item = ConnCommand::status_private();
            // ca_conn.cmd_queue.push_back(item);
        }
        let reg1 = regex::Regex::new(&req.name)?;
        Ok(())
    }

    fn handle_channel_command(&mut self, cmd: ChannelCommand) -> Result<(), Error> {
        if self.shutdown_stopping {
            return Ok(());
        }
        // TODO handle, send to corresponding CaConn
        let name = cmd.channel.clone();
        let mut cmd = Some(cmd);
        // TODO no need to iterate anymore
        self.channel_states
            .iter_mut()
            .filter(|(k, _)| k.name() == name)
            .map(|(_, st1)| {
                if let ChannelStateValue::Active(st2) = &mut st1.value {
                    if let ActiveChannelState::WithStatusSeriesId(st3) = st2 {
                        if let WithStatusSeriesIdStateInner::WithAddress { addr, state: _ } = &mut st3.inner {
                            let addr2 = SocketAddr::V4(*addr);
                            self.ca_conn_ress.get_mut(&addr2).map(|q| {
                                if let Some(cmd) = cmd.take() {
                                    q.cmd_queue.push_back(cmd.conn_command);
                                }
                            });
                        }
                    }
                };
            })
            .for_each(|_| ());
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

    fn handle_conn_command_result(&mut self, _addr: SocketAddr, res: ConnCommandResult) -> Result<(), Error> {
        use crate::ca::conn::ConnCommandResultKind::*;
        match res.kind {
            Unused => Ok(()),
            //CheckHealth(res) => self.apply_ca_conn_health_update(addr, res),
        }
    }

    fn apply_ca_conn_health_update(&mut self, addr: SocketAddr, res: ChannelStatusPartial) -> Result<(), Error> {
        trace_health_update!("apply_ca_conn_health_update  {}", addr);
        let tsnow = SystemTime::now();
        let mut rogue_channel_count = 0;
        for (k, v) in res.channel_statuses {
            trace_health_update!("self.rogue_channel_count {}", rogue_channel_count);
            trace_health_update!("apply_ca_conn_health_update  {:?}  {:?}", k, v);
            let ch = if let Some(x) = self.channel_by_cssid.get(&k) {
                x
            } else {
                return Err(Error::UnknownCssid);
            };
            if let Some(st1) = self.channel_states.get_mut(&ch) {
                if let ChannelStateValue::Active(st2) = &mut st1.value {
                    if let ActiveChannelState::WithStatusSeriesId(st3) = st2 {
                        if let WithStatusSeriesIdStateInner::WithAddress {
                            addr: conn_addr,
                            state: st4,
                        } = &mut st3.inner
                        {
                            if SocketAddr::V4(*conn_addr) != addr {
                                rogue_channel_count += 1;
                            }
                            if let WithAddressState::Assigned(st5) = st4 {
                                st5.updated = tsnow;
                                st5.health_update_count += 1;
                                st5.value = ConnectionStateValue::ChannelStateInfo(v);
                            } else {
                                rogue_channel_count += 1;
                            }
                        } else {
                            rogue_channel_count += 1;
                        }
                    } else {
                        rogue_channel_count += 1;
                    }
                } else {
                    rogue_channel_count += 1;
                }
            } else {
                rogue_channel_count += 1;
            }
        }
        trace_health_update!("rogue_channel_count {}", rogue_channel_count);
        self.mett.channel_rogue().set(rogue_channel_count);
        Ok(())
    }

    fn handle_channel_create_fail(&mut self, addr: SocketAddr, name: String) -> Result<(), Error> {
        if series::dbg::dbg_chn(&name) {
            info!("handle_channel_create_fail {:?} {:?}", name, addr);
        } else {
            trace!("handle_channel_create_fail {:?} {:?}", name, addr);
        }
        let stnow = SystemTime::now();
        let ch = ChannelName::new(name);
        if let Some(st1) = self.channel_states.get_mut(&ch) {
            if let ChannelStateValue::Active(st2) = &mut st1.value {
                if let ActiveChannelState::WithStatusSeriesId(st3) = st2 {
                    if series::dbg::dbg_chn(ch.name()) {
                        info!(
                            "handle_channel_create_fail  {:?}  {:?}  set to MaybeWrongAddress",
                            ch, addr
                        );
                    } else {
                        trace!(
                            "handle_channel_create_fail  {:?}  {:?}  set to MaybeWrongAddress",
                            ch, addr
                        );
                    }
                    bump_backoff(&mut st3.addr_find_backoff);
                    let snew = MaybeWrongAddressState::new(stnow, st3.addr_find_backoff);
                    if series::dbg::dbg_chn(ch.name()) {
                        info!("handle_channel_create_fail  update state  {:?}", snew);
                    }
                    st3.inner = WithStatusSeriesIdStateInner::MaybeWrongAddress(snew);
                }
            }
        }
        Ok(())
    }

    fn handle_ca_conn_eos(&mut self, addr: SocketAddr, reason: EndOfStreamReason) -> Result<(), Error> {
        debug!("handle_ca_conn_eos  {}  {:?}", addr, reason);
        if let Some(e) = self.ca_conn_ress.remove(&addr) {
            self.mett.ca_conn_eos_ok().inc();
            self.await_ca_conn_jhs.push_back((addr, e.jh));
        } else {
            self.mett.ca_conn_eos_unexpected().inc();
            warn!("end-of-stream received for non-existent CaConn {}", addr);
        }
        {
            use EndOfStreamReason::*;
            match reason {
                UnspecifiedReason => {
                    warn!("EndOfStreamReason::UnspecifiedReason");
                    self.handle_connect_fail(addr)?
                }
                Error(e) => {
                    warn!("received error  {}  {}", addr, e);
                    self.handle_connect_fail(addr)?
                }
                ConnectRefused => self.handle_connect_fail(addr)?,
                ConnectTimeout => self.handle_connect_fail(addr)?,
                OnCommand => {
                    // warn!("TODO  make sure no channel is in state which could trigger health timeout")
                }
                RemoteClosed => self.handle_connect_fail(addr)?,
                IocTimeout => self.handle_connect_fail(addr)?,
                IoError => self.handle_connect_fail(addr)?,
            }
        }
        // self.remove_channel_status_for_addr(addr)?;
        trace2!("still CaConn left  {}", self.ca_conn_ress.len());
        Ok(())
    }

    fn handle_ca_conn_channel_removed(&mut self, addr: SocketAddr, name: String) -> Result<(), Error> {
        debug!("handle_ca_conn_channel_removed  {}  {}", addr, name);
        let name = ChannelName::new(name);
        if let Some(st1) = self.channel_states.get_mut(&name) {
            match &mut st1.value {
                ChannelStateValue::Active(st2) => match st2 {
                    ActiveChannelState::Init { .. } => Ok(()),
                    ActiveChannelState::WaitForStatusSeriesId { .. } => Ok(()),
                    ActiveChannelState::WithStatusSeriesId(st3) => match &st3.inner {
                        WithStatusSeriesIdStateInner::AddrSearchPending { .. } => Ok(()),
                        WithStatusSeriesIdStateInner::WithAddress { .. } => Ok(()),
                        WithStatusSeriesIdStateInner::UnknownAddress { .. } => Ok(()),
                        WithStatusSeriesIdStateInner::NoAddress { .. } => Ok(()),
                        WithStatusSeriesIdStateInner::MaybeWrongAddress(..) => Ok(()),
                        WithStatusSeriesIdStateInner::UnassigningForConfigChange(st4) => {
                            st1.config = st4.config_new.clone();
                            let cmd = ChannelAddWithAddr {
                                ch_cfg: st4.config_new.clone(),
                                cssid: st3.cssid,
                                addr: st4.addr,
                            };
                            self.handle_add_channel_with_addr(cmd)?;
                            Ok(())
                        }
                        WithStatusSeriesIdStateInner::AddrSearchPlanned { .. } => Ok(()),
                    },
                },
                ChannelStateValue::ToRemove { .. } => {
                    self.channel_states.remove(&name);
                    Ok(())
                }
                ChannelStateValue::InitDummy => Err(Error::UnexpectedChannelDummyState),
            }
        } else {
            debug!("can not find channel for removed channel {:?}", name);
            Ok(())
        }
    }

    fn handle_connect_fail(&mut self, addr: SocketAddr) -> Result<(), Error> {
        self.transition_channels_to_maybe_wrong_address(addr)?;
        Ok(())
    }

    fn transition_channels_to_maybe_wrong_address(&mut self, addr: SocketAddr) -> Result<(), Error> {
        // TODO take a "reason" as parameter for status emit.
        let stnow = SystemTime::now();
        for (ch, st1) in self.channel_states.iter_mut() {
            match &mut st1.value {
                ChannelStateValue::Active(st2) => match st2 {
                    ActiveChannelState::Init { since: _ } => {}
                    ActiveChannelState::WaitForStatusSeriesId { since: _ } => {}
                    ActiveChannelState::WithStatusSeriesId(st3) => {
                        use WithStatusSeriesIdStateInner::*;
                        match &mut st3.inner {
                            AddrSearchPending { since: _ } => {}
                            WithAddress { addr: addr2, state: _ } => {
                                if SocketAddr::V4(*addr2) == addr {
                                    bump_backoff(&mut st3.addr_find_backoff);
                                    let snew = MaybeWrongAddressState::new(stnow, st3.addr_find_backoff);
                                    st3.inner = WithStatusSeriesIdStateInner::MaybeWrongAddress(snew.clone());
                                    if series::dbg::dbg_chn(&ch.name()) {
                                        info!(
                                            "transition_channels_to_maybe_wrong_address  BB  {:?}  {:?}  {:?}  {:?}",
                                            ch, addr, snew, st1
                                        );
                                    }
                                } else {
                                    // nothing to do
                                }
                            }
                            UnknownAddress { since: _ } => {}
                            NoAddress { since: _ } => {}
                            MaybeWrongAddress(_) => {}
                            UnassigningForConfigChange(_) => {}
                            AddrSearchPlanned { .. } => {}
                        }
                    }
                },
                ChannelStateValue::ToRemove { addr: _ } => {}
                ChannelStateValue::InitDummy => {
                    // TODO must never occur
                }
            }
        }
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

    fn create_ca_conn(&mut self, add: ChannelAddWithAddr) -> Result<CaConnRes, Error> {
        // TODO should we save this as event?
        let opts = CaConnOpts::default().binwriter_use(self.use_binwriter);
        let addr = add.addr;
        let addr_v4 = if let SocketAddr::V4(x) = add.addr {
            x
        } else {
            return Err(Error::ExpectIpv4);
        };
        self.mett.create_ca_conn().inc();
        let conn = CaConn::new(
            opts,
            self.backend.clone(),
            addr_v4,
            self.local_epics_hostname.clone(),
            self.iqtx.clone2(),
            self.channel_info_query_tx
                .clone()
                .ok_or_else(|| Error::MissingChannelInfoChannelTx)?,
        );
        let conn_tx = conn.conn_command_tx();
        let tx1 = self.ca_conn_res_tx.as_ref().get_ref().clone();
        let log_level = "trace";
        let logspan = if log_level == "trace" {
            trace!("enable trace for handler");
            tracing::span!(tracing::Level::INFO, "log_span_trace")
        } else if log_level == "debug" {
            debug!("enable debug for handler");
            tracing::span!(tracing::Level::INFO, "log_span_debug")
        } else {
            tracing::Span::none()
        };
        let fut = Self::ca_conn_item_merge(conn, tx1, addr);
        let fut = fut.instrument(logspan);
        let jh = tokio::spawn(fut);
        let ca_conn_res = CaConnRes {
            sender: Box::pin(conn_tx.into()),
            cmd_queue: VecDeque::new(),
            jh,
        };
        Ok(ca_conn_res)
    }

    async fn ca_conn_item_merge(
        conn: CaConn,
        tx1: Sender<(SocketAddr, CaConnEvent)>,
        addr: SocketAddr,
    ) -> Result<(), Error> {
        trace2!("ca_conn_consumer  begin  {}", addr);
        let ret = Self::ca_conn_item_merge_inner(Box::pin(conn), tx1.clone(), addr).await;
        trace2!("ca_conn_consumer  ended {}", addr);
        match ret {
            Ok(x) => {
                trace!("sending  CaConnEventValue::EndOfStream");
                tx1.send((addr, CaConnEvent::new_now(CaConnEventValue::EndOfStream(x))))
                    .await?;
            }
            Err(e) => {
                error!("ca_conn_item_merge received from inner: {}", e);
            }
        }
        Ok(())
    }

    async fn ca_conn_item_merge_inner(
        mut conn: Pin<Box<CaConn>>,
        tx1: Sender<(SocketAddr, CaConnEvent)>,
        addr: SocketAddr,
    ) -> Result<EndOfStreamReason, Error> {
        let mut eos_reason = None;
        while let Some(item) = conn.next().await {
            trace!("ca_conn_item_merge_inner  item {}", item.desc_short());
            if let Some(x) = &eos_reason {
                // TODO enable again, should not happen.
                // let e = Error::with_msg_no_trace(format!("CaConn delivered already eos  {addr}  {x:?}"));
                // error!("{e}");
                // return Err(e);
                warn!("CaConn {} EOS reason [{:?}] after [{:?}]", addr, x, eos_reason);
            }
            match item.value {
                CaConnEventValue::None
                | CaConnEventValue::EchoTimeout
                | CaConnEventValue::ConnCommandResult(..)
                | CaConnEventValue::ChannelCreateFail(..)
                | CaConnEventValue::ChannelStatus(..)
                | CaConnEventValue::Metrics(..) => {
                    if let Err(e) = tx1.send((addr, item)).await {
                        error!("channel send  {:?}", e);
                        return Err(e.into());
                    }
                }
                CaConnEventValue::EndOfStream(reason) => {
                    eos_reason = Some(reason);
                }
                CaConnEventValue::ChannelRemoved(_) => {
                    debug!("ca_conn_item_merge_inner  {:?}", item);
                    if let Err(e) = tx1.send((addr, item)).await {
                        error!("channel send  {:?}", e);
                        return Err(e.into());
                    }
                }
            }
        }
        if let Some(x) = eos_reason {
            Ok(x)
        } else {
            let e = Error::CaConnEndWithoutReason;
            Err(e)
        }
    }

    fn check_channel_states(&mut self, tsnow: Instant, stnow: SystemTime) -> Result<(), Error> {
        let (mut search_pending_count, mut assigned_without_health_update) = self.update_channel_state_counts();
        let mut channel_health_timeout_soon = 0;
        let mut channel_health_timeout_reached = 0;
        let mut cmd_remove_channel = Vec::new();
        let mut cmd_add_channel = Vec::new();
        let k = self.chan_check_next.take();
        let it = if let Some(last) = k {
            trace!("check_chans  start at {:?}", last);
            self.channel_states.range_mut(last..)
        } else {
            self.channel_states.range_mut(..)
        };
        #[allow(unused)]
        let mut st_qu_2 = VecDeque::new();
        let mut lt_qu_2 = VecDeque::new();
        for (i, (ch, st)) in it.enumerate() {
            match &mut st.value {
                ChannelStateValue::Active(st2) => match st2 {
                    ActiveChannelState::Init { since: _ } => {
                        // TODO no longer used? remove?
                        self.mett.logic_err().inc();
                    }
                    ActiveChannelState::WaitForStatusSeriesId { since } => {
                        let dt = stnow.duration_since(*since).unwrap_or(Duration::ZERO);
                        if dt > Duration::from_millis(20000) {
                            warn!("timeout can not get status series id for {:?}", ch);
                            *st2 = ActiveChannelState::Init { since: stnow };
                        } else {
                            // TODO
                        }
                    }
                    ActiveChannelState::WithStatusSeriesId(st3) => match &mut st3.inner {
                        WithStatusSeriesIdStateInner::UnknownAddress { .. } => {
                            self.find_ioc_queue_set.push_back(ch.clone());
                            st3.inner = WithStatusSeriesIdStateInner::AddrSearchPlanned { since: stnow };
                        }
                        WithStatusSeriesIdStateInner::AddrSearchPending { since } => {
                            let dt = stnow.duration_since(*since).unwrap_or(Duration::ZERO);
                            if dt > SEARCH_PENDING_TIMEOUT {
                                info!("should receive some error indication instead of timeout for {:?}", ch);
                                st3.inner = WithStatusSeriesIdStateInner::NoAddress { since: stnow };
                                search_pending_count -= 1;
                            }
                        }
                        WithStatusSeriesIdStateInner::WithAddress {
                            addr: addr_v4,
                            state: st4,
                        } => {
                            use WithAddressState::*;
                            match st4 {
                                Unassigned { since } => {
                                    if assigned_without_health_update < CHANNEL_MAX_WITHOUT_HEALTH_UPDATE as _ {
                                        if *since + CHANNEL_UNASSIGNED_TIMEOUT < stnow {
                                            assigned_without_health_update += 1;
                                            let cmd = ChannelAddWithAddr {
                                                ch_cfg: st.config.clone(),
                                                cssid: st3.cssid.clone(),
                                                addr: SocketAddr::V4(*addr_v4),
                                            };
                                            cmd_add_channel.push(cmd);
                                        }
                                    }
                                }
                                Assigned(st4) => {
                                    if st4.updated + CHANNEL_HEALTH_TIMEOUT / 3 < stnow {
                                        channel_health_timeout_soon += 1;
                                    }
                                    if st4.updated + CHANNEL_HEALTH_TIMEOUT < stnow {
                                        channel_health_timeout_reached += 1;
                                        trace!("health timeout  channel {:?}  ~~~~~~~~~~~~~~~~~~~", ch);
                                        // TODO
                                        error!("TODO  health timeout  channel {:?}  ~~~~~~~~~~~~~~~~~~~", ch);
                                        if true {
                                            std::process::exit(1);
                                        }
                                        let addr = SocketAddr::V4(*addr_v4);
                                        cmd_remove_channel.push((addr, ch.clone()));
                                        bump_backoff(&mut st3.addr_find_backoff);
                                        let snew = MaybeWrongAddressState::new(stnow, st3.addr_find_backoff);
                                        if series::dbg::dbg_chn(ch.name()) {
                                            info!(
                                                "check_channel_states  update state  {:?}  {:?}  {:?}",
                                                ch, addr, snew
                                            );
                                        }
                                        st3.inner = WithStatusSeriesIdStateInner::MaybeWrongAddress(snew);
                                        let item = ChannelStatusItem::new_closed_conn_timeout(stnow, st3.cssid.clone());
                                        let (tsev, val) = item.to_ts_val();
                                        let deque = &mut lt_qu_2;
                                        st3.writer_status.as_mut().unwrap().write(
                                            serieswriter::fixgridwriter::ChannelStatusWriteValue::new(tsev, val),
                                            &mut (),
                                            tsnow,
                                            tsev,
                                            deque,
                                        )?;
                                    }
                                }
                            }
                        }
                        WithStatusSeriesIdStateInner::NoAddress { since } => {
                            if *since + NO_ADDRESS_STAY < stnow {
                                st3.inner = WithStatusSeriesIdStateInner::UnknownAddress { since: stnow };
                            }
                        }
                        WithStatusSeriesIdStateInner::MaybeWrongAddress(st4) => {
                            if st4.since + st4.backoff_dt < stnow {
                                if series::dbg::dbg_chn(ch.name()) {
                                    info!(
                                        "check_channel_states  MaybeWrongAddress  set to  AddrSearchPlanned  {:?}",
                                        ch
                                    );
                                }
                                self.find_ioc_queue_set.push_back(ch.clone());
                                st3.inner = WithStatusSeriesIdStateInner::AddrSearchPlanned { since: stnow };
                            } else {
                                if series::dbg::dbg_chn(ch.name()) {
                                    // info!("MaybeWrongAddress  back off  {:?}", ch);
                                }
                            }
                        }
                        WithStatusSeriesIdStateInner::UnassigningForConfigChange(st4) => {
                            if tsnow.saturating_duration_since(st4.since) >= UNASSIGN_FOR_CONFIG_CHANGE_TIMEOUT {
                                debug!("timeout unassign for config change");
                            }
                        }
                        WithStatusSeriesIdStateInner::AddrSearchPlanned { since: _ } => {
                            // TODO record elapsed from since for metrics
                            if series::dbg::dbg_chn(ch.name()) {
                                info!("AddrSearchPlanned  {:?}  {:?}", ch, search_pending_count);
                            }
                        }
                    },
                },
                ChannelStateValue::ToRemove { .. } => {
                    // TODO if assigned to some address,
                }
                ChannelStateValue::InitDummy => {
                    // TODO must never occur
                }
            }
            if i >= CHECK_CHANS_PER_TICK {
                self.chan_check_next = Some(ch.clone());
                break;
            }
        }
        self.mett
            .channel_health_timeout_reached()
            .set(channel_health_timeout_reached);
        self.mett.channel_health_timeout_soon().set(channel_health_timeout_soon);
        loop {
            break if search_pending_count >= CURRENT_SEARCH_PENDING_MAX as _ {
            } else {
                if let Some(ch) = self.find_ioc_queue_set.pop_front() {
                    if let Some(st1) = self.channel_states.get_mut(&ch) {
                        match &mut st1.value {
                            ChannelStateValue::Active(st2) => match st2 {
                                ActiveChannelState::WithStatusSeriesId(st3) => {
                                    if series::dbg::dbg_chn(ch.name()) {
                                        info!("issue ioc search  {:?}", ch);
                                    } else {
                                        trace!("issue ioc search  {:?}", ch);
                                    }
                                    search_pending_count += 1;
                                    st3.inner = WithStatusSeriesIdStateInner::AddrSearchPending { since: stnow };
                                    let qu = IocAddrQuery::uncached(ch.name().into());
                                    self.find_ioc_query_queue.push_back(qu);
                                    self.mett.ioc_search_start().inc();
                                }
                                _ => {}
                            },
                            _ => {}
                        }
                    }
                    continue;
                }
            };
        }
        self.storage_insert_st_qu.push_back(st_qu_2);
        self.storage_insert_lt_qu.push_back(lt_qu_2);
        for (addr, ch) in cmd_remove_channel {
            if let Some(g) = self.ca_conn_ress.get_mut(&addr) {
                let cmd = ConnCommand::channel_close(ch.name().into());
                g.cmd_queue.push_back(cmd);
            }
            let cmd = ChannelRemove { name: ch.name().into() };
            self.handle_remove_channel(cmd)?;
        }
        for cmd in cmd_add_channel {
            self.handle_add_channel_with_addr(cmd)?;
        }
        Ok(())
    }

    // TODO should use both counters and values
    fn update_channel_state_counts(&mut self) -> (u32, u32) {
        let mut unknown_address = 0;
        let mut search_pending = 0;
        let mut no_address = 0;
        let mut unassigned = 0;
        let mut assigned = 0;
        let mut connected = 0;
        let mut maybe_wrong_address = 0;
        let mut assigned_without_health_update = 0;
        for (_ch, st) in self.channel_states.iter() {
            match &st.value {
                ChannelStateValue::Active(st2) => match st2 {
                    ActiveChannelState::Init { .. } => {
                        unknown_address += 1;
                    }
                    ActiveChannelState::WaitForStatusSeriesId { .. } => {
                        unknown_address += 1;
                    }
                    ActiveChannelState::WithStatusSeriesId(st3) => match &st3.inner {
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
                        WithStatusSeriesIdStateInner::UnassigningForConfigChange(_) => {
                            assigned += 1;
                        }
                        WithStatusSeriesIdStateInner::AddrSearchPlanned { .. } => {
                            no_address += 1;
                        }
                    },
                },
                ChannelStateValue::ToRemove { .. } => {
                    unassigned += 1;
                }
                ChannelStateValue::InitDummy => {}
            }
        }
        self.mett.channel_unknown_address().set(unknown_address);
        self.mett.channel_search_pending().set(search_pending);
        self.mett.channel_no_address().set(no_address);
        self.mett.channel_unassigned().set(unassigned);
        // self.mett.channel_backoff().set(backoff);
        self.mett.channel_assigned().set(assigned);
        self.mett.channel_connected().set(connected);
        self.mett.channel_maybe_wrong_address().set(maybe_wrong_address);
        self.mett
            .channel_assigned_without_health_update()
            .set(assigned_without_health_update);
        (search_pending, assigned_without_health_update)
    }

    fn try_push_ca_conn_cmds(&mut self, cx: &mut Context) -> Option<Poll<Result<(), Error>>> {
        use Poll::*;
        let mut have_pending = false;
        let mut have_progress = false;
        for (addr, v) in self.ca_conn_ress.iter_mut() {
            let tx = &mut v.sender;
            loop {
                break if tx.is_sending() {
                    match tx.poll_unpin(cx) {
                        Ready(Ok(())) => {
                            self.mett.try_push_ca_conn_cmds_sent().inc();
                            have_progress = true;
                            continue;
                        }
                        Ready(Err(e)) => match e {
                            scywr::senderpolling::Error::NoSendInProgress => {
                                let e = Error::PushCmdsNoSendInProgress(*addr);
                                error!("try_push_ca_conn_cmds {}", e);
                                return Some(Ready(Err(e)));
                            }
                            scywr::senderpolling::Error::Closed(_) => {
                                // TODO
                                // Should be nothing to do here.
                                // The connection ended, which CaConnSet notices anyway.
                                // self.handle_connect_fail(addr)?;
                                self.mett.try_push_ca_conn_cmds_closed().inc();
                            }
                        },
                        Pending => {
                            have_pending = true;
                        }
                    }
                } else if let Some(item) = v.cmd_queue.pop_front() {
                    tx.as_mut().send_pin(item);
                    continue;
                } else {
                    ()
                };
            }
        }
        if have_progress {
            Some(Ready(Ok(())))
        } else if have_pending {
            Some(Pending)
        } else {
            None
        }
    }

    fn handle_own_ticker_tick(mut self: Pin<&mut Self>, cx: &mut Context) -> Result<(), Error> {
        // debug!("handle_own_ticker_tick  {}", Self::self_name());
        if !self.ready_for_end_of_stream() {
            self.ticker = Self::new_self_ticker();
            let _ = self.ticker.poll_unpin(cx);
            // cx.waker().wake_by_ref();
        }
        self.handle_check_health()?;
        {
            if self.storage_insert_st_qu_l1.len() != 0 {
                let a = std::mem::replace(&mut self.storage_insert_st_qu_l1, VecDeque::new());
                self.storage_insert_st_qu.push_back(a);
            }
        }
        {
            if self.storage_insert_lt_qu_l1.len() != 0 {
                let a = std::mem::replace(&mut self.storage_insert_lt_qu_l1, VecDeque::new());
                self.storage_insert_lt_qu.push_back(a);
            }
        }
        {
            // let item = std::mem::replace(&mut self.ca_connset_metrics, CaConnSetMetrics::new());
            let item = self.mett.take_and_reset();
            let item = CaConnSetItem::Metrics(item);
            self.connset_out_queue.push_back(item);
        }
        Ok(())
    }
}

struct PendingProgress {
    pending: bool,
    progress: bool,
}

impl PendingProgress {
    fn new() -> Self {
        Self {
            pending: false,
            progress: false,
        }
    }

    fn mark_pending(&mut self) {
        self.pending = true;
    }

    fn mark_progress(&mut self) {
        self.progress = true;
    }

    fn pending(&self) -> bool {
        self.pending
    }

    fn progress(&self) -> bool {
        self.progress
    }
}

fn merge_pending_progress<E>(res: Option<Poll<Result<(), E>>>, penpro: &mut PendingProgress) -> Result<(), E>
where
    E: std::error::Error,
{
    use Poll::*;
    match res {
        Some(x) => match x {
            Ready(x) => match x {
                Ok(()) => {
                    penpro.mark_progress();
                    Ok(())
                }
                Err(e) => {
                    penpro.mark_progress();
                    Err(e)
                }
            },
            Pending => {
                penpro.mark_pending();
                Ok(())
            }
        },
        None => Ok(()),
    }
}

fn sender_polling_send<T, F>(
    qu: &mut VecDeque<T>,
    mut sender: Pin<&mut SenderPolling<T>>,
    cx: &mut Context,
    on_send_ok: F,
) -> Option<Poll<Result<(), Error>>>
where
    T: Unpin,
    F: FnOnce(),
{
    use Poll::*;
    if sender.is_idle() {
        if let Some(item) = qu.pop_front() {
            sender.as_mut().send_pin(item);
        }
    }
    if sender.is_sending() {
        match sender.poll_unpin(cx) {
            Ready(Ok(())) => {
                on_send_ok();
                Some(Ready(Ok(())))
            }
            Ready(Err(e)) => {
                error!("sender_polling_send {}", e);
                Some(Ready(Err(e.into())))
            }
            Pending => Some(Pending),
        }
    } else {
        None
    }
}

impl Stream for CaConnSet {
    type Item = CaConnSetItem;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        trace4!("CaConnSet  poll  begin");
        let poll_ts1 = Instant::now();
        self.mett.poll_fn_begin().inc();
        let ret = loop {
            trace4!("CaConnSet  poll  loop");
            {
                let mut self2 = self.as_mut().get_mut();
                self2.mett.poll_loop_begin().inc();
                self2
                    .mett
                    .channel_info_query_queue_len()
                    .set(self2.channel_info_query_qu.len() as _);
                self2
                    .mett
                    .channel_info_query_sender_len()
                    .set(self2.channel_info_query_sender.len().unwrap_or(0) as _);
                self2
                    .mett
                    .channel_info_res_tx_len()
                    .set(self2.channel_info_res_tx.len() as _);
                self2
                    .mett
                    .find_ioc_query_sender_len()
                    .set(self2.find_ioc_query_sender.len().unwrap_or(0) as _);
                self2.mett.ca_conn_res_tx_len().set(self2.ca_conn_res_tx.len() as _);
            }

            let mut penpro = PendingProgress::new();

            let res = self.try_push_ca_conn_cmds(cx);
            if let Err(e) = merge_pending_progress(res, &mut penpro) {
                break Ready(Some(CaConnSetItem::Error(e)));
            }

            match self.ticker.poll_unpin(cx) {
                Ready(()) => match self.as_mut().handle_own_ticker_tick(cx) {
                    Ok(()) => {
                        penpro.mark_progress();
                    }
                    Err(e) => {
                        error!("ticker {}", e);
                        break Ready(Some(CaConnSetItem::Error(e)));
                    }
                },
                Pending => {
                    penpro.mark_pending();
                }
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
                                self.mett.ca_conn_task_join_done_ok().inc();
                                debug!("CaConn {} finished well  left {}", addr, left);
                            }
                            Ok(Err(e)) => {
                                self.mett.ca_conn_task_join_done_err().inc();
                                error!("CaConn {} task error: {}  left {}", addr, e, left);
                            }
                            Err(e) => {
                                self.mett.ca_conn_task_join_err().inc();
                                error!("CaConn {} join error: {}  left {}", addr, e, left);
                            }
                        }
                        penpro.mark_progress();
                    }
                    Pending => {
                        penpro.mark_pending();
                    }
                }
            }

            {
                let this = self.as_mut().get_mut();
                let qu = &mut this.storage_insert_st_qu;
                let tx = this.storage_insert_st_tx.as_mut();
                let counter = this.mett.storage_insert_queue_send();
                let x = sender_polling_send(qu, tx, cx, || {
                    counter.inc();
                });
                if let Err(e) = merge_pending_progress(x, &mut penpro) {
                    break Ready(Some(CaConnSetItem::Error(e)));
                }
            }
            {
                let this = self.as_mut().get_mut();
                let qu = &mut this.storage_insert_lt_qu;
                let tx = this.storage_insert_lt_tx.as_mut();
                let counter = this.mett.storage_insert_queue_send();
                let x = sender_polling_send(qu, tx, cx, || {
                    counter.inc();
                });
                if let Err(e) = merge_pending_progress(x, &mut penpro) {
                    break Ready(Some(CaConnSetItem::Error(e)));
                }
            }
            {
                let this = self.as_mut().get_mut();
                let qu = &mut this.find_ioc_query_queue;
                let tx = this.find_ioc_query_sender.as_mut();
                let x = sender_polling_send(qu, tx, cx, || ());
                if let Err(e) = merge_pending_progress(x, &mut penpro) {
                    break Ready(Some(CaConnSetItem::Error(e)));
                }
            }
            {
                let this = self.as_mut().get_mut();
                let qu = &mut this.channel_info_query_qu;
                let tx = this.channel_info_query_sender.as_mut();
                let x = sender_polling_send(qu, tx, cx, || ());
                if let Err(e) = merge_pending_progress(x, &mut penpro) {
                    break Ready(Some(CaConnSetItem::Error(e)));
                }
            }

            match self.find_ioc_res_rx.as_mut().poll_next(cx) {
                Ready(Some(x)) => match self.handle_ioc_query_result(x) {
                    Ok(()) => {
                        penpro.mark_progress();
                    }
                    Err(e) => break Ready(Some(CaConnSetItem::Error(e))),
                },
                Ready(None) => {
                    // TODO trigger shutdown because of error
                }
                Pending => {
                    penpro.mark_pending();
                }
            }

            match self.ca_conn_res_rx.as_mut().poll_next(cx) {
                Ready(Some((addr, ev))) => match self.handle_ca_conn_event(addr, ev) {
                    Ok(()) => {
                        penpro.mark_progress();
                    }
                    Err(e) => break Ready(Some(CaConnSetItem::Error(e))),
                },
                Ready(None) => {}
                Pending => {
                    penpro.mark_pending();
                }
            }

            match self.channel_info_res_rx.as_mut().poll_next(cx) {
                Ready(Some(x)) => match self.handle_series_lookup_result(x) {
                    Ok(()) => {
                        penpro.mark_progress();
                    }
                    Err(e) => break Ready(Some(CaConnSetItem::Error(e))),
                },
                Ready(None) => {}
                Pending => {
                    penpro.mark_pending();
                }
            }

            match self.connset_inp_rx.as_mut().poll_next(cx) {
                Ready(Some(x)) => match self.handle_event(x) {
                    Ok(()) => {
                        penpro.mark_progress();
                    }
                    Err(e) => break Ready(Some(CaConnSetItem::Error(e))),
                },
                Ready(None) => {
                    warn!("connset_inp_rx broken?")
                }
                Pending => {
                    penpro.mark_pending();
                }
            }

            break if self.ready_for_end_of_stream() {
                self.mett.ready_for_end_of_stream().inc();
                if penpro.progress() {
                    self.mett.ready_for_end_of_stream_with_progress().inc();
                    continue;
                } else {
                    Ready(None)
                }
            } else {
                if penpro.progress() {
                    self.mett.poll_reloop().inc();
                    continue;
                } else {
                    if penpro.pending() {
                        self.mett.poll_pending().inc();
                        Pending
                    } else {
                        self.mett.poll_no_progress_no_pending().inc();
                        let e = Error::NoProgressNoPending;
                        Ready(Some(CaConnSetItem::Error(e)))
                    }
                }
            };
        };
        trace4!("CaConnSet  poll  done");
        let poll_ts2 = Instant::now();
        let dt = poll_ts2.saturating_duration_since(poll_ts1);
        self.mett.poll_all_dt().push_dur_100us(dt);
        ret
    }
}
