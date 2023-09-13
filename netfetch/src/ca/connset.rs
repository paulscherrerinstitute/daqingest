use super::connset_input_merge::InputMerge;
use super::findioc::FindIocRes;
use super::statemap;
use super::statemap::ChannelState;
use crate::ca::conn::CaConn;
use crate::ca::conn::CaConnEvent;
use crate::ca::conn::CaConnEventValue;
use crate::ca::conn::CaConnOpts;
use crate::ca::conn::ConnCommand;
use crate::ca::statemap::CaConnState;
use crate::ca::statemap::ConnectionState;
use crate::ca::statemap::ConnectionStateValue;
use crate::ca::statemap::WithAddressState;
use crate::daemon_common::Channel;
use crate::errconv::ErrConv;
use crate::rt::JoinHandle;
use crate::rt::TokMx;
use crate::senderpolling::SenderPolling;
use async_channel::Receiver;
use async_channel::Sender;
use atomic::AtomicUsize;
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
use netpod::Shape;
use scywr::iteminsertqueue::ChannelStatusItem;
use scywr::iteminsertqueue::QueryItem;
use series::series::Existence;
use series::ChannelStatusSeriesId;
use series::SeriesId;
use statemap::ActiveChannelState;
use statemap::CaConnStateValue;
use statemap::ChannelStateMap;
use statemap::ChannelStateValue;
use statemap::WithStatusSeriesIdState;
use statemap::WithStatusSeriesIdStateInner;
use statemap::CHANNEL_STATUS_DUMMY_SCALAR_TYPE;
use stats::CaConnSetStats;
use stats::CaConnStats;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::net::SocketAddrV4;
use std::pin::Pin;
use std::sync::atomic;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use taskrun::tokio;

const DO_ASSIGN_TO_CA_CONN: bool = true;
const CHECK_CHANS_PER_TICK: usize = 10000;
pub const SEARCH_BATCH_MAX: usize = 256;
pub const CURRENT_SEARCH_PENDING_MAX: usize = SEARCH_BATCH_MAX * 4;
const UNKNOWN_ADDRESS_STAY: Duration = Duration::from_millis(2000);
const NO_ADDRESS_STAY: Duration = Duration::from_millis(20000);
const SEARCH_PENDING_TIMEOUT: Duration = Duration::from_millis(30000);
const SEARCH_PENDING_TIMEOUT_WARN: Duration = Duration::from_millis(8000);

// TODO put all these into metrics
static SEARCH_REQ_MARK_COUNT: AtomicUsize = AtomicUsize::new(0);
static SEARCH_REQ_SEND_COUNT: AtomicUsize = AtomicUsize::new(0);
static SEARCH_REQ_RECV_COUNT: AtomicUsize = AtomicUsize::new(0);
static SEARCH_REQ_BATCH_SEND_COUNT: AtomicUsize = AtomicUsize::new(0);
static SEARCH_ANS_COUNT: AtomicUsize = AtomicUsize::new(0);

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
    sender: Sender<ConnCommand>,
    stats: Arc<CaConnStats>,
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

#[derive(Debug)]
pub enum ConnSetCmd {
    ChannelAdd(ChannelAdd),
    ChannelRemove(ChannelRemove),
    CheckHealth(Instant),
    Shutdown,
}

#[derive(Debug)]
pub enum CaConnSetEvent {
    ConnSetCmd(ConnSetCmd),
}

#[derive(Debug, Clone)]
pub enum CaConnSetItem {
    Error(Error),
    Healthy(Instant, Instant),
}

pub struct CaConnSetCtrl {
    tx: Sender<CaConnSetEvent>,
    rx: Receiver<CaConnSetItem>,
    jh: JoinHandle<Result<(), Error>>,
}

impl CaConnSetCtrl {
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
        self.tx.send(CaConnSetEvent::ConnSetCmd(cmd)).await?;
        Ok(())
    }

    pub async fn join(self) -> Result<(), Error> {
        self.jh.await.map_err(|e| Error::with_msg_no_trace(e.to_string()))??;
        Ok(())
    }
}

#[derive(Debug)]
pub struct IocAddrQuery {
    pub name: String,
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
    connset_inp_rx: Receiver<CaConnSetEvent>,
    channel_info_query_queue: VecDeque<ChannelInfoQuery>,
    channel_info_query_sender: SenderPolling<ChannelInfoQuery>,
    channel_info_query_tx: Sender<ChannelInfoQuery>,
    channel_info_res_tx: Sender<Result<ChannelInfoResult, Error>>,
    channel_info_res_rx: Receiver<Result<ChannelInfoResult, Error>>,
    find_ioc_query_queue: VecDeque<IocAddrQuery>,
    find_ioc_query_sender: SenderPolling<IocAddrQuery>,
    find_ioc_res_rx: Receiver<VecDeque<FindIocRes>>,
    storage_insert_queue: VecDeque<QueryItem>,
    storage_insert_sender: SenderPolling<QueryItem>,
    ca_conn_res_tx: Sender<(SocketAddr, CaConnEvent)>,
    ca_conn_res_rx: Receiver<(SocketAddr, CaConnEvent)>,
    connset_out_queue: VecDeque<CaConnSetItem>,
    connset_out_tx: Sender<CaConnSetItem>,
    shutdown_stopping: bool,
    shutdown_done: bool,
    chan_check_next: Option<Channel>,
    stats: CaConnSetStats,
    ioc_finder_jh: JoinHandle<Result<(), Error>>,
    await_ca_conn_jhs: VecDeque<(SocketAddr, JoinHandle<Result<(), Error>>)>,
}

impl CaConnSet {
    pub fn start(
        backend: String,
        local_epics_hostname: String,
        storage_insert_tx: Sender<QueryItem>,
        channel_info_query_tx: Sender<ChannelInfoQuery>,
        pgconf: Database,
    ) -> CaConnSetCtrl {
        let (ca_conn_res_tx, ca_conn_res_rx) = async_channel::bounded(5000);
        let (connset_inp_tx, connset_inp_rx) = async_channel::bounded(5000);
        let (connset_out_tx, connset_out_rx) = async_channel::bounded(5000);
        let (find_ioc_res_tx, find_ioc_res_rx) = async_channel::bounded(5000);
        let (find_ioc_query_tx, ioc_finder_jh) =
            super::finder::start_finder(find_ioc_res_tx.clone(), backend.clone(), pgconf);
        let (channel_info_res_tx, channel_info_res_rx) = async_channel::bounded(5000);
        let connset = Self {
            backend,
            local_epics_hostname,
            ca_conn_ress: BTreeMap::new(),
            channel_states: ChannelStateMap::new(),
            connset_inp_rx,
            channel_info_query_queue: VecDeque::new(),
            channel_info_query_sender: SenderPolling::new(channel_info_query_tx.clone()),
            channel_info_query_tx,
            channel_info_res_tx,
            channel_info_res_rx,
            find_ioc_query_queue: VecDeque::new(),
            find_ioc_query_sender: SenderPolling::new(find_ioc_query_tx),
            find_ioc_res_rx,
            storage_insert_queue: VecDeque::new(),
            storage_insert_sender: SenderPolling::new(storage_insert_tx),
            ca_conn_res_tx,
            ca_conn_res_rx,
            shutdown_stopping: false,
            shutdown_done: false,
            chan_check_next: None,
            stats: CaConnSetStats::new(),
            connset_out_tx,
            connset_out_queue: VecDeque::new(),
            // connset_out_sender: SenderPolling::new(connset_out_tx),
            ioc_finder_jh,
            await_ca_conn_jhs: VecDeque::new(),
        };
        // TODO await on jh
        let jh = tokio::spawn(CaConnSet::run(connset));
        CaConnSetCtrl {
            tx: connset_inp_tx,
            rx: connset_out_rx,
            jh,
        }
    }

    async fn run(mut this: CaConnSet) -> Result<(), Error> {
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
            },
        }
    }

    fn handle_ca_conn_event(&mut self, addr: SocketAddr, ev: CaConnEvent) -> Result<(), Error> {
        match ev.value {
            CaConnEventValue::None => Ok(()),
            CaConnEventValue::EchoTimeout => todo!(),
            CaConnEventValue::ConnCommandResult(_) => todo!(),
            CaConnEventValue::QueryItem(item) => {
                self.storage_insert_queue.push_back(item);
                Ok(())
            }
            CaConnEventValue::EndOfStream => self.handle_ca_conn_eos(addr),
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

    fn handle_add_channel(&mut self, add: ChannelAdd) -> Result<(), Error> {
        trace3!("handle_add_channel {}", add.name);
        if self.shutdown_stopping {
            trace3!("handle_add_channel but shutdown_stopping");
            return Ok(());
        }
        // TODO should I add the transition through ActiveChannelState::Init as well?
        let ch = Channel::new(add.name.clone());
        let _st = self.channel_states.inner().entry(ch).or_insert_with(|| ChannelState {
            value: ChannelStateValue::Active(ActiveChannelState::WaitForStatusSeriesId {
                since: SystemTime::now(),
            }),
        });
        let item = ChannelInfoQuery {
            backend: add.backend,
            channel: add.name,
            scalar_type: CHANNEL_STATUS_DUMMY_SCALAR_TYPE,
            shape_dims: Vec::new(),
            tx: Box::pin(SeriesLookupSender {
                tx: self.channel_info_res_tx.clone(),
            }),
        };
        self.channel_info_query_queue.push_back(item);
        Ok(())
    }

    fn handle_add_channel_with_status_id(&mut self, add: ChannelAddWithStatusId) -> Result<(), Error> {
        trace3!("handle_add_channel_with_status_id {}", add.name);
        if self.shutdown_stopping {
            debug!("handle_add_channel but shutdown_stopping");
            return Ok(());
        }
        let ch = Channel::new(add.name.clone());
        if let Some(chst) = self.channel_states.inner().get_mut(&ch) {
            if let ChannelStateValue::Active(chst2) = &mut chst.value {
                if let ActiveChannelState::WaitForStatusSeriesId { .. } = chst2 {
                    *chst2 = ActiveChannelState::WithStatusSeriesId {
                        status_series_id: add.cssid,
                        state: WithStatusSeriesIdState {
                            inner: WithStatusSeriesIdStateInner::SearchPending {
                                since: SystemTime::now(),
                            },
                        },
                    };
                    let qu = IocAddrQuery { name: add.name };
                    self.find_ioc_query_queue.push_back(qu);
                } else {
                    warn!("TODO have a status series id but no more channel");
                }
            } else {
                warn!("TODO have a status series id but no more channel");
            }
        } else {
            warn!("TODO have a status series id but no more channel");
        }
        Ok(())
    }

    fn handle_add_channel_with_addr(&mut self, add: ChannelAddWithAddr) -> Result<(), Error> {
        if self.shutdown_stopping {
            trace3!("handle_add_channel but shutdown_stopping");
            return Ok(());
        }
        if !self.ca_conn_ress.contains_key(&add.addr) {
            let c = self.create_ca_conn(add.clone())?;
            self.ca_conn_ress.insert(add.addr, c);
        }
        let conn_ress = self.ca_conn_ress.get_mut(&add.addr).unwrap();
        let cmd = ConnCommand::channel_add(add.name, add.cssid);
        // TODO not the nicest
        let tx = conn_ress.sender.clone();
        tokio::spawn(async move { tx.send(cmd).await });
        Ok(())
    }

    fn handle_remove_channel(&mut self, add: ChannelRemove) -> Result<(), Error> {
        if self.shutdown_stopping {
            return Ok(());
        }
        let ch = Channel::new(add.name);
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
                        WithStatusSeriesIdStateInner::SearchPending { .. } => {
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
                    },
                },
                ChannelStateValue::ToRemove { .. } => {}
            }
        }
        Ok(())
    }

    fn handle_ioc_query_result(&mut self, res: VecDeque<FindIocRes>) -> Result<(), Error> {
        if self.shutdown_stopping {
            return Ok(());
        }
        trace3!("handle_ioc_query_result");
        for e in res {
            let ch = Channel::new(e.channel.clone());
            if let Some(chst) = self.channel_states.inner().get_mut(&ch) {
                if let ChannelStateValue::Active(ast) = &mut chst.value {
                    if let ActiveChannelState::WithStatusSeriesId {
                        status_series_id,
                        state,
                    } = ast
                    {
                        if let Some(addr) = e.addr {
                            trace3!("ioc found {e:?}");
                            let add = ChannelAddWithAddr {
                                backend: self.backend.clone(),
                                name: e.channel,
                                addr: SocketAddr::V4(addr),
                                cssid: status_series_id.clone(),
                                local_epics_hostname: self.local_epics_hostname.clone(),
                            };
                            let since = SystemTime::now();
                            state.inner = WithStatusSeriesIdStateInner::WithAddress {
                                addr,
                                state: WithAddressState::Unassigned { since },
                            };
                            // TODO move state change also in there?
                            self.handle_add_channel_with_addr(add)?;
                        } else {
                            trace3!("ioc not found {e:?}");
                            let since = SystemTime::now();
                            state.inner = WithStatusSeriesIdStateInner::UnknownAddress { since };
                        }
                    } else {
                        warn!("TODO got address but no longer active");
                    }
                } else {
                    warn!("TODO got address but no longer active");
                }
            } else {
                warn!("ioc addr lookup done but channel no longer here");
            }
        }
        Ok(())
    }

    fn handle_check_health(&mut self, ts1: Instant) -> Result<(), Error> {
        if self.shutdown_stopping {
            return Ok(());
        }
        debug!("TODO handle_check_health");
        let ts2 = Instant::now();
        let item = CaConnSetItem::Healthy(ts1, ts2);
        self.connset_out_queue.push_back(item);
        Ok(())
    }

    fn handle_shutdown(&mut self) -> Result<(), Error> {
        if self.shutdown_stopping {
            return Ok(());
        }
        debug!("handle_shutdown");
        self.shutdown_stopping = true;
        self.channel_info_query_sender.drop();
        self.find_ioc_query_sender.drop();
        for (addr, res) in self.ca_conn_ress.iter() {
            let item = ConnCommand::shutdown();
            // TODO not the nicest
            let tx = res.sender.clone();
            tokio::spawn(async move { tx.send(item).await });
        }
        Ok(())
    }

    fn handle_ca_conn_eos(&mut self, addr: SocketAddr) -> Result<(), Error> {
        debug!("handle_ca_conn_eos {addr}");
        if let Some(e) = self.ca_conn_ress.remove(&addr) {
            self.await_ca_conn_jhs.push_back((addr, e.jh));
        } else {
            self.stats.ca_conn_task_eos_non_exist.inc();
            warn!("end-of-stream received for non-existent CaConn {addr}");
        }
        debug!("still CaConn left  {}", self.ca_conn_ress.len());
        Ok(())
    }

    fn ready_for_end_of_stream(&self) -> bool {
        if self.ca_conn_ress.len() > 0 {
            false
        } else if self.await_ca_conn_jhs.len() > 1 {
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
        debug!("create new CaConn  {:?}", addr);
        let conn = CaConn::new(
            opts,
            add.backend.clone(),
            addr_v4,
            add.local_epics_hostname,
            self.channel_info_query_tx.clone(),
        );
        let conn_tx = conn.conn_command_tx();
        let conn_stats = conn.stats();
        let ca_conn_res_tx = self.ca_conn_res_tx.clone();
        let jh = tokio::spawn(Self::ca_conn_item_merge(conn, ca_conn_res_tx, addr));
        let ca_conn_res = CaConnRes {
            state: CaConnState::new(CaConnStateValue::Fresh),
            sender: conn_tx,
            stats: conn_stats,
            jh,
        };
        Ok(ca_conn_res)
    }

    async fn ca_conn_item_merge(
        conn: CaConn,
        tx: Sender<(SocketAddr, CaConnEvent)>,
        addr: SocketAddr,
    ) -> Result<(), Error> {
        debug!("ca_conn_consumer  begin  {}", addr);
        let stats = conn.stats();
        let mut conn = conn;
        let mut ret = Ok(());
        while let Some(item) = conn.next().await {
            match item {
                Ok(item) => {
                    stats.conn_item_count.inc();
                    tx.send((addr, item)).await?;
                }
                Err(e) => {
                    error!("CaConn gives error: {e:?}");
                    ret = Err(e);
                }
            }
        }
        debug!("ca_conn_consumer  ended {}", addr);
        tx.send((
            addr,
            CaConnEvent {
                ts: Instant::now(),
                value: CaConnEventValue::EndOfStream,
            },
        ))
        .await?;
        debug!("ca_conn_consumer  signaled {}", addr);
        ret
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

    async fn conn_remove(
        ca_conn_ress: &TokMx<BTreeMap<SocketAddrV4, CaConnRes>>,
        addr: SocketAddrV4,
    ) -> Result<bool, Error> {
        // TODO make this lock-free.
        //warn!("Lock for conn_remove");
        if let Some(_caconn) = ca_conn_ress.lock().await.remove(&addr) {
            Ok(true)
        } else {
            Ok(false)
        }
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

    async fn check_channel_states(&mut self) -> Result<(), Error> {
        let (mut search_pending_count,) = self.update_channel_state_counts();
        let k = self.chan_check_next.take();
        let it = if let Some(last) = k {
            trace!("check_chans  start at {:?}", last);
            self.channel_states.inner().range_mut(last..)
        } else {
            self.channel_states.inner().range_mut(..)
        };
        let tsnow = SystemTime::now();
        let mut attempt_series_search = true;
        for (i, (ch, st)) in it.enumerate() {
            match &mut st.value {
                ChannelStateValue::Active(st2) => match st2 {
                    ActiveChannelState::Init { since: _ } => {
                        todo!()
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
                            let dt = tsnow.duration_since(*since).unwrap_or(Duration::ZERO);
                            if dt > UNKNOWN_ADDRESS_STAY {
                                //info!("UnknownAddress {} {:?}", i, ch);
                                if (search_pending_count as usize) < CURRENT_SEARCH_PENDING_MAX {
                                    search_pending_count += 1;
                                    state.inner = WithStatusSeriesIdStateInner::SearchPending { since: tsnow };
                                    SEARCH_REQ_MARK_COUNT.fetch_add(1, atomic::Ordering::AcqRel);
                                }
                            }
                        }
                        WithStatusSeriesIdStateInner::SearchPending { since } => {
                            //info!("SearchPending {} {:?}", i, ch);
                            let dt = tsnow.duration_since(*since).unwrap_or(Duration::ZERO);
                            if dt > SEARCH_PENDING_TIMEOUT {
                                info!("Search timeout for {ch:?}");
                                state.inner = WithStatusSeriesIdStateInner::NoAddress { since: tsnow };
                                search_pending_count -= 1;
                            }
                        }
                        WithStatusSeriesIdStateInner::WithAddress { addr: addr_v4, state } => {
                            //info!("WithAddress {} {:?}", i, ch);
                            use WithAddressState::*;
                            match state {
                                Unassigned { since } => {
                                    // TODO do I need this case anymore?
                                    #[cfg(DISABLED)]
                                    if DO_ASSIGN_TO_CA_CONN && *assign_at <= tsnow {
                                        let backend = self.backend.clone();
                                        let addr = SocketAddr::V4(*addr_v4);
                                        let name = ch.id().into();
                                        let cssid = status_series_id.clone();
                                        let local_epics_hostname = self.local_epics_hostname.clone();
                                        // This operation is meant to complete very quickly
                                        let add = ChannelAdd {
                                            backend: backend,
                                            name: name,
                                            addr,
                                            cssid,
                                            local_epics_hostname,
                                        };
                                        self.handle_add_channel(add).await?;
                                        let cs = ConnectionState {
                                            updated: tsnow,
                                            value: ConnectionStateValue::Unconnected,
                                        };
                                        // TODO if a matching CaConn does not yet exist, it gets created
                                        // via the command through the channel, so we can not await it here.
                                        // Therefore, would be good to have a separate status entry out of
                                        // the ca_conn_ress right here in a sync fashion.
                                        *state = WithAddressState::Assigned(cs);
                                        let item = QueryItem::ChannelStatus(ChannelStatusItem {
                                            ts: tsnow,
                                            series: SeriesId::new(status_series_id.id()),
                                            status: scywr::iteminsertqueue::ChannelStatus::AssignedToAddress,
                                        });
                                        match self.storage_insert_tx.send(item).await {
                                            Ok(_) => {}
                                            Err(_) => {
                                                // TODO feed into throttled log, or count as unlogged
                                            }
                                        }
                                    }
                                }
                                Assigned(_) => {
                                    // TODO check if channel is healthy and alive
                                }
                            }
                        }
                        WithStatusSeriesIdStateInner::NoAddress { since } => {
                            let dt = tsnow.duration_since(*since).unwrap_or(Duration::ZERO);
                            if dt > NO_ADDRESS_STAY {
                                state.inner = WithStatusSeriesIdStateInner::UnknownAddress { since: tsnow };
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
        Ok(())
    }

    fn update_channel_state_counts(&mut self) -> (u64,) {
        let mut unknown_address = 0;
        let mut search_pending = 0;
        let mut unassigned = 0;
        let mut assigned = 0;
        let mut no_address = 0;
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
                        WithStatusSeriesIdStateInner::SearchPending { .. } => {
                            search_pending += 1;
                        }
                        WithStatusSeriesIdStateInner::WithAddress { state, .. } => match state {
                            WithAddressState::Unassigned { .. } => {
                                unassigned += 1;
                            }
                            WithAddressState::Assigned(_) => {
                                assigned += 1;
                            }
                        },
                        WithStatusSeriesIdStateInner::NoAddress { .. } => {
                            no_address += 1;
                        }
                    },
                },
                ChannelStateValue::ToRemove { .. } => {
                    unknown_address += 1;
                }
            }
        }
        self.stats.channel_unknown_address.__set(unknown_address);
        self.stats.channel_search_pending.__set(search_pending);
        self.stats.channel_no_address.__set(no_address);
        self.stats.channel_unassigned.__set(unassigned);
        self.stats.channel_assigned.__set(assigned);
        (search_pending,)
    }
}

impl Stream for CaConnSet {
    type Item = CaConnSetItem;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        debug!("CaConnSet::poll");
        loop {
            let mut have_pending = false;

            if let Some(item) = self.connset_out_queue.pop_front() {
                break Ready(Some(item));
            }

            if let Some((addr, jh)) = self.await_ca_conn_jhs.front_mut() {
                match jh.poll_unpin(cx) {
                    Ready(x) => {
                        let addr = *addr;
                        self.await_ca_conn_jhs.pop_front();
                        debug!("await_ca_conn_jhs  still jhs left  {}", self.await_ca_conn_jhs.len());
                        match x {
                            Ok(Ok(())) => {
                                self.stats.ca_conn_task_join_done_ok.inc();
                                debug!("CaConn {addr} finished well");
                            }
                            Ok(Err(e)) => {
                                self.stats.ca_conn_task_join_done_err.inc();
                                error!("CaConn {addr} task error: {e}");
                            }
                            Err(e) => {
                                self.stats.ca_conn_task_join_err.inc();
                                error!("CaConn {addr} join error: {e}");
                            }
                        }
                    }
                    Pending => {}
                }
            }

            if self.storage_insert_sender.is_idle() {
                if let Some(item) = self.storage_insert_queue.pop_front() {
                    self.storage_insert_sender.send(item);
                }
            }
            if self.storage_insert_sender.is_sending() {
                match self.storage_insert_sender.poll_unpin(cx) {
                    Ready(Ok(())) => continue,
                    Ready(Err(e)) => {
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
                    self.find_ioc_query_sender.send(item);
                }
            }
            if self.find_ioc_query_sender.is_sending() {
                match self.find_ioc_query_sender.poll_unpin(cx) {
                    Ready(Ok(())) => continue,
                    Ready(Err(e)) => {
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
                    self.channel_info_query_sender.send(item);
                }
            }
            if self.channel_info_query_sender.is_sending() {
                match self.channel_info_query_sender.poll_unpin(cx) {
                    Ready(Ok(())) => continue,
                    Ready(Err(e)) => {
                        let e = Error::with_msg_no_trace("can not send into channel");
                        error!("{e}");
                        break Ready(Some(CaConnSetItem::Error(e)));
                    }
                    Pending => {
                        have_pending = true;
                    }
                }
            }

            let item = match self.find_ioc_res_rx.poll_next_unpin(cx) {
                Ready(Some(x)) => match self.handle_ioc_query_result(x) {
                    Ok(()) => continue,
                    Err(e) => Ready(Some(CaConnSetItem::Error(e))),
                },
                Ready(None) => Ready(None),
                Pending => {
                    have_pending = true;
                    Pending
                }
            };
            match item {
                Ready(Some(x)) => break Ready(Some(x)),
                _ => {}
            }

            let item = match self.ca_conn_res_rx.poll_next_unpin(cx) {
                Ready(Some((addr, ev))) => match self.handle_ca_conn_event(addr, ev) {
                    Ok(()) => continue,
                    Err(e) => Ready(Some(CaConnSetItem::Error(e))),
                },
                Ready(None) => Ready(None),
                Pending => {
                    have_pending = true;
                    Pending
                }
            };
            match item {
                Ready(Some(x)) => break Ready(Some(x)),
                _ => {}
            }

            let item = match self.channel_info_res_rx.poll_next_unpin(cx) {
                Ready(Some(x)) => match self.handle_series_lookup_result(x) {
                    Ok(()) => continue,
                    Err(e) => Ready(Some(CaConnSetItem::Error(e))),
                },
                Ready(None) => Ready(None),
                Pending => {
                    have_pending = true;
                    Pending
                }
            };
            match item {
                Ready(Some(x)) => break Ready(Some(x)),
                _ => {}
            }

            let item = match self.connset_inp_rx.poll_next_unpin(cx) {
                Ready(Some(x)) => match self.handle_event(x) {
                    Ok(()) => continue,
                    Err(e) => Ready(Some(CaConnSetItem::Error(e))),
                },
                Ready(None) => Ready(None),
                Pending => {
                    have_pending = true;
                    Pending
                }
            };
            match item {
                Ready(Some(x)) => break Ready(Some(x)),
                _ => {}
            }

            break if have_pending {
                if self.shutdown_stopping && self.ready_for_end_of_stream() {
                    Ready(None)
                } else {
                    Pending
                }
            } else if self.shutdown_stopping && self.ready_for_end_of_stream() {
                debug!("nothing to do but shutdown");
                Ready(None)
            } else {
                let e = Error::with_msg_no_trace("connset not pending and not shutdown");
                error!("{e}");
                Ready(Some(CaConnSetItem::Error(e)))
            };
        }
    }
}
