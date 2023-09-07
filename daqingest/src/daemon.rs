pub mod finder;
pub mod inserthook;

use async_channel::Receiver;
use async_channel::Sender;
use async_channel::WeakReceiver;
use dbpg::seriesbychannel::ChannelInfoQuery;
use err::Error;
use log::*;
use netfetch::ca::conn::CaConnEvent;
use netfetch::ca::conn::ConnCommand;
use netfetch::ca::connset::CaConnSet;
use netfetch::ca::connset::CaConnSetCtrl;
use netfetch::ca::findioc::FindIocRes;
use netfetch::ca::IngestCommons;
use netfetch::ca::SlowWarnable;
use netfetch::conf::CaIngestOpts;
use netfetch::daemon_common::Channel;
use netfetch::daemon_common::DaemonEvent;
use netfetch::metrics::ExtraInsertsConf;
use netfetch::metrics::StatsSet;
use netpod::Database;
use netpod::ScyllaConfig;
use scywr::insertworker::Ttls;
use scywr::iteminsertqueue as scywriiq;
use scywr::store::DataStore;
use scywriiq::ChannelStatus;
use scywriiq::ChannelStatusItem;
use scywriiq::CommonInsertItemQueue;
use scywriiq::ConnectionStatus;
use scywriiq::ConnectionStatusItem;
use scywriiq::QueryItem;
use serde::Serialize;
use series::series::Existence;
use series::ChannelStatusSeriesId;
use series::SeriesId;
use stats::DaemonStats;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::net::SocketAddrV4;
use std::sync::atomic;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use taskrun::tokio;
use tokio::task::JoinHandle;
use tracing::info_span;
use tracing::Instrument;

const CA_CONN_INSERT_QUEUE_MAX: usize = 256;

const CHANNEL_CHECK_INTERVAL: Duration = Duration::from_millis(5000);
const PRINT_ACTIVE_INTERVAL: Duration = Duration::from_millis(60000);
const PRINT_STATUS_INTERVAL: Duration = Duration::from_millis(20000);

pub struct DaemonOpts {
    backend: String,
    local_epics_hostname: String,
    array_truncate: usize,
    insert_item_queue_cap: usize,
    pgconf: Database,
    scyconf: ScyllaConfig,
    ttls: Ttls,
    #[allow(unused)]
    test_bsread_addr: Option<String>,
    insert_worker_count: usize,
    insert_scylla_sessions: usize,
}

impl DaemonOpts {
    pub fn backend(&self) -> &str {
        &self.backend
    }
}

pub struct Daemon {
    opts: DaemonOpts,
    tx: Sender<DaemonEvent>,
    rx: Receiver<DaemonEvent>,
    insert_queue_counter: Arc<AtomicUsize>,
    count_unknown_address: usize,
    count_search_pending: usize,
    count_search_sent: usize,
    count_no_address: usize,
    count_unassigned: usize,
    count_assigned: usize,
    last_status_print: SystemTime,
    insert_workers_jh: Vec<JoinHandle<()>>,
    ingest_commons: Arc<IngestCommons>,
    caconn_last_channel_check: Instant,
    stats: Arc<DaemonStats>,
    shutting_down: bool,
    insert_rx_weak: WeakReceiver<QueryItem>,
    channel_info_query_tx: Sender<ChannelInfoQuery>,
    connset_ctrl: CaConnSetCtrl,
    query_item_tx: Sender<QueryItem>,
}

impl Daemon {
    pub async fn new(opts: DaemonOpts) -> Result<Self, Error> {
        let datastore = DataStore::new(&opts.scyconf)
            .await
            .map_err(|e| Error::with_msg_no_trace(e.to_string()))?;
        let datastore = Arc::new(datastore);
        let (daemon_ev_tx, daemon_ev_rx) = async_channel::bounded(32);

        // TODO keep join handles and await later
        let (channel_info_query_tx, ..) = dbpg::seriesbychannel::start_lookup_workers(4, &opts.pgconf)
            .await
            .map_err(|e| Error::with_msg_no_trace(e.to_string()))?;

        let common_insert_item_queue = Arc::new(CommonInsertItemQueue::new(opts.insert_item_queue_cap));
        let insert_queue_counter = Arc::new(AtomicUsize::new(0));

        // Insert queue hook
        let rx = inserthook::active_channel_insert_hook(common_insert_item_queue.receiver().unwrap());
        let common_insert_item_queue_2 = rx;

        let conn_set_ctrl = CaConnSet::start(
            opts.backend.clone(),
            opts.local_epics_hostname.clone(),
            common_insert_item_queue.sender().unwrap().inner().clone(),
            channel_info_query_tx.clone(),
            opts.pgconf.clone(),
        );

        let ingest_commons = IngestCommons {
            pgconf: Arc::new(opts.pgconf.clone()),
            backend: opts.backend().into(),
            local_epics_hostname: opts.local_epics_hostname.clone(),
            insert_item_queue: common_insert_item_queue.clone(),
            data_store: datastore.clone(),
            insert_ivl_min: Arc::new(AtomicU64::new(0)),
            extra_inserts_conf: tokio::sync::Mutex::new(ExtraInsertsConf::new()),
            store_workers_rate: Arc::new(AtomicU64::new(20000)),
            insert_frac: Arc::new(AtomicU64::new(1000)),
            insert_workers_running: Arc::new(AtomicU64::new(0)),
        };
        let ingest_commons = Arc::new(ingest_commons);

        let use_rate_limit_queue = false;

        // TODO use a new stats type:
        let store_stats = Arc::new(stats::CaConnStats::new());
        let ttls = opts.ttls.clone();
        let insert_worker_opts = Arc::new(ingest_commons.as_ref().into());
        let jh_insert_workers = scywr::insertworker::spawn_scylla_insert_workers(
            opts.scyconf.clone(),
            opts.insert_scylla_sessions,
            opts.insert_worker_count,
            common_insert_item_queue_2.clone(),
            insert_worker_opts,
            store_stats.clone(),
            use_rate_limit_queue,
            ttls,
        )
        .await?;

        #[cfg(feature = "bsread")]
        if let Some(bsaddr) = &opts.test_bsread_addr {
            //netfetch::zmtp::Zmtp;
            let zmtpopts = ingest_bsread::zmtp::ZmtpClientOpts {
                backend: opts.backend().into(),
                addr: bsaddr.parse().unwrap(),
                do_pulse_id: false,
                rcvbuf: None,
                array_truncate: Some(1024),
                process_channel_count_limit: Some(32),
            };
            let client = ingest_bsread::bsreadclient::BsreadClient::new(
                zmtpopts,
                ingest_commons.insert_item_queue.sender().unwrap().inner().clone(),
                channel_info_query_tx.clone(),
            )
            .await
            .map_err(|e| Error::from(e.to_string()))?;
            let fut = {
                async move {
                    let mut client = client;
                    client.run().await.map_err(|e| Error::from(e.to_string()))?;
                    Ok::<_, Error>(())
                }
            };
            // TODO await on shutdown
            let _jh = tokio::spawn(fut);
            //let mut jhs = Vec::new();
            //jhs.push(jh);
            //futures_util::future::join_all(jhs).await;
            //jh.await.map_err(|e| e.to_string()).map_err(Error::from)??;
        }

        let ret = Self {
            opts,
            tx: daemon_ev_tx,
            rx: daemon_ev_rx,
            insert_queue_counter,
            count_unknown_address: 0,
            count_search_pending: 0,
            count_search_sent: 0,
            count_no_address: 0,
            count_unassigned: 0,
            count_assigned: 0,
            last_status_print: SystemTime::now(),
            insert_workers_jh: jh_insert_workers,
            ingest_commons,
            caconn_last_channel_check: Instant::now(),
            stats: Arc::new(DaemonStats::new()),
            shutting_down: false,
            insert_rx_weak: common_insert_item_queue_2.downgrade(),
            channel_info_query_tx,
            connset_ctrl: conn_set_ctrl,
            query_item_tx: common_insert_item_queue.sender().unwrap().inner().clone(),
        };
        Ok(ret)
    }

    fn stats(&self) -> &Arc<DaemonStats> {
        &self.stats
    }

    fn allow_create_new_connections(&self) -> bool {
        !self.shutting_down
    }

    async fn check_caconn_chans(&mut self) -> Result<(), Error> {
        if self.caconn_last_channel_check.elapsed() > CHANNEL_CHECK_INTERVAL {
            self.connset_ctrl.check_health().await?;
            self.caconn_last_channel_check = Instant::now();
        }
        Ok(())
    }

    async fn ca_conn_send_shutdown(&mut self) -> Result<(), Error> {
        self.connset_ctrl.shutdown().await?;
        Ok(())
    }

    async fn handle_timer_tick(&mut self) -> Result<(), Error> {
        if self.shutting_down {
            let sa1 = self.ingest_commons.insert_item_queue.sender_count();
            let sa2 = self.ingest_commons.insert_item_queue.sender_count_2();
            let nworkers = self
                .ingest_commons
                .insert_workers_running
                .load(atomic::Ordering::Acquire);
            let nitems = self.insert_rx_weak.upgrade().map(|x| x.len());
            info!(
                "qu senders A {:?} {:?}  nworkers {}  nitems {:?}",
                sa1, sa2, nworkers, nitems
            );
            if nworkers == 0 {
                info!("goodbye");
                std::process::exit(0);
            }
        }
        self.stats.handle_timer_tick_count_inc();
        let ts1 = Instant::now();
        let tsnow = SystemTime::now();
        if SIGINT.load(atomic::Ordering::Acquire) == 1 {
            warn!("Received SIGINT");
            SIGINT.store(2, atomic::Ordering::Release);
        }
        if SIGTERM.load(atomic::Ordering::Acquire) == 1 {
            warn!("Received SIGTERM");
            SIGTERM.store(2, atomic::Ordering::Release);
        }
        warn!("TODO let CaConnSet check health");
        // TODO
        // self.check_connection_states()?;
        // self.check_channel_states().await?;
        let dt = ts1.elapsed();
        if dt > Duration::from_millis(500) {
            info!("slow check_chans  {}ms", dt.as_secs_f32() * 1e3);
        }
        let ts1 = Instant::now();
        self.check_caconn_chans().await?;
        let dt = ts1.elapsed();
        if dt > Duration::from_millis(500) {
            info!("slow check_chans  {}ms", dt.as_secs_f32() * 1e3);
        }
        if tsnow.duration_since(self.last_status_print).unwrap_or(Duration::ZERO) >= PRINT_STATUS_INTERVAL {
            self.last_status_print = tsnow;
            info!(
                "{:8}  {:8} {:8} : {:8} : {:8} {:8} : {:10}",
                self.count_unknown_address,
                self.count_search_pending,
                self.count_search_sent,
                self.count_no_address,
                self.count_unassigned,
                self.count_assigned,
                self.insert_queue_counter.load(atomic::Ordering::Acquire),
            );
        }
        Ok(())
    }

    async fn handle_channel_add(&mut self, ch: Channel) -> Result<(), Error> {
        self.connset_ctrl
            .add_channel(
                self.opts.backend.clone(),
                ch.id().into(),
                self.opts.local_epics_hostname.clone(),
            )
            .await?;
        Ok(())
    }

    async fn handle_channel_remove(&mut self, ch: Channel) -> Result<(), Error> {
        warn!("TODO handle_channel_remove");
        #[cfg(DISABLED)]
        if let Some(k) = self.channel_states.get_mut(&ch) {
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
                    } => match state.inner {
                        WithStatusSeriesIdStateInner::UnknownAddress { .. } => {
                            k.value = ChannelStateValue::ToRemove { addr: None };
                        }
                        WithStatusSeriesIdStateInner::SearchPending { .. } => {
                            k.value = ChannelStateValue::ToRemove { addr: None };
                        }
                        WithStatusSeriesIdStateInner::WithAddress { addr, .. } => {
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

    async fn handle_search_done(&mut self, item: Result<VecDeque<FindIocRes>, Error>) -> Result<(), Error> {
        warn!("TODO handle_search_done");
        //debug!("handle SearchDone: {res:?}");
        // let allow_create_new_connections = self.allow_create_new_connections();
        // let tsnow = SystemTime::now();
        #[cfg(DISABLED)]
        match item {
            Ok(ress) => {
                SEARCH_ANS_COUNT.fetch_add(ress.len(), atomic::Ordering::AcqRel);
                for res in ress {
                    if let Some(addr) = &res.addr {
                        self.stats.ioc_search_some_inc();

                        let ch = Channel::new(res.channel);
                        if let Some(st) = self.channel_states.get_mut(&ch) {
                            match &st.value {
                                ChannelStateValue::Active(st2) => match st2 {
                                    ActiveChannelState::Init { .. } => {}
                                    ActiveChannelState::WaitForStatusSeriesId { .. } => {}
                                    ActiveChannelState::WithStatusSeriesId {
                                        status_series_id,
                                        state,
                                    } => match state.inner {
                                        WithStatusSeriesIdStateInner::SearchPending { since, did_send: _ } => {
                                            if allow_create_new_connections {
                                                let dt = tsnow.duration_since(since).unwrap();
                                                if dt > SEARCH_PENDING_TIMEOUT_WARN {
                                                    warn!(
                                                        "    FOUND {:5.0}  {:5.0}  {addr}",
                                                        1e3 * dt.as_secs_f32(),
                                                        1e3 * res.dt.as_secs_f32()
                                                    );
                                                }
                                                let stnew =
                                                    ChannelStateValue::Active(ActiveChannelState::WithStatusSeriesId {
                                                        status_series_id: status_series_id.clone(),
                                                        state: WithStatusSeriesIdState {
                                                            inner: WithStatusSeriesIdStateInner::WithAddress {
                                                                addr: addr.clone(),
                                                                state: WithAddressState::Unassigned {
                                                                    assign_at: tsnow,
                                                                },
                                                            },
                                                        },
                                                    });
                                                st.value = stnew;
                                            } else {
                                                // Emit something here?
                                            }
                                        }
                                        _ => {
                                            warn!(
                                                "address found, but state for {ch:?} is not SearchPending: {:?}",
                                                st.value
                                            );
                                        }
                                    },
                                },
                                ChannelStateValue::ToRemove { addr: _ } => {}
                            }
                        } else {
                            warn!("can not find channel state for {ch:?}");
                        }
                    } else {
                        //debug!("no addr from search in {res:?}");
                        let ch = Channel::new(res.channel);
                        if let Some(st) = self.channel_states.get_mut(&ch) {
                            let mut unexpected_state = true;
                            match &st.value {
                                ChannelStateValue::Active(st2) => match st2 {
                                    ActiveChannelState::Init { .. } => {}
                                    ActiveChannelState::WaitForStatusSeriesId { .. } => {}
                                    ActiveChannelState::WithStatusSeriesId {
                                        status_series_id,
                                        state: st3,
                                    } => match &st3.inner {
                                        WithStatusSeriesIdStateInner::UnknownAddress { .. } => {}
                                        WithStatusSeriesIdStateInner::SearchPending { since, .. } => {
                                            unexpected_state = false;
                                            let dt = tsnow.duration_since(*since).unwrap();
                                            if dt > SEARCH_PENDING_TIMEOUT_WARN {
                                                warn!(
                                                    "NOT FOUND {:5.0}  {:5.0}",
                                                    1e3 * dt.as_secs_f32(),
                                                    1e3 * res.dt.as_secs_f32()
                                                );
                                            }
                                            st.value =
                                                ChannelStateValue::Active(ActiveChannelState::WithStatusSeriesId {
                                                    status_series_id: status_series_id.clone(),
                                                    state: WithStatusSeriesIdState {
                                                        inner: WithStatusSeriesIdStateInner::NoAddress { since: tsnow },
                                                    },
                                                });
                                        }
                                        WithStatusSeriesIdStateInner::WithAddress { .. } => {}
                                        WithStatusSeriesIdStateInner::NoAddress { .. } => {}
                                    },
                                },
                                ChannelStateValue::ToRemove { .. } => {}
                            }
                            if unexpected_state {
                                warn!("no address, but state for {ch:?} is not SearchPending: {:?}", st.value);
                            }
                        } else {
                            warn!("can not find channel state for {ch:?}");
                        }
                    }
                }
            }
            Err(e) => {
                self.stats.ioc_search_err_inc();
                error!("error from search: {e}");
            }
        }
        Ok(())
    }

    async fn handle_ca_conn_done(&mut self, conn_addr: SocketAddrV4) -> Result<(), Error> {
        warn!("TODO handle_ca_conn_done {conn_addr:?}");
        Ok(())
    }

    #[cfg(DISABLED)]
    async fn handle_ca_conn_done(&mut self, conn_addr: SocketAddrV4) -> Result<(), Error> {
        info!("handle_ca_conn_done {conn_addr:?}");
        self.connection_states.remove(&conn_addr);
        for (_k, v) in self.channel_states.iter_mut() {
            match &v.value {
                ChannelStateValue::Active(st2) => match st2 {
                    ActiveChannelState::WithStatusSeriesId {
                        status_series_id: _,
                        state: st3,
                    } => match &st3.inner {
                        WithStatusSeriesIdStateInner::UnknownAddress { .. } => {}
                        WithStatusSeriesIdStateInner::SearchPending { .. } => {}
                        WithStatusSeriesIdStateInner::WithAddress { addr, .. } => {
                            if addr == &conn_addr {
                                self.stats.caconn_done_channel_state_reset_inc();
                                // TODO reset channel, emit log event for the connection addr only
                                //info!("ca conn down, reset {k:?}");
                                *v = ChannelState {
                                    value: ChannelStateValue::Active(ActiveChannelState::Init {
                                        since: SystemTime::now(),
                                    }),
                                };
                            } else {
                                // nothing to do
                            }
                        }
                        WithStatusSeriesIdStateInner::NoAddress { .. } => {}
                    },
                    ActiveChannelState::Init { .. } => {}
                    ActiveChannelState::WaitForStatusSeriesId { .. } => {}
                },
                ChannelStateValue::ToRemove { .. } => {}
            }
        }
        let item = QueryItem::ConnectionStatus(ConnectionStatusItem {
            ts: SystemTime::now(),
            addr: conn_addr,
            status: ConnectionStatus::ConnectionHandlerDone,
        });
        if let Some(tx) = self.ingest_commons.insert_item_queue.sender() {
            if let Err(_) = tokio::time::timeout(Duration::from_millis(1000), tx.send(item)).await {
                error!("timeout on insert queue send");
            } else {
            }
        } else {
            error!("can not emit CaConn done event");
        }
        Ok(())
    }

    async fn handle_ca_conn_event(&mut self, addr: SocketAddrV4, item: CaConnEvent) -> Result<(), Error> {
        self.stats.event_ca_conn_inc();
        use netfetch::ca::conn::CaConnEventValue::*;
        match item.value {
            None => {
                // TODO count, maybe reduce.
                Ok(())
            }
            EchoTimeout => {
                self.stats.ca_echo_timeout_total_inc();
                error!("TODO on EchoTimeout remove the CaConn and reset channels");
                Ok(())
            }
            ConnCommandResult(item) => {
                self.stats.todo_mark_inc();
                use netfetch::ca::conn::ConnCommandResultKind::*;
                match &item.kind {
                    CheckHealth => {
                        todo!("TODO collect the CaConn health check in CaConnSet");
                        #[cfg(DISABLED)]
                        if let Some(st) = self.connection_states.get_mut(&addr) {
                            self.stats.ca_conn_status_feedback_recv_inc();
                            st.last_feedback = Instant::now();
                            Ok(())
                        } else {
                            self.stats.ca_conn_status_feedback_no_dst_inc();
                            Ok(())
                        }
                    }
                }
            }
            QueryItem(item) => {
                self.query_item_tx.send(item).await?;
                Ok(())
            }
            EndOfStream => {
                self.stats.ca_conn_status_done_inc();
                self.handle_ca_conn_done(addr).await
            }
        }
    }

    async fn handle_shutdown(&mut self) -> Result<(), Error> {
        error!("TODO handle_shutdown");
        // TODO make sure we:
        // set a flag so that we don't attempt to use resources any longer (why could that happen?)
        // does anybody might still want to communicate with us? can't be excluded.
        // send shutdown signal to everyone.
        // drop our ends of channels to workers (gate them behind option?).
        // await the connection sets.
        // await other workers that we've spawned.
        self.connset_ctrl.shutdown().await?;
        Ok(())
    }

    #[cfg(DISABLED)]
    async fn handle_shutdown(&mut self) -> Result<(), Error> {
        warn!("received shutdown event");
        if self.shutting_down {
            info!("already shutting down");
            Ok(())
        } else {
            self.shutting_down = true;
            self.channel_states.clear();
            self.ca_conn_send_shutdown().await?;
            self.ingest_commons.insert_item_queue.drop_sender();
            Ok(())
        }
    }

    async fn handle_event(&mut self, item: DaemonEvent) -> Result<(), Error> {
        use DaemonEvent::*;
        self.stats.events_inc();
        let ts1 = Instant::now();
        let item_summary = item.summary();
        let ret = match item {
            TimerTick(i, tx) => {
                let ts1 = Instant::now();
                let ret = self.handle_timer_tick().await;
                match tx.send(i.wrapping_add(1)).await {
                    Ok(_) => {}
                    Err(_) => {
                        self.stats.ticker_token_release_error_inc();
                        error!("can not send ticker token");
                        return Err(Error::with_msg_no_trace("can not send ticker token"));
                    }
                }
                // TODO collect timer tick min/max/avg metrics.
                let _ = ts1.elapsed();
                ret
            }
            ChannelAdd(ch) => self.handle_channel_add(ch).await,
            ChannelRemove(ch) => self.handle_channel_remove(ch).await,
            SearchDone(item) => self.handle_search_done(item).await,
            CaConnEvent(addr, item) => self.handle_ca_conn_event(addr, item).await,
            Shutdown => self.handle_shutdown().await,
        };
        let dt = ts1.elapsed();
        if dt > Duration::from_millis(200) {
            warn!("handle_event  slow  {}ms  {}", dt.as_secs_f32() * 1e3, item_summary);
        }
        ret
    }

    fn spawn_ticker(tx: Sender<DaemonEvent>, stats: Arc<DaemonStats>) {
        let (ticker_inp_tx, ticker_inp_rx) = async_channel::bounded::<u32>(1);
        let ticker = {
            async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    if SIGINT.load(atomic::Ordering::Acquire) != 0 || SIGTERM.load(atomic::Ordering::Acquire) != 0 {
                        if SHUTDOWN_SENT.load(atomic::Ordering::Acquire) == 0 {
                            if let Err(e) = tx.send(DaemonEvent::Shutdown).await {
                                error!("can not send TimerTick {e}");
                                break;
                            } else {
                                SHUTDOWN_SENT.store(1, atomic::Ordering::Release);
                            }
                        }
                    }
                    if let Err(e) = tx.send(DaemonEvent::TimerTick(0, ticker_inp_tx.clone())).await {
                        error!("can not send TimerTick {e}");
                        break;
                    }
                    let c = ticker_inp_rx.len().max(1);
                    for _ in 0..c {
                        match ticker_inp_rx.recv().await {
                            Ok(_) => {}
                            Err(_) => {
                                stats.ticker_token_acquire_error_inc();
                                break;
                            }
                        }
                    }
                }
            }
        };
        // TODO use join handle
        taskrun::spawn(ticker);
    }

    pub async fn daemon(&mut self) -> Result<(), Error> {
        Self::spawn_ticker(self.tx.clone(), self.stats.clone());
        loop {
            match self.rx.recv().await {
                Ok(item) => match self.handle_event(item).await {
                    Ok(_) => {}
                    Err(e) => {
                        error!("daemon: {e}");
                        break;
                    }
                },
                Err(e) => {
                    error!("{e}");
                    break;
                }
            }
        }
        warn!("TODO wait for insert workers");
        let _ = &self.insert_workers_jh;
        info!("daemon done");
        Ok(())
    }
}

static SIGINT: AtomicUsize = AtomicUsize::new(0);
static SIGTERM: AtomicUsize = AtomicUsize::new(0);
static SHUTDOWN_SENT: AtomicUsize = AtomicUsize::new(0);

fn handler_sigint(_a: libc::c_int, _b: *const libc::siginfo_t, _c: *const libc::c_void) {
    SIGINT.store(1, atomic::Ordering::Release);
    let _ = ingest_linux::signal::unset_signal_handler(libc::SIGINT);
}

fn handler_sigterm(_a: libc::c_int, _b: *const libc::siginfo_t, _c: *const libc::c_void) {
    SIGTERM.store(1, atomic::Ordering::Release);
    let _ = ingest_linux::signal::unset_signal_handler(libc::SIGTERM);
}

pub async fn run(opts: CaIngestOpts, channels: Vec<String>) -> Result<(), Error> {
    info!("start up {opts:?}");
    ingest_linux::signal::set_signal_handler(libc::SIGINT, handler_sigint).map_err(Error::from_string)?;
    ingest_linux::signal::set_signal_handler(libc::SIGTERM, handler_sigterm).map_err(Error::from_string)?;

    let pg = dbpg::conn::make_pg_client(opts.postgresql_config())
        .await
        .map_err(Error::from_string)?;

    dbpg::schema::schema_check(&pg).await.map_err(Error::from_string)?;

    scywr::schema::migrate_scylla_data_schema(opts.scylla_config())
        .await
        .map_err(Error::from_string)?;

    info!("database check done");

    // TODO use a new stats type:
    //let store_stats = Arc::new(CaConnStats::new());
    //let metrics_agg_fut = metrics_agg_task(ingest_commons.clone(), local_stats.clone(), store_stats.clone());
    //let metrics_agg_jh = tokio::spawn(metrics_agg_fut);

    let mut channels = channels;
    if opts.test_bsread_addr.is_some() {
        channels.clear();
    }

    let opts2 = DaemonOpts {
        backend: opts.backend().into(),
        local_epics_hostname: opts.local_epics_hostname().into(),
        array_truncate: opts.array_truncate(),
        insert_item_queue_cap: opts.insert_item_queue_cap(),
        pgconf: opts.postgresql_config().clone(),
        scyconf: opts.scylla_config().clone(),
        ttls: Ttls {
            index: opts.ttl_index(),
            d0: opts.ttl_d0(),
            d1: opts.ttl_d1(),
            binned: opts.ttl_binned(),
        },
        test_bsread_addr: opts.test_bsread_addr.clone(),
        insert_worker_count: opts.insert_worker_count(),
        insert_scylla_sessions: opts.insert_scylla_sessions(),
    };
    let mut daemon = Daemon::new(opts2).await?;
    let tx = daemon.tx.clone();
    let daemon_stats = daemon.stats().clone();

    let dcom = Arc::new(netfetch::metrics::DaemonComm::new(tx.clone()));
    let metrics_jh = {
        let stats_set = StatsSet::new(daemon_stats);
        let fut = netfetch::metrics::start_metrics_service(opts.api_bind(), dcom, stats_set);
        tokio::task::spawn(fut)
    };

    let daemon_jh = taskrun::spawn(async move {
        // TODO handle Err
        daemon.daemon().await.unwrap();
    });
    for s in &channels {
        let ch = Channel::new(s.into());
        tx.send(DaemonEvent::ChannelAdd(ch)).await?;
    }
    info!("configured channels applied");
    daemon_jh.await.unwrap();
    if false {
        metrics_jh.await.unwrap();
    }
    Ok(())
}
