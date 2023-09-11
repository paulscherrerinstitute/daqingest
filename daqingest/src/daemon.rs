pub mod finder;
pub mod inserthook;

use async_channel::Receiver;
use async_channel::Sender;
use async_channel::WeakReceiver;
use async_channel::WeakSender;
use err::Error;
use log::*;
use netfetch::ca::connset::CaConnSet;
use netfetch::ca::connset::CaConnSetCtrl;
use netfetch::ca::connset::CaConnSetItem;
use netfetch::conf::CaIngestOpts;
use netfetch::daemon_common::Channel;
use netfetch::daemon_common::DaemonEvent;
use netfetch::metrics::ExtraInsertsConf;
use netfetch::metrics::StatsSet;
use netpod::Database;
use netpod::ScyllaConfig;
use scywr::insertworker::InsertWorkerOpts;
use scywr::insertworker::Ttls;
use scywr::iteminsertqueue as scywriiq;
use scywr::store::DataStore;
use scywriiq::QueryItem;
use serde::Serialize;
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

const CA_CONN_INSERT_QUEUE_MAX: usize = 256;

const CHANNEL_CHECK_INTERVAL: Duration = Duration::from_millis(5000);
const PRINT_ACTIVE_INTERVAL: Duration = Duration::from_millis(60000);
const PRINT_STATUS_INTERVAL: Duration = Duration::from_millis(20000);

#[derive(Debug)]
enum CheckPeriodic {
    Waiting(Instant),
    Ongoing(Instant),
}

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
    insert_workers_jh: Vec<JoinHandle<Result<(), Error>>>,
    stats: Arc<DaemonStats>,
    shutting_down: bool,
    insert_rx_weak: WeakReceiver<QueryItem>,
    connset_ctrl: CaConnSetCtrl,
    connset_status_last: CheckPeriodic,
    // TODO should be a stats object?
    insert_workers_running: AtomicU64,
    query_item_tx_weak: WeakSender<QueryItem>,
}

impl Daemon {
    pub async fn new(opts: DaemonOpts) -> Result<Self, Error> {
        let (daemon_ev_tx, daemon_ev_rx) = async_channel::bounded(32);

        // TODO keep join handles and await later
        let (channel_info_query_tx, jhs, jh) = dbpg::seriesbychannel::start_lookup_workers(4, &opts.pgconf)
            .await
            .map_err(|e| Error::with_msg_no_trace(e.to_string()))?;

        let (query_item_tx, query_item_rx) = async_channel::bounded(opts.insert_item_queue_cap);
        let query_item_tx_weak = query_item_tx.downgrade();

        let insert_queue_counter = Arc::new(AtomicUsize::new(0));

        // Insert queue hook
        let query_item_rx = inserthook::active_channel_insert_hook(query_item_rx);

        let conn_set_ctrl = CaConnSet::start(
            opts.backend.clone(),
            opts.local_epics_hostname.clone(),
            query_item_tx,
            channel_info_query_tx,
            opts.pgconf.clone(),
        );

        // TODO remove
        tokio::spawn({
            let rx = conn_set_ctrl.receiver().clone();
            let tx = daemon_ev_tx.clone();
            async move {
                loop {
                    match rx.recv().await {
                        Ok(item) => {
                            let item = DaemonEvent::CaConnSetItem(item);
                            if let Err(_) = tx.send(item).await {
                                debug!("CaConnSet to Daemon adapter: tx closed, break");
                                break;
                            }
                        }
                        Err(_) => {
                            debug!("CaConnSet to Daemon adapter: rx done, break");
                            break;
                        }
                    }
                }
            }
        });

        let use_rate_limit_queue = false;

        // TODO use a new stats type:
        let store_stats = Arc::new(stats::CaConnStats::new());
        let ttls = opts.ttls.clone();
        let insert_worker_opts = InsertWorkerOpts {
            store_workers_rate: Arc::new(AtomicU64::new(20000000)),
            insert_workers_running: Arc::new(AtomicU64::new(0)),
            insert_frac: Arc::new(AtomicU64::new(1000)),
        };
        let insert_worker_opts = Arc::new(insert_worker_opts);
        let insert_workers_jh = scywr::insertworker::spawn_scylla_insert_workers(
            opts.scyconf.clone(),
            opts.insert_scylla_sessions,
            opts.insert_worker_count,
            query_item_rx.clone(),
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
            insert_workers_jh,
            stats: Arc::new(DaemonStats::new()),
            shutting_down: false,
            insert_rx_weak: query_item_rx.downgrade(),
            connset_ctrl: conn_set_ctrl,
            connset_status_last: CheckPeriodic::Waiting(Instant::now()),
            insert_workers_running: AtomicU64::new(0),
            query_item_tx_weak,
        };
        Ok(ret)
    }

    fn stats(&self) -> &Arc<DaemonStats> {
        &self.stats
    }

    async fn check_caconn_chans(&mut self, ts1: Instant) -> Result<(), Error> {
        match &self.connset_status_last {
            CheckPeriodic::Waiting(since) => {
                if *since + Duration::from_millis(2000) < ts1 {
                    debug!("========================================   issue health check CaConn");
                    self.connset_ctrl.check_health().await?;
                    self.connset_status_last = CheckPeriodic::Ongoing(ts1);
                    if let Some(tx) = self.query_item_tx_weak.upgrade() {
                        info!("query_item_tx  len {}", tx.len());
                    }
                }
            }
            CheckPeriodic::Ongoing(since) => {
                let dt = ts1.saturating_duration_since(*since);
                if dt > Duration::from_millis(4000) {
                    error!("========================================   CaConnSet has not reported health status  since {:.0}", dt.as_secs_f32() * 1e3);
                }
            }
        }
        Ok(())
    }

    async fn handle_timer_tick(&mut self) -> Result<(), Error> {
        if self.shutting_down {
            let nworkers = self.insert_workers_running.load(atomic::Ordering::Acquire);
            let nitems = self
                .insert_rx_weak
                .upgrade()
                .map(|x| (x.sender_count(), x.receiver_count(), x.len()));
            info!("qu senders A  nworkers {}  nitems {:?}", nworkers, nitems);
            if nworkers == 0 {
                info!("goodbye");
                std::process::exit(0);
            }
        }
        self.stats.handle_timer_tick_count.inc();
        let tsnow = SystemTime::now();
        if SIGINT.load(atomic::Ordering::Acquire) == 1 {
            warn!("Received SIGINT");
            SIGINT.store(2, atomic::Ordering::Release);
        }
        if SIGTERM.load(atomic::Ordering::Acquire) == 1 {
            warn!("Received SIGTERM");
            SIGTERM.store(2, atomic::Ordering::Release);
        }
        let ts1 = Instant::now();
        self.check_caconn_chans(ts1).await?;
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
        debug!("handle_channel_add {ch:?}");
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
        self.connset_ctrl.remove_channel(ch.id().into()).await?;
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

    async fn handle_ca_conn_set_item(&mut self, item: CaConnSetItem) -> Result<(), Error> {
        use CaConnSetItem::*;
        match item {
            Healthy(ts1, ts2) => {
                let ts3 = Instant::now();
                let dt1 = ts2.duration_since(ts1).as_secs_f32() * 1e3;
                let dt2 = ts3.duration_since(ts2).as_secs_f32() * 1e3;
                match &self.connset_status_last {
                    CheckPeriodic::Waiting(_since) => {
                        error!("========================================   received CaConnSet health report without having asked  {dt1:.0} ms  {dt2:.0} ms");
                    }
                    CheckPeriodic::Ongoing(since) => {
                        let dtsince = ts3.duration_since(*since).as_secs_f32() * 1e3;
                        debug!("========================================   received CaConnSet healthy  dtsince {dtsince:.0} ms  {dt1:.0} ms  {dt2:.0} ms");
                        self.connset_status_last = CheckPeriodic::Waiting(ts3);
                    }
                }
            }
        }
        Ok(())
    }

    async fn handle_shutdown(&mut self) -> Result<(), Error> {
        error!("TODO handle_shutdown");
        if self.shutting_down {
            warn!("already shutting down");
        } else {
            self.shutting_down = true;
            // TODO make sure we:
            // set a flag so that we don't attempt to use resources any longer (why could that happen?)
            // does anybody might still want to communicate with us? can't be excluded.
            // send shutdown signal to everyone.
            // drop our ends of channels to workers (gate them behind option?).
            // await the connection sets.
            // await other workers that we've spawned.
            self.connset_ctrl.shutdown().await?;
        }
        Ok(())
    }

    #[cfg(DISABLED)]
    async fn handle_shutdown(&mut self) -> Result<(), Error> {
        warn!("received shutdown event");
        if self.shutting_down {
            Ok(())
        } else {
            self.channel_states.clear();
            self.ca_conn_send_shutdown().await?;
            self.ingest_commons.insert_item_queue.drop_sender();
            Ok(())
        }
    }

    async fn handle_event(&mut self, item: DaemonEvent) -> Result<(), Error> {
        use DaemonEvent::*;
        self.stats.events.inc();
        let ts1 = Instant::now();
        let item_summary = item.summary();
        let ret = match item {
            TimerTick(i, tx) => {
                let ts1 = Instant::now();
                let ret = self.handle_timer_tick().await;
                match tx.send(i.wrapping_add(1)).await {
                    Ok(()) => {}
                    Err(_) => {
                        self.stats.ticker_token_release_error.inc();
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
            CaConnSetItem(item) => self.handle_ca_conn_set_item(item).await,
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
                                stats.ticker_token_acquire_error.inc();
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

    pub async fn daemon(mut self) -> Result<(), Error> {
        Self::spawn_ticker(self.tx.clone(), self.stats.clone());
        loop {
            if self.shutting_down {
                break;
            }
            match self.rx.recv().await {
                Ok(item) => match self.handle_event(item.clone()).await {
                    Ok(()) => {}
                    Err(e) => {
                        error!("fn daemon:  error from handle_event {item:?}  {e}");
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
        while let Some(jh) = self.insert_workers_jh.pop() {
            match jh.await.map_err(Error::from_string) {
                Ok(x) => match x {
                    Ok(()) => {
                        debug!("joined insert worker");
                    }
                    Err(e) => {
                        error!("joined insert worker, error  {e}");
                    }
                },
                Err(e) => {
                    error!("insert worker join error {e}");
                }
            }
        }
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
    let daemon = Daemon::new(opts2).await?;
    let tx = daemon.tx.clone();
    let daemon_stats = daemon.stats().clone();

    let dcom = Arc::new(netfetch::metrics::DaemonComm::new(tx.clone()));
    let metrics_jh = {
        let stats_set = StatsSet::new(daemon_stats);
        let fut = netfetch::metrics::start_metrics_service(opts.api_bind(), dcom, stats_set);
        tokio::task::spawn(fut)
    };

    let daemon_jh = taskrun::spawn(daemon.daemon());

    debug!("will configure {} channels", channels.len());
    let mut i = 0;
    for s in &channels {
        let ch = Channel::new(s.into());
        tx.send(DaemonEvent::ChannelAdd(ch)).await?;
        i += 1;
        if i % 1000 == 0 {
            debug!("sent {} ChannelAdd", i);
        }
    }
    debug!("{} configured channels applied", channels.len());
    daemon_jh.await.map_err(|e| Error::with_msg_no_trace(e.to_string()))??;
    if false {
        metrics_jh.await.unwrap();
    }
    Ok(())
}
