pub mod inserthook;

use async_channel::Receiver;
use async_channel::Sender;
use async_channel::WeakSender;
use dbpg::seriesbychannel::ChannelInfoQuery;
use err::Error;
use log::*;
use netfetch::ca::connset::CaConnSet;
use netfetch::ca::connset::CaConnSetCtrl;
use netfetch::ca::connset::CaConnSetItem;
use netfetch::conf::CaIngestOpts;
use netfetch::conf::ChannelConfig;
use netfetch::conf::ChannelsConfig;
use netfetch::daemon_common::Channel;
use netfetch::daemon_common::DaemonEvent;
use netfetch::metrics::StatsSet;
use netfetch::throttletrace::ThrottleTrace;
use netpod::ttl::RetentionTime;
use netpod::Database;
use scywr::config::ScyllaIngestConfig;
use scywr::insertworker::InsertWorkerOpts;
use scywr::iteminsertqueue as scywriiq;
use scywriiq::QueryItem;
use stats::DaemonStats;
use stats::InsertWorkerStats;
use stats::SeriesByChannelStats;
use stats::SeriesWriterEstablishStats;
use std::collections::VecDeque;
use std::sync::atomic;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use taskrun::tokio;
use tokio::task::JoinHandle;

const CHECK_HEALTH_IVL: Duration = Duration::from_millis(2000);
const CHECK_HEALTH_TIMEOUT: Duration = Duration::from_millis(5000);
const PRINT_ACTIVE_INTERVAL: Duration = Duration::from_millis(60000);
const PRINT_STATUS_INTERVAL: Duration = Duration::from_millis(20000);
const CHECK_CHANNEL_SLOW_WARN: Duration = Duration::from_millis(500);
const RUN_WITHOUT_SCYLLA: bool = true;

pub struct DaemonOpts {
    pgconf: Database,
    scyconf: ScyllaIngestConfig,
    #[allow(unused)]
    test_bsread_addr: Option<String>,
    insert_frac: Arc<AtomicU64>,
    store_workers_rate: Arc<AtomicU64>,
}

pub struct Daemon {
    opts: DaemonOpts,
    ingest_opts: CaIngestOpts,
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
    insert_worker_stats: Arc<InsertWorkerStats>,
    series_by_channel_stats: Arc<SeriesByChannelStats>,
    shutting_down: bool,
    connset_ctrl: CaConnSetCtrl,
    connset_status_last: Instant,
    // TODO should be a stats object?
    insert_workers_running: AtomicU64,
    query_item_tx_weak: WeakSender<VecDeque<QueryItem>>,
    connset_health_lat_ema: f32,
    metrics_shutdown_tx: Sender<u32>,
    metrics_shutdown_rx: Receiver<u32>,
    metrics_jh: Option<JoinHandle<Result<(), Error>>>,
    channel_info_query_tx: Sender<ChannelInfoQuery>,
}

impl Daemon {
    pub async fn new(opts: DaemonOpts, ingest_opts: CaIngestOpts) -> Result<Self, Error> {
        let (daemon_ev_tx, daemon_ev_rx) = async_channel::bounded(32);

        let series_by_channel_stats = Arc::new(SeriesByChannelStats::new());
        let insert_worker_stats = Arc::new(InsertWorkerStats::new());

        // TODO keep join handles and await later
        let (channel_info_query_tx, jhs, jh) = dbpg::seriesbychannel::start_lookup_workers::<
            dbpg::seriesbychannel::SalterRandom,
        >(4, &opts.pgconf, series_by_channel_stats.clone())
        .await
        .map_err(|e| Error::with_msg_no_trace(e.to_string()))?;

        let (query_item_tx, query_item_rx) = async_channel::bounded(ingest_opts.insert_item_queue_cap());
        let query_item_tx_weak = query_item_tx.downgrade();

        let insert_queue_counter = Arc::new(AtomicUsize::new(0));

        // Insert queue hook
        // let query_item_rx = inserthook::active_channel_insert_hook(query_item_rx);

        let wrest_stats = Arc::new(SeriesWriterEstablishStats::new());
        let (writer_establis_tx,) =
            serieswriter::writer::start_writer_establish_worker(channel_info_query_tx.clone(), wrest_stats.clone())
                .map_err(|e| Error::with_msg_no_trace(e.to_string()))?;

        let local_epics_hostname = ingest_linux::net::local_hostname();
        let conn_set_ctrl = CaConnSet::start(
            ingest_opts.backend().into(),
            local_epics_hostname,
            query_item_tx,
            channel_info_query_tx.clone(),
            ingest_opts.clone(),
            writer_establis_tx,
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

        #[cfg(DISABLED)]
        let query_item_rx = {
            // TODO only testing, remove
            tokio::spawn({
                let rx = query_item_rx;
                async move {
                    while let Ok(item) = rx.recv().await {
                        drop(item);
                    }
                }
            });
            let (tx, rx) = async_channel::bounded(128);
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(2000)).await;
                    tx.len();
                }
            });
            rx
        };

        let insert_worker_opts = InsertWorkerOpts {
            store_workers_rate: opts.store_workers_rate.clone(),
            insert_workers_running: Arc::new(AtomicU64::new(0)),
            insert_frac: opts.insert_frac.clone(),
            array_truncate: Arc::new(AtomicU64::new(ingest_opts.array_truncate())),
        };
        let insert_worker_opts = Arc::new(insert_worker_opts);

        debug!("TODO RetentionTime");

        let rett = RetentionTime::Short;

        #[cfg(DISABLED)]
        let insert_workers_jh = scywr::insertworker::spawn_scylla_insert_workers(
            rett,
            opts.scyconf.clone(),
            ingest_opts.insert_scylla_sessions(),
            ingest_opts.insert_worker_count(),
            ingest_opts.insert_worker_concurrency(),
            query_item_rx,
            insert_worker_opts,
            insert_worker_stats.clone(),
            ingest_opts.use_rate_limit_queue(),
        )
        .await?;
        let insert_workers_jh = scywr::insertworker::spawn_scylla_insert_workers_dummy(
            ingest_opts.insert_worker_count(),
            ingest_opts.insert_worker_concurrency(),
            query_item_rx,
            insert_worker_opts,
            insert_worker_stats.clone(),
        )
        .await?;
        let stats = Arc::new(DaemonStats::new());
        stats.insert_worker_spawned().add(insert_workers_jh.len() as _);

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

        let (metrics_shutdown_tx, metrics_shutdown_rx) = async_channel::bounded(8);

        let ret = Self {
            opts,
            ingest_opts,
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
            stats,
            insert_worker_stats,
            series_by_channel_stats,
            shutting_down: false,
            connset_ctrl: conn_set_ctrl,
            connset_status_last: Instant::now(),
            insert_workers_running: AtomicU64::new(0),
            query_item_tx_weak,
            connset_health_lat_ema: 0.,
            metrics_shutdown_tx,
            metrics_shutdown_rx,
            metrics_jh: None,
            channel_info_query_tx,
        };
        Ok(ret)
    }

    fn stats(&self) -> &Arc<DaemonStats> {
        &self.stats
    }

    async fn check_health(&mut self, ts1: Instant) -> Result<(), Error> {
        self.check_health_connset(ts1)?;
        Ok(())
    }

    fn check_health_connset(&mut self, ts1: Instant) -> Result<(), Error> {
        let dt = self.connset_status_last.elapsed();
        if dt > CHECK_HEALTH_TIMEOUT {
            error!(
                "CaConnSet has not reported health status  since {:.0}",
                dt.as_secs_f32() * 1e3
            );
        }
        Ok(())
    }

    async fn handle_timer_tick(&mut self) -> Result<(), Error> {
        if self.shutting_down {
            let nworkers = self.insert_workers_running.load(atomic::Ordering::Acquire);
            let nitems = self
                .query_item_tx_weak
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
        {
            let n = SIGINT.load(atomic::Ordering::Acquire);
            let m = SIGINT_CONFIRM.load(atomic::Ordering::Acquire);
            if m != n {
                warn!("Received SIGINT");
                SIGINT_CONFIRM.store(n, atomic::Ordering::Release);
            }
        }
        if SIGTERM.load(atomic::Ordering::Acquire) == 1 {
            warn!("Received SIGTERM");
            SIGTERM.store(2, atomic::Ordering::Release);
        }
        let ts1 = Instant::now();
        self.check_health(ts1).await?;
        let dt = ts1.elapsed();
        if dt > CHECK_CHANNEL_SLOW_WARN {
            info!("slow check_chans  {:.0} ms", dt.as_secs_f32() * 1e3);
        }
        if false && tsnow.duration_since(self.last_status_print).unwrap_or(Duration::ZERO) >= PRINT_STATUS_INTERVAL {
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

    async fn handle_channel_add(
        &mut self,
        ch_cfg: ChannelConfig,
        restx: netfetch::ca::conn::CmdResTx,
    ) -> Result<(), Error> {
        // debug!("handle_channel_add {ch:?}");
        self.connset_ctrl
            .add_channel(self.ingest_opts.backend().into(), ch_cfg, restx)
            .await?;
        Ok(())
    }

    async fn handle_channel_remove(&mut self, ch: Channel) -> Result<(), Error> {
        self.connset_ctrl.remove_channel(ch.name().into()).await?;
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
            Healthy => {
                let tsnow = Instant::now();
                self.connset_status_last = tsnow;
                self.stats.caconnset_health_response().inc();
            }
            Error(e) => {
                error!("error from CaConnSet: {e}");
                self.handle_shutdown().await?;
            }
        }
        Ok(())
    }

    async fn handle_shutdown(&mut self) -> Result<(), Error> {
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
            self.rx.close();
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
            ChannelAdd(ch, tx) => self.handle_channel_add(ch, tx).await,
            ChannelRemove(ch) => self.handle_channel_remove(ch).await,
            CaConnSetItem(item) => self.handle_ca_conn_set_item(item).await,
            Shutdown => self.handle_shutdown().await,
        };
        let dt = ts1.elapsed();
        if dt > Duration::from_millis(200) {
            warn!("handle_event  slow  {} ms  {}", dt.as_secs_f32() * 1e3, item_summary);
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

    pub async fn spawn_metrics(&mut self) -> Result<(), Error> {
        let tx = self.tx.clone();
        let daemon_stats = self.stats().clone();
        let connset_cmd_tx = self.connset_ctrl.sender().clone();
        let ca_conn_stats = self.connset_ctrl.ca_conn_stats().clone();
        let dcom = Arc::new(netfetch::metrics::DaemonComm::new(tx.clone()));
        let metrics_jh = {
            let conn_set_stats = self.connset_ctrl.stats().clone();
            let stats_set = StatsSet::new(
                daemon_stats,
                conn_set_stats,
                ca_conn_stats,
                self.connset_ctrl.ca_proto_stats().clone(),
                self.insert_worker_stats.clone(),
                self.series_by_channel_stats.clone(),
                self.connset_ctrl.ioc_finder_stats().clone(),
                self.opts.insert_frac.clone(),
            );
            let fut = netfetch::metrics::metrics_service(
                self.ingest_opts.api_bind(),
                dcom,
                connset_cmd_tx,
                stats_set,
                self.metrics_shutdown_rx.clone(),
            );
            tokio::task::spawn(fut)
        };
        self.metrics_jh = Some(metrics_jh);
        Ok(())
    }

    pub async fn daemon(mut self) -> Result<(), Error> {
        {
            let backend = String::new();
            let (item_tx, item_rx) = async_channel::bounded(256);
            let info_worker_tx = self.channel_info_query_tx.clone();
            let iiq_tx = self.query_item_tx_weak.upgrade().unwrap();
            let worker_fut =
                netfetch::metrics::postingest::process_api_query_items(backend, item_rx, info_worker_tx, iiq_tx);
            let worker_jh = taskrun::spawn(worker_fut);
        }
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
        while let Some(jh) = self.insert_workers_jh.pop() {
            match jh.await.map_err(Error::from_string) {
                Ok(x) => match x {
                    Ok(()) => {
                        self.stats.insert_worker_join_ok().inc();
                        // debug!("joined insert worker");
                    }
                    Err(e) => {
                        self.stats.insert_worker_join_ok_err().inc();
                        error!("joined insert worker, error  {e}");
                    }
                },
                Err(e) => {
                    self.stats.insert_worker_join_err().inc();
                    error!("insert worker join error {e}");
                }
            }
        }
        info!("Wait for metrics handler");
        self.metrics_shutdown_tx.send(1).await?;
        if let Some(jh) = self.metrics_jh.take() {
            jh.await??;
        }
        info!("Joined metrics handler");
        Ok(())
    }
}

static SIGINT: AtomicUsize = AtomicUsize::new(0);
static SIGINT_CONFIRM: AtomicUsize = AtomicUsize::new(0);
static SIGTERM: AtomicUsize = AtomicUsize::new(0);
static SHUTDOWN_SENT: AtomicUsize = AtomicUsize::new(0);

fn handler_sigint(_a: libc::c_int, _b: *const libc::siginfo_t, _c: *const libc::c_void) {
    let n = SIGINT.fetch_add(1, atomic::Ordering::AcqRel);
    if n >= 2 {
        let _ = ingest_linux::signal::unset_signal_handler(libc::SIGINT);
        std::process::exit(13);
    }
}

fn handler_sigterm(_a: libc::c_int, _b: *const libc::siginfo_t, _c: *const libc::c_void) {
    SIGTERM.store(1, atomic::Ordering::Release);
    let _ = ingest_linux::signal::unset_signal_handler(libc::SIGTERM);
}

pub async fn run(opts: CaIngestOpts, channels_config: Option<ChannelsConfig>) -> Result<(), Error> {
    info!("start up {opts:?}");
    ingest_linux::signal::set_signal_handler(libc::SIGINT, handler_sigint).map_err(Error::from_string)?;
    ingest_linux::signal::set_signal_handler(libc::SIGTERM, handler_sigterm).map_err(Error::from_string)?;

    let (pg, jh) = dbpg::conn::make_pg_client(opts.postgresql_config())
        .await
        .map_err(Error::from_string)?;
    dbpg::schema::schema_check(&pg).await.map_err(Error::from_string)?;
    drop(pg);
    jh.await?.map_err(Error::from_string)?;

    if RUN_WITHOUT_SCYLLA {
    } else {
        scywr::schema::migrate_scylla_data_schema(opts.scylla_config(), RetentionTime::Short)
            .await
            .map_err(Error::from_string)?;

        if let Some(scyconf) = opts.scylla_config_lt() {
            scywr::schema::migrate_scylla_data_schema(scyconf, RetentionTime::Long)
                .await
                .map_err(Error::from_string)?;
        }
    }
    info!("database check done");

    // TODO use a new stats type:
    //let store_stats = Arc::new(CaConnStats::new());
    //let metrics_agg_fut = metrics_agg_task(ingest_commons.clone(), local_stats.clone(), store_stats.clone());
    //let metrics_agg_jh = tokio::spawn(metrics_agg_fut);

    let channels_config = if opts.test_bsread_addr.is_some() {
        None
    } else {
        channels_config
    };

    let insert_frac = Arc::new(AtomicU64::new(opts.insert_frac()));
    let store_workers_rate = Arc::new(AtomicU64::new(opts.store_workers_rate()));

    let opts2 = DaemonOpts {
        pgconf: opts.postgresql_config().clone(),
        scyconf: opts.scylla_config().clone(),
        test_bsread_addr: opts.test_bsread_addr.clone(),
        insert_frac: insert_frac.clone(),
        store_workers_rate,
    };
    let daemon = Daemon::new(opts2, opts.clone()).await?;
    let daemon_tx = daemon.tx.clone();
    let daemon_jh = taskrun::spawn(daemon.daemon());
    if let Some(channels_config) = channels_config {
        debug!("will configure {} channels", channels_config.len());
        let mut thr_msg = ThrottleTrace::new(Duration::from_millis(1000));
        let mut i = 0;
        for ch_cfg in channels_config.channels() {
            match daemon_tx
                .send(DaemonEvent::ChannelAdd(ch_cfg.clone(), async_channel::bounded(1).0))
                .await
            {
                Ok(()) => {}
                Err(e) => {
                    error!("{e}");
                    break;
                }
            }
            thr_msg.trigger("daemon sent ChannelAdd", &[&i as &_]);
            i += 1;
        }
        debug!("{} configured channels applied", channels_config.len());
    }
    daemon_jh.await.map_err(|e| Error::with_msg_no_trace(e.to_string()))??;
    info!("Joined daemon");
    Ok(())
}
