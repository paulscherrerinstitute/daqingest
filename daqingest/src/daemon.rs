use async_channel::Receiver;
use async_channel::Sender;
use err::Error;
use futures_util::FutureExt;
use futures_util::StreamExt;
use log::*;
use netfetch::ca::conn::CaConnEvent;
use netfetch::ca::conn::ConnCommand;
use netfetch::ca::connset::CaConnSet;
use netfetch::ca::findioc::FindIocRes;
use netfetch::ca::findioc::FindIocStream;
use netfetch::ca::store::DataStore;
use netfetch::ca::IngestCommons;
use netfetch::ca::SlowWarnable;
use netfetch::conf::CaIngestOpts;
use netfetch::daemon_common::Channel;
use netfetch::daemon_common::DaemonEvent;
use netfetch::errconv::ErrConv;
use netfetch::insertworker::Ttls;
use netfetch::metrics::ExtraInsertsConf;
use netfetch::metrics::StatsSet;
use netfetch::store::CommonInsertItemQueue;
use netpod::Database;
use netpod::ScyllaConfig;
use serde::Serialize;
use stats::DaemonStats;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::net::SocketAddrV4;
use std::sync::atomic;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use tokio_postgres::Client as PgClient;
use tokio_postgres::Row as PgRow;
use tracing::info_span;
use tracing::Instrument;

const SEARCH_BATCH_MAX: usize = 256;
const CURRENT_SEARCH_PENDING_MAX: usize = SEARCH_BATCH_MAX * 4;
const SEARCH_DB_PIPELINE_LEN: usize = 4;
const FINDER_JOB_QUEUE_LEN_MAX: usize = 10;
const FINDER_IN_FLIGHT_MAX: usize = 800;
const FINDER_BATCH_SIZE: usize = 8;
const CHECK_CHANS_PER_TICK: usize = 10000;
const CA_CONN_INSERT_QUEUE_MAX: usize = 256;

const UNKNOWN_ADDRESS_STAY: Duration = Duration::from_millis(2000);
const NO_ADDRESS_STAY: Duration = Duration::from_millis(20000);
const SEARCH_PENDING_TIMEOUT: Duration = Duration::from_millis(30000);
const SEARCH_PENDING_TIMEOUT_WARN: Duration = Duration::from_millis(8000);
const FINDER_TIMEOUT: Duration = Duration::from_millis(100);
const CHANNEL_CHECK_INTERVAL: Duration = Duration::from_millis(5000);
const PRINT_ACTIVE_INTERVAL: Duration = Duration::from_millis(8000);

const DO_ASSIGN_TO_CA_CONN: bool = true;

static SEARCH_REQ_MARK_COUNT: AtomicUsize = AtomicUsize::new(0);
static SEARCH_REQ_SEND_COUNT: AtomicUsize = AtomicUsize::new(0);
static SEARCH_REQ_RECV_COUNT: AtomicUsize = AtomicUsize::new(0);
static SEARCH_REQ_BATCH_SEND_COUNT: AtomicUsize = AtomicUsize::new(0);
static SEARCH_REQ_BATCH_RECV_COUNT: AtomicUsize = AtomicUsize::new(0);
static SEARCH_RES_0_COUNT: AtomicUsize = AtomicUsize::new(0);
static SEARCH_RES_1_COUNT: AtomicUsize = AtomicUsize::new(0);
static SEARCH_RES_2_COUNT: AtomicUsize = AtomicUsize::new(0);
static SEARCH_RES_3_COUNT: AtomicUsize = AtomicUsize::new(0);
static SEARCH_ANS_COUNT: AtomicUsize = AtomicUsize::new(0);

#[allow(unused)]
macro_rules! debug_batch {
    (D$($arg:tt)*) => ();
    ($($arg:tt)*) => (if false {
        info!($($arg)*);
    });
}

#[allow(unused)]
macro_rules! trace_batch {
    (D$($arg:tt)*) => ();
    ($($arg:tt)*) => (if false {
        trace!($($arg)*);
    });
}

#[allow(non_snake_case)]
mod serde_Instant {
    use serde::Serializer;
    use std::time::Instant;

    #[allow(unused)]
    pub fn serialize<S>(val: &Instant, ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let dur = val.elapsed();
        ser.serialize_u64(dur.as_secs() * 1000 + dur.subsec_millis() as u64)
    }
}

#[derive(Clone, Debug, Serialize)]
pub enum ConnectionStateValue {
    Unconnected,
    Connected {
        //#[serde(with = "serde_Instant")]
        since: SystemTime,
    },
}

#[derive(Clone, Debug, Serialize)]
pub struct ConnectionState {
    //#[serde(with = "serde_Instant")]
    updated: SystemTime,
    value: ConnectionStateValue,
}

#[derive(Clone, Debug, Serialize)]
pub enum WithAddressState {
    Unassigned {
        //#[serde(with = "serde_Instant")]
        assign_at: SystemTime,
    },
    Assigned(ConnectionState),
}

#[derive(Clone, Debug, Serialize)]
pub enum ActiveChannelState {
    UnknownAddress {
        since: SystemTime,
    },
    SearchPending {
        //#[serde(with = "serde_Instant")]
        since: SystemTime,
        did_send: bool,
    },
    WithAddress {
        addr: SocketAddrV4,
        state: WithAddressState,
    },
    NoAddress {
        since: SystemTime,
    },
}

#[derive(Debug)]
pub enum ChannelStateValue {
    Active(ActiveChannelState),
    ToRemove { addr: Option<SocketAddrV4> },
}

#[derive(Debug)]
pub struct ChannelState {
    value: ChannelStateValue,
}

#[derive(Debug)]
pub enum CaConnStateValue {
    Fresh,
    HadFeedback,
    Shutdown { since: Instant },
}

#[derive(Debug)]
pub struct CaConnState {
    last_feedback: Instant,
    value: CaConnStateValue,
}

pub struct DaemonOpts {
    backend: String,
    local_epics_hostname: String,
    array_truncate: usize,
    insert_item_queue_cap: usize,
    pgconf: Database,
    scyconf: ScyllaConfig,
    ttls: Ttls,
}

impl DaemonOpts {
    pub fn backend(&self) -> &str {
        &self.backend
    }
}

struct OptFut<F> {
    fut: Option<F>,
}

impl<F> OptFut<F> {
    fn empty() -> Self {
        Self { fut: None }
    }

    fn new(fut: F) -> Self {
        Self { fut: Some(fut) }
    }

    fn is_enabled(&self) -> bool {
        self.fut.is_some()
    }
}

impl<F> futures_util::Future for OptFut<F>
where
    F: futures_util::Future + std::marker::Unpin,
{
    type Output = <F as futures_util::Future>::Output;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context) -> std::task::Poll<Self::Output> {
        match self.fut.as_mut() {
            Some(fut) => fut.poll_unpin(cx),
            None => std::task::Poll::Pending,
        }
    }
}

pub async fn make_pg_client(d: &Database) -> Result<PgClient, Error> {
    let (client, pg_conn) = tokio_postgres::connect(
        &format!("postgresql://{}:{}@{}:{}/{}", d.user, d.pass, d.host, d.port, d.name),
        tokio_postgres::tls::NoTls,
    )
    .await
    .err_conv()?;
    // TODO allow clean shutdown on ctrl-c and join the pg_conn in the end:
    tokio::spawn(pg_conn);
    Ok(client)
}

pub struct Daemon {
    opts: DaemonOpts,
    connection_states: BTreeMap<SocketAddrV4, CaConnState>,
    channel_states: BTreeMap<Channel, ChannelState>,
    tx: Sender<DaemonEvent>,
    rx: Receiver<DaemonEvent>,
    chan_check_next: Option<Channel>,
    search_tx: Sender<String>,
    ioc_finder_jh: tokio::task::JoinHandle<()>,
    datastore: Arc<DataStore>,
    common_insert_item_queue: Arc<CommonInsertItemQueue>,
    insert_queue_counter: Arc<AtomicUsize>,
    count_unknown_address: usize,
    count_search_pending: usize,
    count_search_sent: usize,
    count_no_address: usize,
    count_unassigned: usize,
    count_assigned: usize,
    last_status_print: SystemTime,
    insert_workers_jh: Vec<tokio::task::JoinHandle<()>>,
    #[allow(unused)]
    pg_client: Arc<PgClient>,
    ingest_commons: Arc<IngestCommons>,
    caconn_last_channel_check: Instant,
    stats: Arc<DaemonStats>,
    shutting_down: bool,
}

impl Daemon {
    pub async fn new(opts: DaemonOpts) -> Result<Self, Error> {
        let pg_client = Arc::new(make_pg_client(&opts.pgconf).await?);
        let datastore = DataStore::new(&opts.scyconf).await?;
        let datastore = Arc::new(datastore);
        let (tx, rx) = async_channel::bounded(32);
        let pgcs = {
            let mut a = Vec::new();
            for _ in 0..SEARCH_DB_PIPELINE_LEN {
                let pgc = Arc::new(make_pg_client(&opts.pgconf).await?);
                a.push(pgc);
            }
            a
        };
        let (search_tx, ioc_finder_jh) = Self::start_finder(tx.clone(), opts.backend().into(), pgcs);

        let channel_info_query_tx = netfetch::batchquery::series_by_channel::start_task(&opts.pgconf).await?;

        let common_insert_item_queue = Arc::new(CommonInsertItemQueue::new(opts.insert_item_queue_cap));
        let common_insert_item_queue_2 = Arc::new(CommonInsertItemQueue::new(opts.insert_item_queue_cap));
        let insert_queue_counter = Arc::new(AtomicUsize::new(0));

        // Insert queue hook
        if true {
            tokio::spawn({
                let rx = common_insert_item_queue
                    .receiver()
                    .ok_or_else(|| Error::with_msg_no_trace("can not derive receiver for insert queue adapter"))?;
                let tx = common_insert_item_queue_2
                    .sender()
                    .ok_or_else(|| Error::with_msg_no_trace("can not derive sender for insert queue adapter"))?;
                let insert_queue_counter = insert_queue_counter.clone();
                let common_insert_item_queue_2 = common_insert_item_queue_2.clone();
                async move {
                    let mut printed_last = Instant::now();
                    let mut histo = BTreeMap::new();
                    while let Ok(item) = rx.recv().await {
                        insert_queue_counter.fetch_add(1, atomic::Ordering::AcqRel);
                        //trace!("insert queue item {item:?}");
                        match &item {
                            netfetch::store::QueryItem::Insert(item) => {
                                let shape_kind = match &item.shape {
                                    netpod::Shape::Scalar => 0 as u32,
                                    netpod::Shape::Wave(_) => 1,
                                    netpod::Shape::Image(_, _) => 2,
                                };
                                histo
                                    .entry(item.series.clone())
                                    .and_modify(|(c, msp, lsp, pulse, _shape_kind)| {
                                        *c += 1;
                                        *msp = item.ts_msp;
                                        *lsp = item.ts_lsp;
                                        *pulse = item.pulse;
                                        // TODO should check that shape_kind stays the same.
                                    })
                                    .or_insert((0 as usize, item.ts_msp, item.ts_lsp, item.pulse, shape_kind));
                            }
                            _ => {}
                        }
                        match tx.send(item).await {
                            Ok(_) => {}
                            Err(e) => {
                                error!("insert queue hook send {e}");
                                break;
                            }
                        }
                        let tsnow = Instant::now();
                        if tsnow.duration_since(printed_last) >= PRINT_ACTIVE_INTERVAL {
                            printed_last = tsnow;
                            let mut all: Vec<_> = histo
                                .iter()
                                .map(|(k, (c, msp, lsp, pulse, shape_kind))| {
                                    (usize::MAX - *c, k.clone(), *msp, *lsp, *pulse, *shape_kind)
                                })
                                .collect();
                            all.sort_unstable();
                            info!("Active scalar");
                            for (c, sid, msp, lsp, pulse, _shape_kind) in all.iter().filter(|x| x.5 == 0).take(6) {
                                info!("{:10}  {:20}  {:14}  {:20}  {:?}", usize::MAX - c, msp, lsp, pulse, sid);
                            }
                            info!("Active wave");
                            for (c, sid, msp, lsp, pulse, _shape_kind) in all.iter().filter(|x| x.5 == 1).take(6) {
                                info!("{:10}  {:20}  {:14}  {:20}  {:?}", usize::MAX - c, msp, lsp, pulse, sid);
                            }
                            histo.clear();
                        }
                    }
                    info!("insert queue adapter ended");
                    common_insert_item_queue_2.drop_sender();
                }
            });
        }

        let ingest_commons = IngestCommons {
            pgconf: Arc::new(opts.pgconf.clone()),
            backend: opts.backend().into(),
            local_epics_hostname: opts.local_epics_hostname.clone(),
            insert_item_queue: common_insert_item_queue.clone(),
            data_store: datastore.clone(),
            insert_ivl_min: Arc::new(AtomicU64::new(0)),
            extra_inserts_conf: tokio::sync::Mutex::new(ExtraInsertsConf::new()),
            store_workers_rate: AtomicU64::new(20000),
            insert_frac: AtomicU64::new(1000),
            ca_conn_set: CaConnSet::new(channel_info_query_tx),
            insert_workers_running: atomic::AtomicUsize::new(0),
        };
        let ingest_commons = Arc::new(ingest_commons);

        tokio::task::spawn({
            let rx = ingest_commons.ca_conn_set.conn_item_rx();
            let tx = tx.clone();
            async move {
                while let Ok(item) = rx.recv().await {
                    match tx.send(DaemonEvent::CaConnEvent(item.0, item.1)).await {
                        Ok(_) => {}
                        Err(e) => {
                            error!("{e}");
                            break;
                        }
                    }
                }
            }
        });

        let insert_scylla_sessions = 1;
        let insert_worker_count = 1000;
        let use_rate_limit_queue = false;

        // TODO use a new stats type:
        let store_stats = Arc::new(stats::CaConnStats::new());
        let ttls = opts.ttls.clone();
        let jh_insert_workers = netfetch::insertworker::spawn_scylla_insert_workers(
            opts.scyconf.clone(),
            insert_scylla_sessions,
            insert_worker_count,
            common_insert_item_queue_2.clone(),
            ingest_commons.clone(),
            pg_client.clone(),
            store_stats.clone(),
            use_rate_limit_queue,
            ttls,
        )
        .await?;

        let ret = Self {
            opts,
            connection_states: BTreeMap::new(),
            channel_states: BTreeMap::new(),
            tx,
            rx,
            chan_check_next: None,
            search_tx,
            ioc_finder_jh,
            datastore,
            common_insert_item_queue,
            insert_queue_counter,
            count_unknown_address: 0,
            count_search_pending: 0,
            count_search_sent: 0,
            count_no_address: 0,
            count_unassigned: 0,
            count_assigned: 0,
            last_status_print: SystemTime::now(),
            insert_workers_jh: jh_insert_workers,
            pg_client,
            ingest_commons,
            caconn_last_channel_check: Instant::now(),
            stats: Arc::new(DaemonStats::new()),
            shutting_down: false,
        };
        Ok(ret)
    }

    fn start_finder(
        tx: Sender<DaemonEvent>,
        backend: String,
        pgcs: Vec<Arc<PgClient>>,
    ) -> (Sender<String>, tokio::task::JoinHandle<()>) {
        fn transform_pgres(rows: Vec<PgRow>) -> VecDeque<FindIocRes> {
            let mut ret = VecDeque::new();
            for row in rows {
                let ch: Result<String, _> = row.try_get(0);
                if let Ok(ch) = ch {
                    if let Some(addr) = row.get::<_, Option<String>>(1) {
                        let addr = addr.parse().map_or(None, |x| Some(x));
                        let item = FindIocRes {
                            channel: ch,
                            response_addr: None,
                            addr,
                            dt: Duration::from_millis(0),
                        };
                        ret.push_back(item);
                    } else {
                        let item = FindIocRes {
                            channel: ch,
                            response_addr: None,
                            addr: None,
                            dt: Duration::from_millis(0),
                        };
                        ret.push_back(item);
                    }
                } else if let Err(e) = ch {
                    error!("bad string from pg: {e:?}");
                }
            }
            ret
        }
        let (qtx, qrx) = async_channel::bounded(CURRENT_SEARCH_PENDING_MAX);
        let fut = async move {
            let (batch_rx, _jh) = netfetch::batcher::batch(
                SEARCH_BATCH_MAX,
                Duration::from_millis(200),
                SEARCH_DB_PIPELINE_LEN,
                qrx,
            );
            let (pgc_tx, pgc_rx) = async_channel::bounded(128);
            for pgc in pgcs {
                let sql = concat!(
                    "with q1 as (select * from unnest($2::text[]) as unn (ch))",
                    " select distinct on (tt.facility, tt.channel) tt.channel, tt.addr",
                    " from ioc_by_channel_log tt join q1 on tt.channel = q1.ch and tt.facility = $1 and tt.addr is not null",
                    " order by tt.facility, tt.channel, tsmod desc",
                );
                let qu_select_multi = pgc.prepare(sql).await.unwrap();
                let qu_select_multi = Arc::new(qu_select_multi);
                match pgc_tx.send((pgc, qu_select_multi)).await {
                    Ok(_) => {}
                    Err(e) => {
                        error!("can not enqueue pgc {e}");
                    }
                }
            }
            let backend = Arc::new(backend.clone());
            let stream = batch_rx
                .map(|batch: Vec<String>| {
                    let pgc_tx = pgc_tx.clone();
                    let pgc_rx = pgc_rx.clone();
                    let backend = backend.clone();
                    SEARCH_REQ_BATCH_RECV_COUNT.fetch_add(batch.len(), atomic::Ordering::AcqRel);
                    async move {
                        let ts1 = Instant::now();
                        let (pgc, qu_select_multi) = pgc_rx.recv().await.unwrap();
                        debug_batch!("run  query batch  len {}", batch.len());
                        let qres = pgc.query(qu_select_multi.as_ref(), &[backend.as_ref(), &batch]).await;
                        let dt = ts1.elapsed();
                        debug_batch!(
                            "done query batch  len {}: {}  {:.3}ms",
                            batch.len(),
                            qres.is_ok(),
                            dt.as_secs_f32() * 1e3
                        );
                        if dt > Duration::from_millis(5000) {
                            let mut out = String::from("[");
                            for s in &batch {
                                if out.len() > 1 {
                                    out.push_str(", ");
                                }
                                out.push('\'');
                                out.push_str(s);
                                out.push('\'');
                            }
                            out.push(']');
                            eprintln!("VERY SLOW QUERY\n{out}");
                        }
                        pgc_tx.send((pgc, qu_select_multi)).await.unwrap();
                        (batch, qres)
                    }
                })
                .buffer_unordered(SEARCH_DB_PIPELINE_LEN);
            let mut resdiff = 0;
            let mut stream = Box::pin(stream);
            while let Some((batch, pgres)) = stream.next().await {
                match pgres {
                    Ok(rows) => {
                        if rows.len() > batch.len() {
                            error!("MORE RESULTS THAN INPUT");
                        } else if rows.len() < batch.len() {
                            resdiff += batch.len() - rows.len();
                        }
                        let nbatch = batch.len();
                        trace_batch!("received results {}  resdiff {}", rows.len(), resdiff);
                        SEARCH_RES_0_COUNT.fetch_add(rows.len(), atomic::Ordering::AcqRel);
                        let items = transform_pgres(rows);
                        let names: HashMap<_, _> = items.iter().map(|x| (&x.channel, true)).collect();
                        let mut to_add = Vec::new();
                        for s in batch {
                            if !names.contains_key(&s) {
                                let item = FindIocRes {
                                    channel: s,
                                    response_addr: None,
                                    addr: None,
                                    dt: Duration::from_millis(0),
                                };
                                to_add.push(item);
                            }
                        }
                        SEARCH_RES_1_COUNT.fetch_add(items.len(), atomic::Ordering::AcqRel);
                        SEARCH_RES_2_COUNT.fetch_add(to_add.len(), atomic::Ordering::AcqRel);
                        let mut items = items;
                        items.extend(to_add.into_iter());
                        if items.len() != nbatch {
                            error!("STILL NOT MATCHING LEN");
                        }
                        SEARCH_RES_3_COUNT.fetch_add(items.len(), atomic::Ordering::AcqRel);
                        let x = tx.send(DaemonEvent::SearchDone(Ok(items))).await;
                        match x {
                            Ok(_) => {}
                            Err(e) => {
                                error!("finder sees: {e}");
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        error!("finder sees error: {e}");
                        tokio::time::sleep(Duration::from_millis(1000)).await;
                    }
                }
            }
        };
        let jh = taskrun::spawn(fut);
        (qtx, jh)
    }

    #[allow(unused)]
    fn start_finder_ca(
        tx: Sender<DaemonEvent>,
        tgts: Vec<SocketAddrV4>,
    ) -> (Sender<String>, tokio::task::JoinHandle<()>) {
        let (qtx, qrx) = async_channel::bounded(32);
        let (atx, arx) = async_channel::bounded(32);
        let ioc_finder_fut = async move {
            let mut finder = FindIocStream::new(tgts, FINDER_TIMEOUT, FINDER_IN_FLIGHT_MAX, FINDER_BATCH_SIZE);
            let fut_tick_dur = Duration::from_millis(100);
            let mut finder_more = true;
            let mut finder_fut = OptFut::new(finder.next());
            let mut qrx_fut = OptFut::new(qrx.recv());
            let mut qrx_more = true;
            let mut fut_tick = Box::pin(tokio::time::sleep(fut_tick_dur));
            let mut asend = OptFut::empty();
            loop {
                tokio::select! {
                    _ = &mut asend, if asend.is_enabled() => {
                        asend = OptFut::empty();
                    }
                    r1 = &mut finder_fut, if finder_fut.is_enabled() => {
                        finder_fut = OptFut::empty();
                        match r1 {
                            Some(item) => {
                                asend = OptFut::new(atx.send(item));
                            }
                            None => {
                                // TODO finder has stopped, do no longer poll on it
                                warn!("Finder has stopped");
                                finder_more = false;
                            }
                        }
                        if qrx_more && finder.job_queue_len() < FINDER_JOB_QUEUE_LEN_MAX {
                            qrx_fut = OptFut::new(qrx.recv());
                        }
                        if finder_more {
                            finder_fut = OptFut::new(finder.next());
                        }
                        fut_tick = Box::pin(tokio::time::sleep(fut_tick_dur));
                    }
                    r2 = &mut qrx_fut, if qrx_fut.is_enabled() => {
                        qrx_fut = OptFut::empty();
                        match r2 {
                            Ok(item) => {
                                finder.push(item);
                            }
                            Err(e) => {
                                // TODO input is done... ignore from here on.
                                error!("Finder input channel error {e}");
                                qrx_more = false;
                            }
                        }
                        if qrx_more && finder.job_queue_len() < FINDER_JOB_QUEUE_LEN_MAX {
                            qrx_fut = OptFut::new(qrx.recv());
                        }
                        if finder_more {
                            finder_fut = OptFut::new(finder.next());
                        } else {
                            finder_fut = OptFut::empty();
                        }
                        fut_tick = Box::pin(tokio::time::sleep(fut_tick_dur));
                    }
                    _ = &mut fut_tick => {
                        if qrx_more && finder.job_queue_len() < FINDER_JOB_QUEUE_LEN_MAX {
                            qrx_fut = OptFut::new(qrx.recv());
                        }
                        if finder_more {
                            finder_fut = OptFut::new(finder.next());
                        } else {
                            finder_fut = OptFut::empty();
                        }
                        fut_tick = Box::pin(tokio::time::sleep(fut_tick_dur));
                    }
                    else => {
                        error!("all branches are disabled");
                        break;
                    }
                };
            }
        };
        let ioc_finder_jh = taskrun::spawn(ioc_finder_fut);
        taskrun::spawn({
            async move {
                while let Ok(item) = arx.recv().await {
                    match tx.send(DaemonEvent::SearchDone(item)).await {
                        Ok(_) => {}
                        Err(e) => {
                            error!("search res fwd {e}");
                        }
                    }
                }
                warn!("search res fwd nput broken");
            }
        });
        (qtx, ioc_finder_jh)
    }

    fn stats(&self) -> &Arc<DaemonStats> {
        &self.stats
    }

    fn allow_create_new_connections(&self) -> bool {
        !self.shutting_down
    }

    async fn check_chans(&mut self) -> Result<(), Error> {
        {
            let tsnow = Instant::now();
            for (k, v) in &mut self.connection_states {
                match v.value {
                    CaConnStateValue::Fresh => {
                        // TODO check for delta t since last issued status command.
                        if tsnow.duration_since(v.last_feedback) > Duration::from_millis(20000) {
                            error!("TODO send connection-close for {k:?}");
                            self.stats.ca_conn_status_feedback_timeout_inc();
                            v.value = CaConnStateValue::Shutdown { since: tsnow };
                        }
                    }
                    CaConnStateValue::HadFeedback => {
                        // TODO check for delta t since last issued status command.
                        if tsnow.duration_since(v.last_feedback) > Duration::from_millis(20000) {
                            error!("TODO send connection-close for {k:?}");
                            self.stats.ca_conn_status_feedback_timeout_inc();
                            v.value = CaConnStateValue::Shutdown { since: tsnow };
                        }
                    }
                    CaConnStateValue::Shutdown { since } => {
                        if tsnow.saturating_duration_since(since) > Duration::from_millis(10000) {
                            self.stats.critical_error_inc();
                            error!("Shutdown of CaConn to {} failed", k);
                        }
                    }
                }
            }
        }
        let mut currently_search_pending = 0;
        {
            let mut with_address_count = 0;
            let mut without_address_count = 0;
            for (_ch, st) in &self.channel_states {
                match &st.value {
                    ChannelStateValue::Active(st2) => match st2 {
                        ActiveChannelState::UnknownAddress { since: _ } => {
                            without_address_count += 1;
                        }
                        ActiveChannelState::SearchPending { since: _, did_send: _ } => {
                            currently_search_pending += 1;
                            without_address_count += 1;
                        }
                        ActiveChannelState::WithAddress { addr: _, state } => match state {
                            WithAddressState::Unassigned { assign_at: _ } => {
                                with_address_count += 1;
                            }
                            WithAddressState::Assigned(_) => {
                                with_address_count += 1;
                            }
                        },
                        ActiveChannelState::NoAddress { since: _ } => {
                            without_address_count += 1;
                        }
                    },
                    ChannelStateValue::ToRemove { addr: _ } => {
                        with_address_count += 1;
                    }
                }
            }
            self.stats
                .channel_with_address
                .store(with_address_count, atomic::Ordering::Release);
            self.stats
                .channel_without_address
                .store(without_address_count, atomic::Ordering::Release);
        }
        let k = self.chan_check_next.take();
        trace!("------------   check_chans  start at {:?}", k);
        let it = if let Some(last) = k {
            self.channel_states.range_mut(last..)
        } else {
            self.channel_states.range_mut(..)
        };
        let tsnow = SystemTime::now();
        for (i, (ch, st)) in it.enumerate() {
            use ActiveChannelState::*;
            use ChannelStateValue::*;
            match &mut st.value {
                Active(st2) => match st2 {
                    UnknownAddress { since } => {
                        let dt = tsnow.duration_since(*since).unwrap_or(Duration::ZERO);
                        if dt > UNKNOWN_ADDRESS_STAY {
                            //info!("UnknownAddress {} {:?}", i, ch);
                            if currently_search_pending < CURRENT_SEARCH_PENDING_MAX {
                                currently_search_pending += 1;
                                st.value = Active(SearchPending {
                                    since: tsnow,
                                    did_send: false,
                                });
                                SEARCH_REQ_MARK_COUNT.fetch_add(1, atomic::Ordering::AcqRel);
                            }
                        }
                    }
                    SearchPending { since, did_send: _ } => {
                        //info!("SearchPending {} {:?}", i, ch);
                        let dt = tsnow.duration_since(*since).unwrap_or(Duration::ZERO);
                        if dt > SEARCH_PENDING_TIMEOUT {
                            info!("Search timeout for {ch:?}");
                            st.value = Active(ActiveChannelState::NoAddress { since: tsnow });
                            currently_search_pending -= 1;
                        }
                    }
                    WithAddress { addr, state } => {
                        //info!("WithAddress {} {:?}", i, ch);
                        use WithAddressState::*;
                        match state {
                            Unassigned { assign_at } => {
                                if DO_ASSIGN_TO_CA_CONN && *assign_at <= tsnow {
                                    let backend = self.opts.backend().into();
                                    let channel_name = ch.id().into();
                                    // This operation is meant to complete very quickly
                                    self.ingest_commons
                                        .ca_conn_set
                                        .add_channel_to_addr(
                                            backend,
                                            *addr,
                                            channel_name,
                                            &self.common_insert_item_queue,
                                            &self.datastore,
                                            CA_CONN_INSERT_QUEUE_MAX,
                                            self.opts.array_truncate,
                                            self.opts.local_epics_hostname.clone(),
                                        )
                                        .slow_warn(2000)
                                        .instrument(info_span!("add_channel_to_addr"))
                                        .await?;
                                    let cs = ConnectionState {
                                        updated: tsnow,
                                        value: ConnectionStateValue::Unconnected,
                                    };
                                    *state = WithAddressState::Assigned(cs);
                                    self.connection_states.entry(*addr).or_insert_with(|| {
                                        let t = CaConnState {
                                            last_feedback: Instant::now(),
                                            value: CaConnStateValue::Fresh,
                                        };
                                        t
                                    });
                                }
                            }
                            Assigned(_) => {
                                // TODO check if channel is healthy and alive
                            }
                        }
                    }
                    NoAddress { since } => {
                        let dt = tsnow.duration_since(*since).unwrap_or(Duration::ZERO);
                        if dt > NO_ADDRESS_STAY {
                            st.value = Active(ActiveChannelState::UnknownAddress { since: tsnow });
                        }
                    }
                },
                ToRemove { .. } => {
                    // TODO if assigned to some address,
                }
            }
            if i >= CHECK_CHANS_PER_TICK {
                self.chan_check_next = Some(ch.clone());
                break;
            }
        }
        for (ch, st) in &mut self.channel_states {
            if let ChannelStateValue::Active(ActiveChannelState::SearchPending { since: _, did_send }) = &mut st.value {
                if *did_send == false {
                    match self.search_tx.try_send(ch.id().into()) {
                        Ok(()) => {
                            *did_send = true;
                            SEARCH_REQ_SEND_COUNT.fetch_add(1, atomic::Ordering::AcqRel);
                        }
                        Err(e) => match e {
                            async_channel::TrySendError::Full(_) => {}
                            async_channel::TrySendError::Closed(_) => {
                                error!("Finder channel closed");
                                // TODO recover from this.
                                panic!();
                            }
                        },
                    }
                }
            }
        }
        {
            self.count_unknown_address = 0;
            self.count_search_pending = 0;
            self.count_search_sent = 0;
            self.count_no_address = 0;
            self.count_unassigned = 0;
            self.count_assigned = 0;
            for (_ch, st) in &self.channel_states {
                match &st.value {
                    ChannelStateValue::Active(st) => match st {
                        ActiveChannelState::UnknownAddress { .. } => {
                            self.count_unknown_address += 1;
                        }
                        ActiveChannelState::SearchPending { did_send, .. } => {
                            self.count_search_pending += 1;
                            if *did_send {
                                self.count_search_sent += 1;
                            }
                        }
                        ActiveChannelState::WithAddress { state, .. } => match state {
                            WithAddressState::Unassigned { .. } => {
                                self.count_unassigned += 1;
                            }
                            WithAddressState::Assigned(_) => {
                                self.count_assigned += 1;
                            }
                        },
                        ActiveChannelState::NoAddress { .. } => {
                            self.count_no_address += 1;
                        }
                    },
                    ChannelStateValue::ToRemove { .. } => {}
                }
            }
        }
        Ok(())
    }

    async fn check_caconn_chans(&mut self) -> Result<(), Error> {
        if self.caconn_last_channel_check.elapsed() > CHANNEL_CHECK_INTERVAL {
            info!("Issue channel check to all CaConn");
            self.ingest_commons
                .ca_conn_set
                .enqueue_command_to_all(|| ConnCommand::check_health())
                .await?;
            self.caconn_last_channel_check = Instant::now();
        }
        Ok(())
    }

    async fn ca_conn_send_shutdown(&mut self) -> Result<(), Error> {
        warn!("send shutdown to all ca connections");
        self.ingest_commons
            .ca_conn_set
            .enqueue_command_to_all(|| ConnCommand::shutdown())
            .await?;
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
            info!("qu senders A {:?} {:?}  nworkers {}", sa1, sa2, nworkers);
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
        self.check_chans().await?;
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
        if tsnow.duration_since(self.last_status_print).unwrap_or(Duration::ZERO) >= Duration::from_millis(1000) {
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
            if false {
                info!(
                    "{:5} {:5} {:5} {:5} {:5} {:5} {:5} {:5} {:5} {:5}",
                    SEARCH_REQ_MARK_COUNT.load(atomic::Ordering::Acquire),
                    SEARCH_REQ_SEND_COUNT.load(atomic::Ordering::Acquire),
                    SEARCH_REQ_RECV_COUNT.load(atomic::Ordering::Acquire),
                    SEARCH_REQ_BATCH_SEND_COUNT.load(atomic::Ordering::Acquire),
                    SEARCH_REQ_BATCH_RECV_COUNT.load(atomic::Ordering::Acquire),
                    SEARCH_RES_0_COUNT.load(atomic::Ordering::Acquire),
                    SEARCH_RES_1_COUNT.load(atomic::Ordering::Acquire),
                    SEARCH_RES_2_COUNT.load(atomic::Ordering::Acquire),
                    SEARCH_RES_3_COUNT.load(atomic::Ordering::Acquire),
                    SEARCH_ANS_COUNT.load(atomic::Ordering::Acquire),
                );
            }
        }
        Ok(())
    }

    fn handle_channel_add(&mut self, ch: Channel) -> Result<(), Error> {
        if !self.channel_states.contains_key(&ch) {
            let st = ChannelState {
                value: ChannelStateValue::Active(ActiveChannelState::UnknownAddress {
                    since: SystemTime::now(),
                }),
            };
            self.channel_states.insert(ch, st);
        }
        Ok(())
    }

    fn handle_channel_remove(&mut self, ch: Channel) -> Result<(), Error> {
        if let Some(k) = self.channel_states.get_mut(&ch) {
            match &k.value {
                ChannelStateValue::Active(j) => match j {
                    ActiveChannelState::UnknownAddress { .. } => {
                        k.value = ChannelStateValue::ToRemove { addr: None };
                    }
                    ActiveChannelState::SearchPending { .. } => {
                        k.value = ChannelStateValue::ToRemove { addr: None };
                    }
                    ActiveChannelState::WithAddress { addr, .. } => {
                        k.value = ChannelStateValue::ToRemove {
                            addr: Some(addr.clone()),
                        };
                    }
                    ActiveChannelState::NoAddress { .. } => {
                        k.value = ChannelStateValue::ToRemove { addr: None };
                    }
                },
                ChannelStateValue::ToRemove { .. } => {}
            }
        }
        Ok(())
    }

    async fn handle_search_done(&mut self, item: Result<VecDeque<FindIocRes>, Error>) -> Result<(), Error> {
        //debug!("handle SearchDone: {res:?}");
        let tsnow = SystemTime::now();
        match item {
            Ok(ress) => {
                SEARCH_ANS_COUNT.fetch_add(ress.len(), atomic::Ordering::AcqRel);
                for res in ress {
                    if let Some(addr) = &res.addr {
                        self.stats.ioc_search_some_inc();
                        if self.allow_create_new_connections() {
                            let ch = Channel::new(res.channel);
                            if let Some(st) = self.channel_states.get_mut(&ch) {
                                if let ChannelStateValue::Active(ActiveChannelState::SearchPending {
                                    since,
                                    did_send: _,
                                }) = &st.value
                                {
                                    let dt = tsnow.duration_since(*since).unwrap();
                                    if dt > SEARCH_PENDING_TIMEOUT_WARN {
                                        warn!(
                                            "    FOUND {:5.0}  {:5.0}  {addr}",
                                            1e3 * dt.as_secs_f32(),
                                            1e3 * res.dt.as_secs_f32()
                                        );
                                    }
                                    let stnew = ChannelStateValue::Active(ActiveChannelState::WithAddress {
                                        addr: addr.clone(),
                                        state: WithAddressState::Unassigned { assign_at: tsnow },
                                    });
                                    st.value = stnew;
                                } else {
                                    warn!(
                                        "address found, but state for {ch:?} is not SearchPending: {:?}",
                                        st.value
                                    );
                                }
                            } else {
                                warn!("can not find channel state for {ch:?}");
                            }
                        } else {
                            // Emit something here?
                        }
                    } else {
                        //debug!("no addr from search in {res:?}");
                        let ch = Channel::new(res.channel);
                        if let Some(st) = self.channel_states.get_mut(&ch) {
                            if let ChannelStateValue::Active(ActiveChannelState::SearchPending { since, did_send: _ }) =
                                &st.value
                            {
                                let dt = tsnow.duration_since(*since).unwrap();
                                if dt > SEARCH_PENDING_TIMEOUT_WARN {
                                    warn!(
                                        "NOT FOUND {:5.0}  {:5.0}",
                                        1e3 * dt.as_secs_f32(),
                                        1e3 * res.dt.as_secs_f32()
                                    );
                                }
                                st.value = ChannelStateValue::Active(ActiveChannelState::NoAddress { since: tsnow });
                            } else {
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
        info!("handle_ca_conn_done {conn_addr:?}");
        self.connection_states.remove(&conn_addr);
        for (_k, v) in self.channel_states.iter_mut() {
            match &v.value {
                ChannelStateValue::Active(st2) => match st2 {
                    ActiveChannelState::UnknownAddress { .. } => {}
                    ActiveChannelState::SearchPending { since: _, did_send: _ } => {}
                    ActiveChannelState::WithAddress { addr, state: _ } => {
                        if addr == &conn_addr {
                            // TODO reset channel, emit log event for the connection addr only
                            //info!("ca conn down, reset {k:?}");
                            *v = ChannelState {
                                value: ChannelStateValue::Active(ActiveChannelState::UnknownAddress {
                                    since: SystemTime::now(),
                                }),
                            };
                        }
                    }
                    ActiveChannelState::NoAddress { .. } => {}
                },
                ChannelStateValue::ToRemove { addr: _ } => {}
            }
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
            EndOfStream => {
                self.stats.ca_conn_status_done_inc();
                self.handle_ca_conn_done(addr).await
            }
        }
    }

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

    async fn handle_event(&mut self, item: DaemonEvent, ticker_inp_tx: &Sender<u32>) -> Result<(), Error> {
        use DaemonEvent::*;
        self.stats.events_inc();
        let ts1 = Instant::now();
        let item_summary = item.summary();
        let ret = match item {
            TimerTick => {
                let ret = self.handle_timer_tick().await;
                match ticker_inp_tx.send(42).await {
                    Ok(_) => {}
                    Err(_) => {
                        self.stats.ticker_token_release_error_inc();
                        error!("can not send ticker token");
                        return Err(Error::with_msg_no_trace("can not send ticker token"));
                    }
                }
                ret
            }
            ChannelAdd(ch) => self.handle_channel_add(ch),
            ChannelRemove(ch) => self.handle_channel_remove(ch),
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

    pub async fn daemon(&mut self) -> Result<(), Error> {
        let (ticker_inp_tx, ticker_inp_rx) = async_channel::bounded::<u32>(1);
        let ticker = {
            let tx = self.tx.clone();
            let stats = self.stats.clone();
            async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(100)).await;
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
                    if let Err(e) = tx.send(DaemonEvent::TimerTick).await {
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
        taskrun::spawn(ticker);
        loop {
            match self.rx.recv().await {
                Ok(item) => match self.handle_event(item, &ticker_inp_tx).await {
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
        warn!("TODO wait for IOC finder properly");
        let _ = &self.ioc_finder_jh;
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
    let _ = netfetch::linuxhelper::unset_signal_handler(libc::SIGINT);
}

fn handler_sigterm(_a: libc::c_int, _b: *const libc::siginfo_t, _c: *const libc::c_void) {
    SIGTERM.store(1, atomic::Ordering::Release);
    let _ = netfetch::linuxhelper::unset_signal_handler(libc::SIGTERM);
}

pub async fn run(opts: CaIngestOpts, channels: Vec<String>) -> Result<(), Error> {
    info!("start up {opts:?}");
    netfetch::linuxhelper::set_signal_handler(libc::SIGINT, handler_sigint)?;
    netfetch::linuxhelper::set_signal_handler(libc::SIGTERM, handler_sigterm)?;

    // TODO use a new stats type:
    //let store_stats = Arc::new(CaConnStats::new());
    //let metrics_agg_fut = metrics_agg_task(ingest_commons.clone(), local_stats.clone(), store_stats.clone());
    //let metrics_agg_jh = tokio::spawn(metrics_agg_fut);

    let opts2 = DaemonOpts {
        backend: opts.backend().into(),
        local_epics_hostname: opts.local_epics_hostname().into(),
        array_truncate: opts.array_truncate(),
        insert_item_queue_cap: opts.insert_item_queue_cap(),
        pgconf: opts.postgresql().clone(),
        scyconf: opts.scylla().clone(),
        ttls: Ttls {
            index: opts.ttl_index(),
            d0: opts.ttl_d0(),
            d1: opts.ttl_d1(),
        },
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
    info!("all channels sent to daemon");
    daemon_jh.await.unwrap();
    if false {
        metrics_jh.await.unwrap();
    }
    Ok(())
}
