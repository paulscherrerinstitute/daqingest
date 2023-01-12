use async_channel::Receiver;
use async_channel::Sender;
use err::Error;
use futures_util::Future;
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
use netfetch::conf::CaIngestOpts;
use netfetch::errconv::ErrConv;
use netfetch::insertworker::Ttls;
use netfetch::metrics::ExtraInsertsConf;
use netfetch::store::CommonInsertItemQueue;
use netpod::Database;
use netpod::ScyllaConfig;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::fmt;
use std::net::SocketAddr;
use std::net::SocketAddrV4;
use std::pin::Pin;
use std::sync::atomic;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use tokio_postgres::Client as PgClient;
use tokio_postgres::Statement as PgStatement;

const CHECK_CHANS_PER_TICK: usize = 10000;
const FINDER_TIMEOUT: usize = 100;
const TIMEOUT_WARN: usize = 6000;
const FINDER_JOB_QUEUE_LEN_MAX: usize = 20;
const FINDER_IN_FLIGHT_MAX: usize = 400;
const FINDER_BATCH_SIZE: usize = 8;
const CURRENT_SEARCH_PENDING_MAX: usize = 420;
const SEARCH_PENDING_TIMEOUT: usize = 10000;

#[derive(Clone, Debug, Serialize, PartialEq, PartialOrd, Eq, Ord)]
pub struct Channel {
    id: String,
}

impl Channel {
    pub fn new(id: String) -> Self {
        Self { id }
    }

    pub fn id(&self) -> &str {
        &self.id
    }
}

#[derive(Clone, Debug, Serialize)]
pub enum ConnectionStateValue {
    Unconnected,
    Connected { since: SystemTime },
}

#[derive(Clone, Debug, Serialize)]
pub struct ConnectionState {
    updated: SystemTime,
    value: ConnectionStateValue,
}

#[derive(Clone, Debug, Serialize)]
pub enum WithAddressState {
    Unassigned { assign_at: SystemTime },
    Assigned(ConnectionState),
}

#[derive(Clone, Debug, Serialize)]
pub enum ActiveChannelState {
    UnknownAddress,
    SearchPending {
        since: SystemTime,
    },
    WithAddress {
        addr: SocketAddrV4,
        state: WithAddressState,
    },
    NoAddress,
}

enum ChanOp {
    Finder(String, SystemTime),
    ConnCmd(Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>),
}

impl fmt::Debug for ChanOp {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Finder(arg0, arg1) => fmt.debug_tuple("Finder").field(arg0).field(arg1).finish(),
            Self::ConnCmd(_arg0) => fmt.debug_tuple("ConnCmd").finish(),
        }
    }
}

#[derive(Debug, Serialize)]
pub enum ChannelStateValue {
    Active(ActiveChannelState),
    ToRemove { addr: Option<SocketAddrV4> },
}

#[derive(Debug)]
pub struct ChannelState {
    value: ChannelStateValue,
    pending_op: Option<ChanOp>,
}

#[derive(Debug)]
pub enum DaemonEvent {
    TimerTick,
    ChannelAdd(Channel),
    ChannelRemove(Channel),
    SearchDone(Result<VecDeque<FindIocRes>, Error>),
    CaConnEvent(CaConnEvent),
}

#[derive(Debug, Clone)]
pub struct DaemonOpts {
    backend: String,
    local_epics_hostname: String,
    array_truncate: usize,
    insert_item_queue_cap: usize,
    search_tgts: Vec<SocketAddrV4>,
    //search_excl: Vec<SocketAddrV4>,
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

struct AddrInsertJob {
    backend: String,
    channel: Channel,
    response_addr: SocketAddrV4,
    addr: SocketAddrV4,
}

pub struct Daemon {
    opts: DaemonOpts,
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
    count_no_address: usize,
    count_unassigned: usize,
    count_assigned: usize,
    last_status_print: SystemTime,
    insert_workers_jh: Vec<tokio::task::JoinHandle<()>>,
    #[allow(unused)]
    pg_client: Arc<PgClient>,
    #[allow(unused)]
    qu_addr_insert: PgStatement,
    ioc_addr_inserter_jh: tokio::task::JoinHandle<()>,
    ioc_addr_inserter_tx: Sender<AddrInsertJob>,
    ingest_commons: Arc<IngestCommons>,
    caconn_last_channel_check: Instant,
}

impl Daemon {
    pub async fn new(opts: DaemonOpts) -> Result<Self, Error> {
        let pg_client = Arc::new(make_pg_client(&opts.pgconf).await?);
        let datastore = DataStore::new(&opts.scyconf, pg_client.clone()).await?;
        let datastore = Arc::new(datastore);
        let (tx, rx) = async_channel::bounded(32);
        let tgts = opts.search_tgts.clone();
        let (search_tx, ioc_finder_jh) = Self::start_finder(tx.clone(), tgts);
        let common_insert_item_queue = Arc::new(CommonInsertItemQueue::new(opts.insert_item_queue_cap));
        let common_insert_item_queue_2 = Arc::new(CommonInsertItemQueue::new(opts.insert_item_queue_cap));
        let insert_queue_counter = Arc::new(AtomicUsize::new(0));

        // Insert queue hook
        if true {
            tokio::spawn({
                let rx = common_insert_item_queue.receiver();
                let tx = common_insert_item_queue_2.sender().await;
                let insert_queue_counter = insert_queue_counter.clone();
                async move {
                    let mut printed_last = SystemTime::now();
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
                        let tsnow = SystemTime::now();
                        if tsnow.duration_since(printed_last).unwrap_or(Duration::ZERO) >= Duration::from_millis(2000) {
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
            ca_conn_set: CaConnSet::new(),
        };
        let ingest_commons = Arc::new(ingest_commons);

        tokio::task::spawn({
            let rx = ingest_commons.ca_conn_set.conn_item_rx();
            let tx = tx.clone();
            async move {
                while let Ok(item) = rx.recv().await {
                    match tx.send(DaemonEvent::CaConnEvent(item)).await {
                        Ok(_) => {}
                        Err(e) => break,
                    }
                }
            }
        });

        let qu_addr_insert = {
            const TEXT: tokio_postgres::types::Type = tokio_postgres::types::Type::TEXT;
            let sql = "insert into ioc_by_channel_log (facility, channel, responseaddr, addr) values ($1, $2, $3, $4)";
            let res = pg_client
                .prepare_typed(sql, &[TEXT, TEXT, TEXT, TEXT])
                .await
                .map_err(|e| Error::from(e.to_string()))?;
            res
        };

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

        let (ioc_addr_inserter_tx, ioc_addr_inserter_rx) = async_channel::bounded(64);
        let fut = {
            let pg_client = pg_client.clone();
            let qu_addr_insert = qu_addr_insert.clone();
            async move {
                let mut stream = ioc_addr_inserter_rx
                    .map(|k: AddrInsertJob| {
                        let pgc = pg_client.clone();
                        let qu_addr_insert = qu_addr_insert.clone();
                        async move {
                            let response_addr = k.response_addr.to_string();
                            let addr = k.addr.to_string();
                            let res = pgc
                                .execute(&qu_addr_insert, &[&k.backend, &k.channel.id(), &response_addr, &addr])
                                .await
                                .map_err(|e| Error::from(e.to_string()))?;
                            Ok::<_, Error>(res)
                        }
                    })
                    .buffer_unordered(16);
                while let Some(item) = stream.next().await {
                    match item {
                        Ok(_) => {}
                        Err(e) => {
                            error!("{e}");
                        }
                    }
                }
            }
        };
        let ioc_addr_inserter_task = tokio::spawn(fut);

        let ret = Self {
            opts,
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
            count_no_address: 0,
            count_unassigned: 0,
            count_assigned: 0,
            last_status_print: SystemTime::now(),
            insert_workers_jh: jh_insert_workers,
            pg_client,
            qu_addr_insert,
            ioc_addr_inserter_jh: ioc_addr_inserter_task,
            ioc_addr_inserter_tx,
            ingest_commons,
            caconn_last_channel_check: Instant::now(),
        };
        Ok(ret)
    }

    fn start_finder(tx: Sender<DaemonEvent>, tgts: Vec<SocketAddrV4>) -> (Sender<String>, tokio::task::JoinHandle<()>) {
        let (qtx, qrx) = async_channel::bounded(32);
        let (atx, arx) = async_channel::bounded(32);
        let ioc_finder_fut = async move {
            let mut finder = FindIocStream::new(
                tgts,
                Duration::from_millis(FINDER_TIMEOUT as u64),
                FINDER_IN_FLIGHT_MAX,
                FINDER_BATCH_SIZE,
            );
            let fut_tick_dur = Duration::from_millis(100);
            let mut finder_more = true;
            let mut finder_fut = OptFut::new(finder.next());
            let mut qrx_fut = OptFut::new(qrx.recv());
            let mut qrx_more = true;
            let mut fut_tick = Box::pin(tokio::time::sleep(fut_tick_dur));
            let mut asend = OptFut::empty();
            loop {
                //tokio::time::sleep(Duration::from_millis(1)).await;
                tokio::select! {
                    _ = &mut asend, if asend.is_enabled() => {
                        //info!("finder asend done");
                        asend = OptFut::empty();
                    }
                    r1 = &mut finder_fut, if finder_fut.is_enabled() => {
                        //info!("finder fut1");
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
                        //info!("finder.job_queue_len()  {}", finder.job_queue_len());
                        if qrx_more && finder.job_queue_len() < FINDER_JOB_QUEUE_LEN_MAX {
                            qrx_fut = OptFut::new(qrx.recv());
                        }
                        if finder_more {
                            finder_fut = OptFut::new(finder.next());
                        }
                        fut_tick = Box::pin(tokio::time::sleep(fut_tick_dur));
                    }
                    r2 = &mut qrx_fut, if qrx_fut.is_enabled() => {
                        //info!("finder fut2");
                        qrx_fut = OptFut::empty();
                        match r2 {
                            Ok(item) => {
                                //info!("Push to finder: {item:?}");
                                finder.push(item);
                            }
                            Err(e) => {
                                // TODO input is done... ignore from here on.
                                error!("Finder input channel error {e}");
                                qrx_more = false;
                            }
                        }
                        //info!("finder.job_queue_len()  {}", finder.job_queue_len());
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
                        //info!("finder fut_tick finder.job_queue_len()  {}", finder.job_queue_len());
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
                    //info!("forward search result item");
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

    async fn check_chans(&mut self) -> Result<(), Error> {
        let tsnow = SystemTime::now();
        let k = self.chan_check_next.take();
        trace!("------------   check_chans  start at {:?}", k);
        let mut currently_search_pending = 0;
        {
            for (_ch, st) in &self.channel_states {
                if let ChannelStateValue::Active(ActiveChannelState::SearchPending { .. }) = &st.value {
                    currently_search_pending += 1;
                }
            }
        }
        let it = if let Some(last) = k {
            self.channel_states.range_mut(last..)
        } else {
            self.channel_states.range_mut(..)
        };
        for (i, (ch, st)) in it.enumerate() {
            use ActiveChannelState::*;
            use ChannelStateValue::*;
            match &mut st.value {
                Active(st2) => match st2 {
                    UnknownAddress => {
                        //info!("UnknownAddress {} {:?}", i, ch);
                        if currently_search_pending < CURRENT_SEARCH_PENDING_MAX {
                            currently_search_pending += 1;
                            if st.pending_op.is_none() {
                                st.pending_op = Some(ChanOp::Finder(ch.id().to_string(), tsnow));
                                st.value = Active(SearchPending { since: tsnow });
                            }
                        }
                    }
                    SearchPending { since } => {
                        //info!("SearchPending {} {:?}", i, ch);
                        // TODO handle Err
                        match tsnow.duration_since(*since) {
                            Ok(dt) => {
                                if dt >= Duration::from_millis(SEARCH_PENDING_TIMEOUT as u64) {
                                    debug!("Search timeout for {ch:?}");
                                    st.value = Active(ActiveChannelState::NoAddress);
                                    currently_search_pending -= 1;
                                }
                            }
                            Err(e) => {
                                error!("SearchPending {e}");
                            }
                        }
                    }
                    WithAddress { addr, state } => {
                        //info!("WithAddress {} {:?}", i, ch);
                        use WithAddressState::*;
                        match state {
                            Unassigned { assign_at } => {
                                if *assign_at <= tsnow {
                                    if st.pending_op.is_none() {
                                        if self.ingest_commons.ca_conn_set.has_addr(&SocketAddr::V4(*addr)).await {
                                        } else {
                                            debug!("====================   create CaConn for {ch:?}");
                                            let backend = self.opts.backend().into();
                                            let local_epics_hostname = self.opts.local_epics_hostname.clone();
                                            let array_truncate = self.opts.array_truncate;
                                            let insert_item_queue_sender = self.common_insert_item_queue.sender().await;
                                            let insert_queue_max = 256;
                                            let data_store = self.datastore.clone();
                                            let with_channels = Vec::new();
                                            // TODO want to atomically use or create a connection.
                                            // TODO creating a connection may block too long, because it establishes TCP first.
                                            self.ingest_commons
                                                .ca_conn_set
                                                .create_ca_conn(
                                                    backend,
                                                    *addr,
                                                    local_epics_hostname,
                                                    array_truncate,
                                                    insert_queue_max,
                                                    insert_item_queue_sender,
                                                    data_store,
                                                    with_channels,
                                                )
                                                .await?;
                                        }
                                        {
                                            let backend = self.opts.backend().into();
                                            let channel_name = ch.id().into();
                                            let ingest_commons = self.ingest_commons.clone();
                                            // This operation is meant to complete very quickly
                                            self.ingest_commons
                                                .ca_conn_set
                                                .add_channel_to_addr(
                                                    backend,
                                                    SocketAddr::V4(*addr),
                                                    channel_name,
                                                    ingest_commons,
                                                )
                                                .await?;
                                            let cs = ConnectionState {
                                                updated: tsnow,
                                                value: ConnectionStateValue::Unconnected,
                                            };
                                            *state = WithAddressState::Assigned(cs)
                                        }
                                    }
                                }
                            }
                            Assigned(_) => {
                                // TODO check if channel is healthy and alive
                            }
                        }
                    }
                    NoAddress => {
                        // TODO try to find address again after some randomized timeout
                        //info!("NoAddress {} {:?}", i, ch);
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
            match &mut st.pending_op {
                Some(op) => match op {
                    ChanOp::Finder(s, start) => {
                        if *start + Duration::from_millis(10000) >= tsnow {
                            match self.search_tx.try_send(s.clone()) {
                                Ok(_) => {
                                    *start = tsnow;
                                    st.pending_op = None;
                                }
                                Err(e) => match e {
                                    async_channel::TrySendError::Full(_) => {
                                        //warn!("Finder channel full");
                                        *start = tsnow;
                                    }
                                    async_channel::TrySendError::Closed(_) => {
                                        error!("Finder channel closed");
                                    }
                                },
                            }
                        } else {
                            st.pending_op = None;
                            warn!("ChanOp::Finder send timeout for {ch:?}");
                            *st = ChannelState {
                                value: ChannelStateValue::Active(ActiveChannelState::UnknownAddress),
                                pending_op: None,
                            };
                        }
                    }
                    ChanOp::ConnCmd(fut) => {
                        use std::task::Poll::*;
                        match futures_util::poll!(fut) {
                            Ready(res) => {
                                st.pending_op = None;
                                match res {
                                    Ok(_) => {
                                        debug!("ChanOp::ConnCmd completed fine");
                                    }
                                    Err(e) => {
                                        error!("ChanOp::ConnCmd {e}");
                                    }
                                }
                            }
                            Pending => {}
                        }
                    }
                },
                None => {}
            }
        }
        {
            self.count_unknown_address = 0;
            self.count_search_pending = 0;
            self.count_no_address = 0;
            self.count_unassigned = 0;
            self.count_assigned = 0;
            for (_ch, st) in &self.channel_states {
                match &st.value {
                    ChannelStateValue::Active(st) => match st {
                        ActiveChannelState::UnknownAddress => {
                            self.count_unknown_address += 1;
                        }
                        ActiveChannelState::SearchPending { .. } => {
                            self.count_search_pending += 1;
                        }
                        ActiveChannelState::WithAddress { state, .. } => match state {
                            WithAddressState::Unassigned { .. } => {
                                self.count_unassigned += 1;
                            }
                            WithAddressState::Assigned(_) => {
                                self.count_assigned += 1;
                            }
                        },
                        ActiveChannelState::NoAddress => {
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
        if self.caconn_last_channel_check.elapsed() > Duration::from_millis(5000) {
            info!("Issue channel check to all CaConn");
            let res = self
                .ingest_commons
                .ca_conn_set
                .send_command_to_all(|| ConnCommand::check_channels_alive())
                .await?;
            for x in res {
                if !x {
                    warn!("bad check_channels_alive result");
                }
            }
            self.caconn_last_channel_check = Instant::now();
        }
        Ok(())
    }

    async fn handle_timer_tick(&mut self) -> Result<(), Error> {
        let tsnow = SystemTime::now();
        self.check_chans().await?;
        self.check_caconn_chans().await?;
        if tsnow.duration_since(self.last_status_print).unwrap_or(Duration::ZERO) >= Duration::from_millis(1000) {
            self.last_status_print = tsnow;
            info!(
                "{:8} {:8} {:8} : {:8} {:8} : {:10}",
                self.count_unknown_address,
                self.count_search_pending,
                self.count_no_address,
                self.count_unassigned,
                self.count_assigned,
                self.insert_queue_counter.load(atomic::Ordering::Acquire),
            );
        }
        Ok(())
    }

    fn handle_channel_add(&mut self, ch: Channel) -> Result<(), Error> {
        if !self.channel_states.contains_key(&ch) {
            let st = ChannelState {
                value: ChannelStateValue::Active(ActiveChannelState::UnknownAddress),
                pending_op: None,
            };
            self.channel_states.insert(ch, st);
        }
        Ok(())
    }

    fn handle_channel_remove(&mut self, ch: Channel) -> Result<(), Error> {
        if let Some(k) = self.channel_states.get_mut(&ch) {
            match &k.value {
                ChannelStateValue::Active(j) => match j {
                    ActiveChannelState::UnknownAddress => {
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
                    ActiveChannelState::NoAddress => {
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
        match item {
            Ok(a) => {
                for res in a {
                    if let Some(addr) = &res.addr {
                        let addr = addr.clone();
                        let ch = Channel::new(res.channel);
                        if let Some(st) = self.channel_states.get_mut(&ch) {
                            if let ChannelStateValue::Active(ActiveChannelState::SearchPending { since }) = &st.value {
                                let dt = SystemTime::now().duration_since(*since).unwrap();
                                if dt > Duration::from_millis(TIMEOUT_WARN as u64) {
                                    warn!(
                                        "    FOUND {:5.0}  {:5.0}  {addr}",
                                        1e3 * dt.as_secs_f32(),
                                        1e3 * res.dt.as_secs_f32()
                                    );
                                }
                                let stnew = ChannelStateValue::Active(ActiveChannelState::WithAddress {
                                    addr,
                                    state: WithAddressState::Unassigned {
                                        assign_at: SystemTime::now(),
                                    },
                                });
                                st.value = stnew;
                                if let (Some(response_addr),) = (res.response_addr,) {
                                    let job = AddrInsertJob {
                                        backend: self.opts.backend().into(),
                                        channel: ch.clone(),
                                        response_addr: response_addr,
                                        addr: addr,
                                    };
                                    self.ioc_addr_inserter_tx.send(job).await?;
                                }
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
                        //debug!("no addr from search in {res:?}");
                        let ch = Channel::new(res.channel);
                        if let Some(st) = self.channel_states.get_mut(&ch) {
                            if let ChannelStateValue::Active(ActiveChannelState::SearchPending { since }) = &st.value {
                                let dt = SystemTime::now().duration_since(*since).unwrap();
                                if dt > Duration::from_millis(TIMEOUT_WARN as u64) {
                                    warn!(
                                        "NOT FOUND {:5.0}  {:5.0}",
                                        1e3 * dt.as_secs_f32(),
                                        1e3 * res.dt.as_secs_f32()
                                    );
                                }
                                st.value = ChannelStateValue::Active(ActiveChannelState::NoAddress);
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
                error!("error from search: {e}");
            }
        }
        Ok(())
    }

    async fn handle_event(&mut self, item: DaemonEvent) -> Result<(), Error> {
        use DaemonEvent::*;
        match item {
            TimerTick => self.handle_timer_tick().await,
            ChannelAdd(ch) => self.handle_channel_add(ch),
            ChannelRemove(ch) => self.handle_channel_remove(ch),
            SearchDone(item) => self.handle_search_done(item).await,
            CaConnEvent(item) => {
                use netfetch::ca::conn::CaConnEventValue::*;
                match item.value {
                    None => {
                        // TODO count, maybe reduce.
                        Ok(())
                    }
                    EchoTimeout => {
                        error!("TODO on EchoTimeout remove the CaConn and reset channels");
                        Ok(())
                    }
                }
            }
        }
    }

    pub async fn daemon(&mut self) -> Result<(), Error> {
        let ticker = {
            let tx = self.tx.clone();
            async move {
                let mut ticker = tokio::time::interval(Duration::from_millis(100));
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    ticker.tick().await;
                    if let Err(e) = tx.send(DaemonEvent::TimerTick).await {
                        error!("can not send TimerTick {e}");
                        break;
                    }
                }
            }
        };
        taskrun::spawn(ticker);
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
        warn!("TODO wait for IOC finder properly");
        let _ = &self.ioc_finder_jh;
        warn!("TODO wait for insert workers");
        let _ = &self.insert_workers_jh;
        let _ = &self.ioc_addr_inserter_jh;
        Ok(())
    }
}

pub async fn run(opts: CaIngestOpts, channels: Vec<String>) -> Result<(), Error> {
    info!("start up {opts:?}");
    let mut search_tgts = Vec::new();
    for s in opts.search() {
        let addr: SocketAddrV4 = s.parse()?;
        search_tgts.push(addr);
    }
    info!("parsed search_tgts {search_tgts:?}");
    let opts2 = DaemonOpts {
        backend: opts.backend().into(),
        local_epics_hostname: opts.local_epics_hostname().into(),
        array_truncate: opts.array_truncate(),
        insert_item_queue_cap: opts.insert_item_queue_cap(),
        pgconf: opts.postgresql().clone(),
        scyconf: opts.scylla().clone(),
        search_tgts,
        ttls: Ttls {
            index: opts.ttl_index(),
            d0: opts.ttl_d0(),
            d1: opts.ttl_d1(),
        },
    };
    let mut daemon = Daemon::new(opts2).await?;
    let tx = daemon.tx.clone();
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
    Ok(())
}
