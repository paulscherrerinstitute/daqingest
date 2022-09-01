pub mod conn;
pub mod findioc;
pub mod proto;
pub mod search;
pub mod store;

use self::store::DataStore;
use crate::ca::conn::ConnCommand;
use crate::errconv::ErrConv;
use crate::store::{CommonInsertItemQueue, QueryItem};
use async_channel::Sender;
use conn::CaConn;
use err::Error;
use futures_util::future::Fuse;
use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, StreamExt};
use log::*;
use netpod::{Database, ScyllaConfig};
use serde::{Deserialize, Serialize};
use stats::{CaConnStats, CaConnStatsAgg, CaConnStatsAggDiff};
use std::collections::{BTreeMap, VecDeque};
use std::ffi::CStr;
use std::mem::MaybeUninit;
use std::net::{IpAddr, Ipv4Addr, SocketAddrV4};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Once};
use std::time::Duration;
use tokio::fs::OpenOptions;
use tokio::io::AsyncReadExt;
use tokio::sync::Mutex as TokMx;
use tokio::task::JoinHandle;
use tokio_postgres::Client as PgClient;

static mut METRICS: Option<Mutex<Option<CaConnStatsAgg>>> = None;
static METRICS_ONCE: Once = Once::new();

pub fn get_metrics() -> &'static mut Option<CaConnStatsAgg> {
    METRICS_ONCE.call_once(|| unsafe {
        METRICS = Some(Mutex::new(None));
    });
    let mut g = unsafe { METRICS.as_mut().unwrap().lock().unwrap() };
    let ret: &mut Option<CaConnStatsAgg> = &mut *g;
    let ret = unsafe { &mut *(ret as *mut _) };
    ret
}

#[derive(Debug, Serialize, Deserialize)]
struct ChannelConfig {
    backend: String,
    channels: Vec<String>,
    search: Vec<String>,
    #[serde(default)]
    search_blacklist: Vec<String>,
    #[serde(default)]
    tmp_remove: Vec<String>,
    addr_bind: Option<IpAddr>,
    addr_conn: Option<IpAddr>,
    whitelist: Option<String>,
    blacklist: Option<String>,
    max_simul: Option<usize>,
    timeout: Option<u64>,
    postgresql: Database,
    scylla: ScyllaConfig,
    array_truncate: Option<usize>,
    insert_worker_count: Option<usize>,
    insert_scylla_sessions: Option<usize>,
    insert_queue_max: Option<usize>,
    insert_item_queue_cap: Option<usize>,
    api_bind: Option<String>,
    local_epics_hostname: String,
}

pub struct ListenFromFileOpts {
    pub config: PathBuf,
}

pub async fn parse_config(config: PathBuf) -> Result<CaConnectOpts, Error> {
    let mut file = OpenOptions::new().read(true).open(config).await?;
    let mut buf = vec![];
    file.read_to_end(&mut buf).await?;
    let mut conf: ChannelConfig =
        serde_yaml::from_slice(&buf).map_err(|e| Error::with_msg_no_trace(format!("{:?}", e)))?;
    let re_p = regex::Regex::new(&conf.whitelist.unwrap_or("--nothing-whitelisted--".into()))?;
    let re_n = regex::Regex::new(&conf.blacklist.unwrap_or("--nothing-blacklisted--".into()))?;
    conf.channels = conf
        .channels
        .into_iter()
        .filter(|ch| {
            if let Some(_cs) = re_p.captures(&ch) {
                true
            } else if re_n.is_match(&ch) {
                false
            } else {
                true
            }
        })
        .collect();
    Ok(CaConnectOpts {
        backend: conf.backend,
        channels: conf.channels,
        search: conf.search,
        search_blacklist: conf.search_blacklist,
        addr_bind: conf.addr_bind.unwrap_or(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0))),
        addr_conn: conf.addr_conn.unwrap_or(IpAddr::V4(Ipv4Addr::new(255, 255, 255, 255))),
        timeout: conf.timeout.unwrap_or(2000),
        pgconf: conf.postgresql,
        scyconf: conf.scylla,
        array_truncate: conf.array_truncate.unwrap_or(512),
        insert_worker_count: conf.insert_worker_count.unwrap_or(800),
        insert_scylla_sessions: conf.insert_scylla_sessions.unwrap_or(1),
        insert_queue_max: conf.insert_queue_max.unwrap_or(64),
        insert_item_queue_cap: conf.insert_item_queue_cap.unwrap_or(200000),
        api_bind: conf.api_bind.unwrap_or_else(|| "0.0.0.0:3011".into()),
        local_epics_hostname: conf.local_epics_hostname,
    })
}

pub struct CaConnectOpts {
    pub backend: String,
    pub channels: Vec<String>,
    pub search: Vec<String>,
    pub search_blacklist: Vec<String>,
    pub addr_bind: IpAddr,
    pub addr_conn: IpAddr,
    pub timeout: u64,
    pub pgconf: Database,
    pub scyconf: ScyllaConfig,
    pub array_truncate: usize,
    pub insert_worker_count: usize,
    pub insert_scylla_sessions: usize,
    pub insert_queue_max: usize,
    pub insert_item_queue_cap: usize,
    pub api_bind: String,
    pub local_epics_hostname: String,
}

async fn spawn_scylla_insert_workers(
    scyconf: ScyllaConfig,
    insert_scylla_sessions: usize,
    insert_worker_count: usize,
    insert_item_queue: Arc<CommonInsertItemQueue>,
    insert_frac: Arc<AtomicU64>,
    pg_client: Arc<PgClient>,
    store_stats: Arc<stats::CaConnStats>,
) -> Result<Vec<JoinHandle<()>>, Error> {
    let mut jhs = Vec::new();
    let mut data_stores = Vec::new();
    for _ in 0..insert_scylla_sessions {
        let data_store = Arc::new(DataStore::new(&scyconf, pg_client.clone()).await?);
        data_stores.push(data_store);
    }
    for i1 in 0..insert_worker_count {
        let data_store = data_stores[i1 * data_stores.len() / insert_worker_count].clone();
        let stats = store_stats.clone();
        let recv = insert_item_queue.receiver();
        let insert_frac = insert_frac.clone();
        let fut = async move {
            let mut i1 = 0;
            while let Ok(item) = recv.recv().await {
                match item {
                    QueryItem::ConnectionStatus(item) => {
                        match crate::store::insert_connection_status(item, &data_store, &stats).await {
                            Ok(_) => {
                                stats.store_worker_item_insert_inc();
                            }
                            Err(e) => {
                                stats.store_worker_item_error_inc();
                                // TODO introduce more structured error variants.
                                if e.msg().contains("WriteTimeout") {
                                    tokio::time::sleep(Duration::from_millis(100)).await;
                                } else {
                                    // TODO back off but continue.
                                    error!("insert worker sees error: {e:?}");
                                    break;
                                }
                            }
                        }
                    }
                    QueryItem::ChannelStatus(item) => {
                        match crate::store::insert_channel_status(item, &data_store, &stats).await {
                            Ok(_) => {
                                stats.store_worker_item_insert_inc();
                            }
                            Err(e) => {
                                stats.store_worker_item_error_inc();
                                // TODO introduce more structured error variants.
                                if e.msg().contains("WriteTimeout") {
                                    tokio::time::sleep(Duration::from_millis(100)).await;
                                } else {
                                    // TODO back off but continue.
                                    error!("insert worker sees error: {e:?}");
                                    break;
                                }
                            }
                        }
                    }
                    QueryItem::Insert(item) => {
                        stats.store_worker_item_recv_inc();
                        let insert_frac = insert_frac.load(Ordering::Acquire);
                        if i1 % 1000 < insert_frac {
                            match crate::store::insert_item(item, &data_store, &stats).await {
                                Ok(_) => {
                                    stats.store_worker_item_insert_inc();
                                }
                                Err(e) => {
                                    stats.store_worker_item_error_inc();
                                    // TODO introduce more structured error variants.
                                    if e.msg().contains("WriteTimeout") {
                                        tokio::time::sleep(Duration::from_millis(100)).await;
                                    } else {
                                        // TODO back off but continue.
                                        error!("insert worker sees error: {e:?}");
                                        break;
                                    }
                                }
                            }
                        } else {
                            stats.store_worker_item_drop_inc();
                        }
                        i1 += 1;
                    }
                    QueryItem::Mute(item) => {
                        let values = (
                            (item.series.id() & 0xff) as i32,
                            item.series.id() as i64,
                            item.ts as i64,
                            item.ema,
                            item.emd,
                        );
                        let qres = data_store
                            .scy
                            .query(
                                "insert into muted (part, series, ts, ema, emd) values (?, ?, ?, ?, ?)",
                                values,
                            )
                            .await;
                        match qres {
                            Ok(_) => {}
                            Err(_) => {
                                stats.store_worker_item_error_inc();
                            }
                        }
                    }
                    QueryItem::Ivl(item) => {
                        let values = (
                            (item.series.id() & 0xff) as i32,
                            item.series.id() as i64,
                            item.ts as i64,
                            item.ema,
                            item.emd,
                        );
                        let qres = data_store
                            .scy
                            .query(
                                "insert into item_recv_ivl (part, series, ts, ema, emd) values (?, ?, ?, ?, ?)",
                                values,
                            )
                            .await;
                        match qres {
                            Ok(_) => {}
                            Err(_) => {
                                stats.store_worker_item_error_inc();
                            }
                        }
                    }
                }
            }
            info!("insert worker has no more messages");
        };
        let jh = tokio::spawn(fut);
        jhs.push(jh);
    }
    Ok(jhs)
}

pub struct CommandQueueSet {
    queues: tokio::sync::Mutex<BTreeMap<SocketAddrV4, Sender<ConnCommand>>>,
}

impl CommandQueueSet {
    pub fn new() -> Self {
        Self {
            queues: tokio::sync::Mutex::new(BTreeMap::<SocketAddrV4, Sender<ConnCommand>>::new()),
        }
    }

    pub async fn queues(&self) -> &tokio::sync::Mutex<BTreeMap<SocketAddrV4, Sender<ConnCommand>>> {
        &self.queues
    }

    pub async fn queues_locked(&self) -> tokio::sync::MutexGuard<BTreeMap<SocketAddrV4, Sender<ConnCommand>>> {
        let mut g = self.queues.lock().await;
        let mut rm = Vec::new();
        for (k, v) in g.iter() {
            if v.is_closed() {
                rm.push(*k);
            }
        }
        for x in rm {
            g.remove(&x);
        }
        g
    }
}

pub struct IngestCommons {
    pub pgconf: Arc<Database>,
    pub local_epics_hostname: String,
    pub insert_item_queue: Arc<CommonInsertItemQueue>,
    pub data_store: Arc<DataStore>,
    pub insert_ivl_min: Arc<AtomicU64>,
    pub conn_stats: Arc<TokMx<Vec<Arc<CaConnStats>>>>,
    pub command_queue_set: Arc<CommandQueueSet>,
}

pub async fn find_channel_addr(
    backend: String,
    name: String,
    pgconf: &Database,
) -> Result<Option<SocketAddrV4>, Error> {
    // TODO also here, provide a db pool.
    let d = pgconf;
    let (pg_client, pg_conn) = tokio_postgres::connect(
        &format!("postgresql://{}:{}@{}:{}/{}", d.user, d.pass, d.host, d.port, d.name),
        tokio_postgres::tls::NoTls,
    )
    .await
    .unwrap();
    // TODO allow clean shutdown on ctrl-c and join the pg_conn in the end:
    tokio::spawn(pg_conn);
    let pg_client = Arc::new(pg_client);
    let qu_find_addr = pg_client
        .prepare(
            "select t1.facility, t1.channel, t1.addr from ioc_by_channel t1 where t1.facility = $1 and t1.channel = $2",
        )
        .await
        .err_conv()?;
    let rows = pg_client.query(&qu_find_addr, &[&backend, &name]).await.err_conv()?;
    if rows.is_empty() {
        error!("can not find any addresses of channels {:?}", name);
        Err(Error::with_msg_no_trace(format!("no address for channel {}", name)))
    } else {
        for row in rows {
            let addr: &str = row.get(2);
            if addr == "" {
                return Err(Error::with_msg_no_trace(format!("no address for channel {}", name)));
            } else {
                match addr.parse::<SocketAddrV4>() {
                    Ok(addr) => return Ok(Some(addr)),
                    Err(e) => {
                        error!("can not parse  {e:?}");
                        return Err(Error::with_msg_no_trace(format!("no address for channel {}", name)));
                    }
                }
            }
        }
        Ok(None)
    }
}

pub async fn create_ca_conn(
    addr: SocketAddrV4,
    local_epics_hostname: String,
    array_truncate: usize,
    insert_queue_max: usize,
    insert_item_queue: Arc<CommonInsertItemQueue>,
    data_store: Arc<DataStore>,
    insert_ivl_min: Arc<AtomicU64>,
    conn_stats: Arc<TokMx<Vec<Arc<CaConnStats>>>>,
    command_queue_set: Arc<CommandQueueSet>,
) -> Result<JoinHandle<Result<(), Error>>, Error> {
    info!("create new CaConn  {:?}", addr);
    let data_store = data_store.clone();
    let conn = CaConn::new(
        addr,
        local_epics_hostname,
        data_store.clone(),
        insert_item_queue.sender(),
        array_truncate,
        insert_queue_max,
        insert_ivl_min.clone(),
    );
    conn_stats.lock().await.push(conn.stats());
    let stats2 = conn.stats();
    let conn_command_tx = conn.conn_command_tx();
    {
        //command_queue_set.queues().lock().await.insert(addr, conn_command_tx);
        command_queue_set.queues_locked().await.insert(addr, conn_command_tx);
    }
    let conn_block = async move {
        let mut conn = conn;
        while let Some(item) = conn.next().await {
            match item {
                Ok(_) => {
                    stats2.conn_item_count_inc();
                }
                Err(e) => {
                    error!("CaConn gives error: {e:?}");
                    break;
                }
            }
        }
        Ok::<_, Error>(())
    };
    let jh = tokio::spawn(conn_block);
    Ok(jh)
}

pub static SIGINT: AtomicU32 = AtomicU32::new(0);

fn handler_sigaction(_a: libc::c_int, _b: *const libc::siginfo_t, _c: *const libc::c_void) {
    SIGINT.store(1, Ordering::Release);
    let _ = unset_signal_handler();
}

fn set_signal_handler() -> Result<(), Error> {
    let mask: libc::sigset_t = unsafe { MaybeUninit::zeroed().assume_init() };
    let handler: libc::sighandler_t = handler_sigaction as *const libc::c_void as _;
    let act = libc::sigaction {
        sa_sigaction: handler,
        sa_mask: mask,
        sa_flags: 0,
        sa_restorer: None,
    };
    unsafe {
        let ec = libc::sigaction(libc::SIGINT, &act, std::ptr::null_mut());
        if ec != 0 {
            let errno = *libc::__errno_location();
            let msg = CStr::from_ptr(libc::strerror(errno));
            error!("error: {:?}", msg);
            return Err(Error::with_msg_no_trace(format!("can not set signal handler")));
        }
    }
    Ok(())
}

fn unset_signal_handler() -> Result<(), Error> {
    let mask: libc::sigset_t = unsafe { MaybeUninit::zeroed().assume_init() };
    let act = libc::sigaction {
        sa_sigaction: libc::SIG_DFL,
        sa_mask: mask,
        sa_flags: 0,
        sa_restorer: None,
    };
    unsafe {
        let ec = libc::sigaction(libc::SIGINT, &act, std::ptr::null_mut());
        if ec != 0 {
            let errno = *libc::__errno_location();
            let msg = CStr::from_ptr(libc::strerror(errno));
            error!("error: {:?}", msg);
            return Err(Error::with_msg_no_trace(format!("can not set signal handler")));
        }
    }
    Ok(())
}

pub async fn ca_connect(opts: ListenFromFileOpts) -> Result<(), Error> {
    set_signal_handler()?;
    let insert_frac = Arc::new(AtomicU64::new(1000));
    let insert_ivl_min = Arc::new(AtomicU64::new(8800));
    let opts = parse_config(opts.config).await?;
    let scyconf = opts.scyconf.clone();

    let pgconf = Database {
        name: opts.pgconf.name.clone(),
        host: opts.pgconf.host.clone(),
        port: opts.pgconf.port.clone(),
        user: opts.pgconf.user.clone(),
        pass: opts.pgconf.pass.clone(),
    };

    let d = &pgconf;
    let (pg_client, pg_conn) = tokio_postgres::connect(
        &format!("postgresql://{}:{}@{}:{}/{}", d.user, d.pass, d.host, d.port, d.name),
        tokio_postgres::tls::NoTls,
    )
    .await
    .unwrap();
    // TODO allow clean shutdown on ctrl-c and join the pg_conn in the end:
    tokio::spawn(pg_conn);
    let pg_client = Arc::new(pg_client);

    // TODO use new struct:
    let local_stats = Arc::new(CaConnStats::new());
    // TODO factor the find loop into a separate Stream.
    let qu_find_addr = pg_client
        .prepare("with q1 as (select t1.facility, t1.channel, t1.addr from ioc_by_channel t1 where t1.facility = $1 and t1.channel in ($2, $3, $4, $5, $6, $7, $8, $9) and t1.addr != '' order by t1.tsmod desc) select distinct on (q1.facility, q1.channel) q1.facility, q1.channel, q1.addr from q1")
        .await
        .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;

    // Fetch all addresses for all channels.
    let rows = pg_client
        .query("select channel, addr from ioc_by_channel", &[])
        .await
        .err_conv()?;
    let mut phonebook = BTreeMap::new();
    for row in rows {
        let channel: String = row.get(0);
        let addr: String = row.get(1);
        let addr: SocketAddrV4 = addr
            .parse()
            .map_err(|_| Error::with_msg_no_trace(format!("can not parse address {addr}")))?;
        phonebook.insert(channel, addr);
    }

    let mut channels_by_host = BTreeMap::new();

    let data_store = Arc::new(DataStore::new(&scyconf, pg_client.clone()).await?);
    let insert_item_queue = CommonInsertItemQueue::new(opts.insert_item_queue_cap);
    let insert_item_queue = Arc::new(insert_item_queue);
    // TODO use a new stats struct
    let store_stats = Arc::new(CaConnStats::new());
    let jh_insert_workers = spawn_scylla_insert_workers(
        opts.scyconf.clone(),
        opts.insert_scylla_sessions,
        opts.insert_worker_count,
        insert_item_queue.clone(),
        insert_frac.clone(),
        pg_client.clone(),
        store_stats.clone(),
    )
    .await?;

    let mut conn_jhs = vec![];
    let conn_stats: Arc<TokMx<Vec<Arc<CaConnStats>>>> = Arc::new(TokMx::new(Vec::new()));
    let command_queue_set = Arc::new(CommandQueueSet::new());

    let ingest_commons = IngestCommons {
        pgconf: Arc::new(pgconf.clone()),
        local_epics_hostname: opts.local_epics_hostname.clone(),
        insert_item_queue: insert_item_queue.clone(),
        data_store: data_store.clone(),
        insert_ivl_min: insert_ivl_min.clone(),
        conn_stats: conn_stats.clone(),
        command_queue_set: command_queue_set.clone(),
    };
    let ingest_commons = Arc::new(ingest_commons);

    if true {
        tokio::spawn(crate::metrics::start_metrics_service(
            opts.api_bind.clone(),
            insert_frac.clone(),
            insert_ivl_min.clone(),
            ingest_commons.clone(),
        ));
    }

    let metrics_agg_fut = {
        let conn_stats = conn_stats.clone();
        let local_stats = local_stats.clone();
        async move {
            let mut agg_last = CaConnStatsAgg::new();
            loop {
                tokio::time::sleep(Duration::from_millis(671)).await;
                let agg = CaConnStatsAgg::new();
                agg.push(&local_stats);
                agg.push(&store_stats);
                for g in conn_stats.lock().await.iter() {
                    agg.push(&g);
                }
                let m = get_metrics();
                *m = Some(agg.clone());
                if false {
                    let diff = CaConnStatsAggDiff::diff_from(&agg_last, &agg);
                    info!("{}", diff.display());
                }
                agg_last = agg;
                if false {
                    break;
                }
            }
        }
    };
    let metrics_agg_jh = tokio::spawn(metrics_agg_fut);

    let mut chns_todo = &opts.channels[..];
    let mut chstmp = ["__NONE__"; 8];
    let mut ix = 0;
    while chns_todo.len() > 0 && SIGINT.load(Ordering::Acquire) == 0 {
        if false {
            for (s1, s2) in chns_todo.iter().zip(chstmp.iter_mut()) {
                *s2 = s1;
            }
            chns_todo = &chns_todo[chstmp.len().min(chns_todo.len())..];
            let rows = pg_client
                .query(
                    &qu_find_addr,
                    &[
                        &opts.backend,
                        &chstmp[0],
                        &chstmp[1],
                        &chstmp[2],
                        &chstmp[3],
                        &chstmp[4],
                        &chstmp[5],
                        &chstmp[6],
                        &chstmp[7],
                    ],
                )
                .await
                .map_err(|e| Error::with_msg_no_trace(format!("pg lookup error: {e:?}")))?;
            for row in rows {
                let ch: &str = row.get(1);
                let addr: &str = row.get(2);
                if addr == "" {
                    // TODO the address was searched before but could not be found.
                } else {
                    let addr: SocketAddrV4 = match addr.parse() {
                        Ok(k) => k,
                        Err(e) => {
                            error!("can not parse {addr:?} for channel {ch:?}  {e:?}");
                            continue;
                        }
                    };
                    let _ = addr;
                }
            }
        }
        if let Some(ch) = chns_todo.first() {
            let ch = ch.clone();
            chns_todo = &chns_todo[1..];
            if let Some(addr) = phonebook.get(&ch) {
                if !channels_by_host.contains_key(&addr) {
                    channels_by_host.insert(addr, vec![ch.to_string()]);
                } else {
                    channels_by_host.get_mut(&addr).unwrap().push(ch.to_string());
                }
                let create_new = {
                    let g = command_queue_set.queues_locked().await;
                    if let Some(tx) = g.get(&addr) {
                        let (cmd, rx) = ConnCommand::channel_add(ch.to_string());
                        tx.send(cmd).await.unwrap();
                        if !rx.recv().await.unwrap() {
                            error!("Could not add channel: {}", ch);
                        }
                        false
                    } else {
                        true
                    }
                };
                if create_new {
                    info!("create new CaConn  {:?}  {:?}", addr, ch);
                    let data_store = data_store.clone();
                    let conn = CaConn::new(
                        addr.clone(),
                        opts.local_epics_hostname.clone(),
                        data_store.clone(),
                        insert_item_queue.sender(),
                        opts.array_truncate,
                        opts.insert_queue_max,
                        insert_ivl_min.clone(),
                    );
                    conn_stats.lock().await.push(conn.stats());
                    let stats2 = conn.stats();
                    let conn_command_tx = conn.conn_command_tx();
                    let tx = conn_command_tx.clone();
                    {
                        command_queue_set
                            .queues_locked()
                            .await
                            .insert(addr.clone(), conn_command_tx);
                    }
                    let conn_block = async move {
                        let mut conn = conn;
                        while let Some(item) = conn.next().await {
                            match item {
                                Ok(_) => {
                                    stats2.conn_item_count_inc();
                                }
                                Err(e) => {
                                    error!("CaConn gives error: {e:?}");
                                    break;
                                }
                            }
                        }
                        Ok::<_, Error>(())
                    };
                    let jh = tokio::spawn(conn_block);
                    conn_jhs.push(jh);
                    {
                        let (cmd, rx) = ConnCommand::channel_add(ch.to_string());
                        tx.send(cmd).await.unwrap();
                        if !rx.recv().await.unwrap() {
                            error!("Could not add channel: {}", ch);
                        }
                    }
                }
            }
            ix += 1;
            if ix % 1000 == 0 {
                info!("{} of {}   {}", ix, opts.channels.len(), ch);
            }
        }
    }
    info!("channels_by_host len {}", channels_by_host.len());

    let mut conn_jhs: VecDeque<Fuse<JoinHandle<Result<(), Error>>>> =
        conn_jhs.into_iter().map(|jh| jh.fuse()).collect();
    let mut sent_stop_commands = false;
    loop {
        if SIGINT.load(Ordering::Acquire) != 0 {
            if false {
                let receiver = insert_item_queue.receiver();
                let sc = receiver.sender_count();
                let rc = receiver.receiver_count();
                info!("item queue  senders {}  receivers {}", sc, rc);
            }
            if !sent_stop_commands {
                sent_stop_commands = true;
                info!("sending stop command");
                let queues = command_queue_set.queues_locked().await;
                for q in queues.iter() {
                    let (cmd, _rx) = ConnCommand::shutdown();
                    let _ = q.1.send(cmd).await;
                }
            }
        }
        let mut jh = if let Some(x) = conn_jhs.pop_front() {
            x
        } else {
            break;
        };
        futures_util::select! {
            a = jh => match a {
                Ok(k) => match k {
                    Ok(_) => {
                    }
                    Err(e) => {
                        error!("{e:?}");
                    }
                },
                Err(e) => {
                    error!("{e:?}");
                }
            },
            _b = tokio::time::sleep(Duration::from_millis(1000)).fuse() => {
                conn_jhs.push_back(jh);
                if sent_stop_commands {
                    info!("waiting for {} connections", conn_jhs.len());
                }
            }
        };
    }
    info!("all connections done.");

    drop(ingest_commons);
    metrics_agg_jh.abort();
    drop(metrics_agg_jh);

    if false {
        let sender = insert_item_queue.sender_raw();
        sender.close();
        let receiver = insert_item_queue.receiver();
        receiver.close();
    }
    if true {
        let receiver = insert_item_queue.receiver();
        let sc = receiver.sender_count();
        let rc = receiver.receiver_count();
        info!("item queue A  senders {}  receivers {}", sc, rc);
    }
    let receiver = insert_item_queue.receiver();
    drop(insert_item_queue);
    if true {
        let sc = receiver.sender_count();
        let rc = receiver.receiver_count();
        info!("item queue B  senders {}  receivers {}", sc, rc);
    }

    let mut futs = FuturesUnordered::from_iter(jh_insert_workers);
    loop {
        futures_util::select!(
            x = futs.next() => match x {
                Some(Ok(_)) => {
                    info!("waiting for {} inserts", futs.len());
                }
                Some(Err(e)) => {
                    error!("error on shutdown: {e:?}");
                }
                None => break,
            },
            _ = tokio::time::sleep(Duration::from_millis(1000)).fuse() => {
                info!("waiting for {} inserters", futs.len());
                if true {
                    let sc = receiver.sender_count();
                    let rc = receiver.receiver_count();
                    info!("item queue B  senders {}  receivers {}", sc, rc);
                }
            }
        );
    }
    info!("all insert workers done.");
    Ok(())
}
