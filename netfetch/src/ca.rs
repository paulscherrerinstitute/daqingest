pub mod conn;
pub mod findioc;
pub mod proto;
pub mod search;
pub mod store;

use self::store::DataStore;
use crate::ca::conn::ConnCommand;
use crate::errconv::ErrConv;
use crate::linuxhelper::local_hostname;
use crate::store::{CommonInsertItemQueue, CommonInsertItemQueueSender, IntoSimplerError, QueryItem};
use async_channel::Sender;
use conn::CaConn;
use err::Error;
use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, StreamExt};
use log::*;
use netpod::{Database, ScyllaConfig};
use serde::{Deserialize, Serialize};
use stats::{CaConnStats, CaConnStatsAgg, CaConnStatsAggDiff};
use std::collections::{BTreeMap, VecDeque};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::fs::OpenOptions;
use tokio::io::AsyncReadExt;
use tokio::sync::Mutex as TokMx;
use tokio::task::JoinHandle;
use tokio_postgres::Client as PgClient;

pub static SIGINT: AtomicU32 = AtomicU32::new(0);

lazy_static::lazy_static! {
    pub static ref METRICS: Mutex<Option<CaConnStatsAgg>> = Mutex::new(None);
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
    local_epics_hostname: Option<String>,
}

#[test]
fn parse_config_minimal() {
    let conf = r###"
backend: scylla
api_bind: 0.0.0.0:3011
channels:
    - CHANNEL-1:A
    - CHANNEL-1:B
    - CHANNEL-2:A
search:
    - 172.26.0.255
    - 172.26.2.255
postgresql:
    host: host.example.com
    port: 5432
    user: USER
    pass: PASS
    name: NAME
scylla:
    hosts:
        - sf-nube-11:19042
        - sf-nube-12:19042
    keyspace: ks1
"###;
    let res: Result<ChannelConfig, _> = serde_yaml::from_slice(conf.as_bytes());
    assert_eq!(res.is_ok(), true);
    let conf = res.unwrap();
    assert_eq!(conf.api_bind, Some("0.0.0.0:3011".to_string()));
    assert_eq!(conf.search.get(0), Some(&"172.26.0.255".to_string()));
    assert_eq!(conf.scylla.hosts.get(1), Some(&"sf-nube-12:19042".to_string()));
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
        timeout: conf.timeout.unwrap_or(1200),
        pgconf: conf.postgresql,
        scyconf: conf.scylla,
        array_truncate: conf.array_truncate.unwrap_or(512),
        insert_worker_count: conf.insert_worker_count.unwrap_or(800),
        insert_scylla_sessions: conf.insert_scylla_sessions.unwrap_or(1),
        insert_queue_max: conf.insert_queue_max.unwrap_or(64),
        insert_item_queue_cap: conf.insert_item_queue_cap.unwrap_or(200000),
        api_bind: conf.api_bind.unwrap_or_else(|| "0.0.0.0:3011".into()),
        local_epics_hostname: conf.local_epics_hostname.unwrap_or_else(local_hostname),
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtraInsertsConf {
    pub copies: Vec<(u64, u64)>,
}

impl ExtraInsertsConf {
    pub fn new() -> Self {
        Self { copies: Vec::new() }
    }
}

fn stats_inc_for_err(stats: &stats::CaConnStats, err: &crate::store::Error) {
    use crate::store::Error;
    match err {
        Error::DbOverload => {
            stats.store_worker_insert_overload_inc();
        }
        Error::DbTimeout => {
            stats.store_worker_insert_timeout_inc();
        }
        Error::DbUnavailable => {
            stats.store_worker_insert_unavailable_inc();
        }
        Error::DbError(_) => {
            stats.store_worker_insert_error_inc();
        }
    }
}

fn back_off_next(backoff_dt: &mut Duration) {
    *backoff_dt = *backoff_dt + (*backoff_dt) * 3 / 2;
    let dtmax = Duration::from_millis(4000);
    if *backoff_dt > dtmax {
        *backoff_dt = dtmax;
    }
}

async fn back_off_sleep(backoff_dt: &mut Duration) {
    back_off_next(backoff_dt);
    tokio::time::sleep(*backoff_dt).await;
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
            let backoff_0 = Duration::from_millis(10);
            let mut backoff = backoff_0.clone();
            let mut i1 = 0;
            while let Ok(item) = recv.recv().await {
                stats.store_worker_item_recv_inc();
                match item {
                    QueryItem::ConnectionStatus(item) => {
                        match crate::store::insert_connection_status(item, &data_store, &stats).await {
                            Ok(_) => {
                                stats.store_worker_insert_done_inc();
                                backoff = backoff_0;
                            }
                            Err(e) => {
                                stats_inc_for_err(&stats, &e);
                                back_off_sleep(&mut backoff).await;
                            }
                        }
                    }
                    QueryItem::ChannelStatus(item) => {
                        match crate::store::insert_channel_status(item, &data_store, &stats).await {
                            Ok(_) => {
                                stats.store_worker_insert_done_inc();
                                backoff = backoff_0;
                            }
                            Err(e) => {
                                stats_inc_for_err(&stats, &e);
                                back_off_sleep(&mut backoff).await;
                            }
                        }
                    }
                    QueryItem::Insert(item) => {
                        let insert_frac = insert_frac.load(Ordering::Acquire);
                        if i1 % 1000 < insert_frac {
                            match crate::store::insert_item(item, &data_store, &stats).await {
                                Ok(_) => {
                                    stats.store_worker_insert_done_inc();
                                    backoff = backoff_0;
                                }
                                Err(e) => {
                                    stats_inc_for_err(&stats, &e);
                                    back_off_sleep(&mut backoff).await;
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
                        let qres = data_store.scy.execute(&data_store.qu_insert_muted, values).await;
                        match qres {
                            Ok(_) => {
                                stats.store_worker_insert_done_inc();
                                backoff = backoff_0;
                            }
                            Err(e) => {
                                let e = e.into_simpler();
                                stats_inc_for_err(&stats, &e);
                                back_off_sleep(&mut backoff).await;
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
                            .execute(&data_store.qu_insert_item_recv_ivl, values)
                            .await;
                        match qres {
                            Ok(_) => {
                                stats.store_worker_insert_done_inc();
                                backoff = backoff_0;
                            }
                            Err(e) => {
                                let e = e.into_simpler();
                                stats_inc_for_err(&stats, &e);
                                back_off_sleep(&mut backoff).await;
                            }
                        }
                    }
                    QueryItem::ChannelInfo(item) => {
                        let params = (
                            item.ts_msp as i32,
                            item.series.id() as i64,
                            item.ivl,
                            item.interest,
                            item.evsize as i32,
                        );
                        let qres = data_store.scy.execute(&data_store.qu_insert_channel_ping, params).await;
                        match qres {
                            Ok(_) => {
                                stats.store_worker_insert_done_inc();
                                backoff = backoff_0;
                            }
                            Err(e) => {
                                let e = e.into_simpler();
                                stats_inc_for_err(&stats, &e);
                                back_off_sleep(&mut backoff).await;
                            }
                        }
                    }
                }
            }
            trace!("insert worker has no more messages");
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

struct CaConnRess {
    sender: Sender<ConnCommand>,
    stats: Arc<CaConnStats>,
    jh: JoinHandle<Result<(), Error>>,
}

// TODO
// Resources belonging to the same CaConn also belong together here.
// Only add or remove them from the set at once.
// That means, they should go together.
// Does not hold the actual CaConn, because that struct is in a task.
// Always create the CaConn via a common code path which also takes care
// to add it to the correct list.
// There, make spawning part of this function?
pub struct CaConnSet {
    ca_conn_ress: TokMx<BTreeMap<SocketAddr, CaConnRess>>,
}

impl CaConnSet {
    pub fn new() -> Self {
        Self {
            ca_conn_ress: Default::default(),
        }
    }

    pub async fn create_ca_conn(
        &self,
        addr: SocketAddrV4,
        local_epics_hostname: String,
        array_truncate: usize,
        insert_queue_max: usize,
        insert_item_queue_sender: CommonInsertItemQueueSender,
        data_store: Arc<DataStore>,
        ingest_commons: Arc<IngestCommons>,
        with_channels: Vec<String>,
    ) -> Result<(), Error> {
        info!("create new CaConn  {:?}", addr);
        let addr2 = SocketAddr::V4(addr.clone());
        let mut conn = CaConn::new(
            addr,
            local_epics_hostname,
            data_store.clone(),
            insert_item_queue_sender,
            array_truncate,
            insert_queue_max,
            ingest_commons,
        );
        for ch in with_channels {
            conn.channel_add(ch);
        }
        let conn = conn;
        let conn_tx = conn.conn_command_tx();
        let conn_stats = conn.stats();
        let conn_fut = async move {
            let stats = conn.stats();
            let mut conn = conn;
            while let Some(item) = conn.next().await {
                match item {
                    Ok(_) => {
                        stats.conn_item_count_inc();
                    }
                    Err(e) => {
                        error!("CaConn gives error: {e:?}");
                        break;
                    }
                }
            }
            Ok::<_, Error>(())
        };
        let jh = tokio::spawn(conn_fut);
        let ca_conn_ress = CaConnRess {
            sender: conn_tx,
            stats: conn_stats,
            jh,
        };
        self.ca_conn_ress.lock().await.insert(addr2, ca_conn_ress);
        Ok(())
    }

    pub async fn send_command_to_all<F, R>(&self, cmdgen: F) -> Result<Vec<R>, Error>
    where
        F: Fn() -> (ConnCommand, async_channel::Receiver<R>),
    {
        //let it = self.ca_conn_ress.iter().map(|x| x);
        //Self::send_command_inner(it, move || cmd.clone());
        let mut rxs = Vec::new();
        for (_addr, ress) in &*self.ca_conn_ress.lock().await {
            let (cmd, rx) = cmdgen();
            match ress.sender.send(cmd).await {
                Ok(()) => {
                    rxs.push(rx);
                }
                Err(e) => {
                    error!("can not send command {e:?}");
                }
            }
        }
        let mut res = Vec::new();
        for rx in rxs {
            let x = rx.recv().await?;
            res.push(x);
        }
        Ok(res)
    }

    pub async fn send_command_to_addr<F, R>(&self, addr: &SocketAddr, cmdgen: F) -> Result<R, Error>
    where
        F: Fn() -> (ConnCommand, async_channel::Receiver<R>),
    {
        if let Some(ress) = self.ca_conn_ress.lock().await.get(addr) {
            let (cmd, rx) = cmdgen();
            ress.sender.send(cmd).await.err_conv()?;
            let ret = rx.recv().await.err_conv()?;
            Ok(ret)
        } else {
            Err(Error::with_msg_no_trace(format!("addr not found")))
        }
    }

    #[allow(unused)]
    async fn send_command_inner<'a, IT, F, R>(it: &mut IT, cmdgen: F) -> Vec<async_channel::Receiver<R>>
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

    pub async fn send_stop(&self) -> Result<(), Error> {
        self.send_command_to_all(|| ConnCommand::shutdown()).await?;
        Ok(())
    }

    pub async fn wait_stopped(&self) -> Result<(), Error> {
        let mut g = self.ca_conn_ress.lock().await;
        let mm = std::mem::replace(&mut *g, BTreeMap::new());
        let mut jhs: VecDeque<_> = VecDeque::new();
        for t in mm {
            jhs.push_back(t.1.jh.fuse());
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
                _b = tokio::time::sleep(Duration::from_millis(1000)).fuse() => {
                    jhs.push_back(jh);
                    info!("waiting for {} connections", jhs.len());
                }
            };
        }
        Ok(())
    }

    pub async fn add_channel_to_addr(
        &self,
        addr: SocketAddr,
        channel_name: String,
        ingest_commons: Arc<IngestCommons>,
    ) -> Result<(), Error> {
        let g = self.ca_conn_ress.lock().await;
        match g.get(&addr) {
            Some(ca_conn) => {
                //info!("try to add to existing... {addr} {channel_name}");
                let (cmd, rx) = ConnCommand::channel_add(channel_name);
                ca_conn.sender.send(cmd).await.err_conv()?;
                let a = rx.recv().await.err_conv()?;
                if a {
                    Ok(())
                } else {
                    Err(Error::with_msg_no_trace(format!("channel add failed")))
                }
            }
            None => {
                //info!("create new {addr} {channel_name}");
                drop(g);
                let addr = if let SocketAddr::V4(x) = addr {
                    x
                } else {
                    return Err(Error::with_msg_no_trace(format!("only ipv4 supported for IOC")));
                };
                // TODO use parameters:
                self.create_ca_conn(
                    addr,
                    ingest_commons.local_epics_hostname.clone(),
                    512,
                    200,
                    ingest_commons.insert_item_queue.sender().await,
                    ingest_commons.data_store.clone(),
                    ingest_commons.clone(),
                    vec![channel_name],
                )
                .await?;
                Ok(())
            }
        }
    }

    pub async fn has_addr(&self, addr: &SocketAddr) -> bool {
        // TODO only used to check on add-channel whether we want to add channel to conn, or create new conn.
        // TODO must do that atomic.
        self.ca_conn_ress.lock().await.contains_key(addr)
    }

    pub async fn addr_nth_mod(&self, n: usize) -> Option<SocketAddr> {
        let g = self.ca_conn_ress.lock().await;
        let u = g.len();
        let n = n % u;
        g.keys().take(n).last().map(Clone::clone)
    }
}

pub struct IngestCommons {
    pub pgconf: Arc<Database>,
    pub local_epics_hostname: String,
    pub insert_item_queue: Arc<CommonInsertItemQueue>,
    pub data_store: Arc<DataStore>,
    pub insert_frac: Arc<AtomicU64>,
    pub insert_ivl_min: Arc<AtomicU64>,
    pub extra_inserts_conf: Mutex<ExtraInsertsConf>,
    pub ca_conn_set: CaConnSet,
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
    tokio::spawn(async {
        pg_conn.await.unwrap();
        info!("drop pg conn after find_channel_addr");
    });
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

#[allow(unused)]
async fn query_addr_multiple(pg_client: &PgClient) -> Result<(), Error> {
    let backend: &String = err::todoval();
    // TODO factor the find loop into a separate Stream.
    let qu_find_addr = pg_client
        .prepare("with q1 as (select t1.facility, t1.channel, t1.addr from ioc_by_channel t1 where t1.facility = $1 and t1.channel in ($2, $3, $4, $5, $6, $7, $8, $9) and t1.addr != '' order by t1.tsmod desc) select distinct on (q1.facility, q1.channel) q1.facility, q1.channel, q1.addr from q1")
        .await
        .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
    let mut chns_todo: &[String] = err::todoval();
    let mut chstmp = ["__NONE__"; 8];
    for (s1, s2) in chns_todo.iter().zip(chstmp.iter_mut()) {
        *s2 = s1;
    }
    chns_todo = &chns_todo[chstmp.len().min(chns_todo.len())..];
    let rows = pg_client
        .query(
            &qu_find_addr,
            &[
                &backend, &chstmp[0], &chstmp[1], &chstmp[2], &chstmp[3], &chstmp[4], &chstmp[5], &chstmp[6],
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
    Ok(())
}

async fn metrics_agg_task(
    ingest_commons: Arc<IngestCommons>,
    local_stats: Arc<CaConnStats>,
    store_stats: Arc<CaConnStats>,
) -> Result<(), Error> {
    let mut agg_last = CaConnStatsAgg::new();
    loop {
        tokio::time::sleep(Duration::from_millis(671)).await;
        let agg = CaConnStatsAgg::new();
        agg.push(&local_stats);
        agg.push(&store_stats);
        {
            let conn_stats_guard = ingest_commons.ca_conn_set.ca_conn_ress.lock().await;
            for (_, g) in conn_stats_guard.iter() {
                agg.push(&g.stats);
            }
        }
        let mut m = METRICS.lock().unwrap();
        *m = Some(agg.clone());
        if false {
            let diff = CaConnStatsAggDiff::diff_from(&agg_last, &agg);
            info!("{}", diff.display());
        }
        agg_last = agg;
    }
}

pub async fn ca_connect(opts: ListenFromFileOpts) -> Result<(), Error> {
    crate::linuxhelper::set_signal_handler()?;
    let insert_frac = Arc::new(AtomicU64::new(1000));
    let extra_inserts_conf = Mutex::new(ExtraInsertsConf { copies: Vec::new() });
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

    // TODO use a new type:
    let local_stats = Arc::new(CaConnStats::new());

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
    // TODO use a new stats type:
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

    let ingest_commons = IngestCommons {
        pgconf: Arc::new(pgconf.clone()),
        local_epics_hostname: opts.local_epics_hostname.clone(),
        insert_item_queue: insert_item_queue.clone(),
        data_store: data_store.clone(),
        insert_ivl_min: insert_ivl_min.clone(),
        insert_frac,
        extra_inserts_conf,
        ca_conn_set: CaConnSet::new(),
    };
    let ingest_commons = Arc::new(ingest_commons);

    if true {
        tokio::spawn(crate::metrics::start_metrics_service(
            opts.api_bind.clone(),
            ingest_commons.clone(),
        ));
    }

    let metrics_agg_fut = metrics_agg_task(ingest_commons.clone(), local_stats.clone(), store_stats.clone());
    let metrics_agg_jh = tokio::spawn(metrics_agg_fut);

    let mut chns_todo = &opts.channels[..];
    let mut ix = 0;
    for ch in chns_todo {
        if SIGINT.load(Ordering::Acquire) != 0 {
            break;
        }
        let ch = ch.to_string();
        chns_todo = &chns_todo[1..];
        if let Some(addr) = phonebook.get(&ch) {
            if !channels_by_host.contains_key(&addr) {
                channels_by_host.insert(addr, vec![ch.to_string()]);
            } else {
                channels_by_host.get_mut(&addr).unwrap().push(ch.to_string());
            }
            ingest_commons
                .ca_conn_set
                .add_channel_to_addr(SocketAddr::V4(addr.clone()), ch.clone(), ingest_commons.clone())
                .await?;
        }
        ix += 1;
        if ix % 1000 == 0 {
            info!("{} of {}   {}", ix, opts.channels.len(), ch);
        }
    }
    info!("channels_by_host len {}", channels_by_host.len());

    // Periodic tasks triggered by commands:
    let mut iper = 0;
    loop {
        if SIGINT.load(Ordering::Acquire) != 0 {
            break;
        }
        let addr = ingest_commons.ca_conn_set.addr_nth_mod(iper).await;
        if let Some(addr) = addr {
            fn cmdgen() -> (ConnCommand, async_channel::Receiver<bool>) {
                ConnCommand::check_channels_alive()
            }
            // TODO race between getting nth address and command send, so ignore error so far.
            let _res = ingest_commons.ca_conn_set.send_command_to_addr(&addr, cmdgen).await;
            let cmdgen = || ConnCommand::save_conn_info();
            // TODO race between getting nth address and command send, so ignore error so far.
            let _res = ingest_commons.ca_conn_set.send_command_to_addr(&addr, cmdgen).await;
        }
        iper += 1;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    loop {
        if SIGINT.load(Ordering::Acquire) != 0 {
            if false {
                let receiver = insert_item_queue.receiver();
                let sc = receiver.sender_count();
                let rc = receiver.receiver_count();
                info!("item queue  senders {}  receivers {}", sc, rc);
            }
            info!("sending stop commands");
            ingest_commons.ca_conn_set.send_stop().await?;
            break;
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    ingest_commons.ca_conn_set.wait_stopped().await?;
    info!("all connections done.");

    insert_item_queue.drop_sender().await;

    drop(ingest_commons);
    metrics_agg_jh.abort();
    drop(metrics_agg_jh);

    if false {
        let sender = insert_item_queue.sender_raw().await;
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
    receiver.close();

    let mut futs = FuturesUnordered::from_iter(jh_insert_workers);
    loop {
        futures_util::select!(
            x = futs.next() => match x {
                Some(Ok(_)) => {}
                Some(Err(e)) => {
                    error!("error on shutdown: {e:?}");
                }
                None => break,
            },
            _ = tokio::time::sleep(Duration::from_millis(1000)).fuse() => {
                if true {
                    let sc = receiver.sender_count();
                    let rc = receiver.receiver_count();
                    info!("waiting  inserters {}  items {}  senders {}  receivers {}", futs.len(), receiver.len(), sc, rc);
                }
            }
        );
    }
    info!("all insert workers done.");
    Ok(())
}
