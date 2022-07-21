pub mod conn;
pub mod proto;
pub mod search;
pub mod store;

use self::store::DataStore;
use crate::ca::conn::ConnCommand;
use crate::store::{CommonInsertItemQueue, QueryItem};
use async_channel::Sender;
use conn::CaConn;
use err::Error;
use futures_util::StreamExt;
use log::*;
use netpod::{Database, ScyllaConfig};
use scylla::batch::Consistency;
use serde::{Deserialize, Serialize};
use stats::{CaConnStats, CaConnStatsAgg, CaConnStatsAggDiff};
use std::collections::{BTreeMap, VecDeque};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Once};
use std::time::Duration;
use tokio::fs::OpenOptions;
use tokio::io::AsyncReadExt;
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
    channels: Vec<String>,
    search: Vec<String>,
    addr_bind: Ipv4Addr,
    addr_conn: Ipv4Addr,
    whitelist: String,
    blacklist: String,
    max_simul: Option<usize>,
    timeout: Option<u64>,
    #[serde(default)]
    abort_after_search: u32,
    pgconf: Database,
    scyconf: ScyllaConfig,
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
    let re_p = regex::Regex::new(&conf.whitelist)?;
    let re_n = regex::Regex::new(&conf.blacklist)?;
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
        channels: conf.channels,
        search: conf.search,
        addr_bind: conf.addr_bind,
        addr_conn: conf.addr_conn,
        timeout: conf.timeout.unwrap_or(2000),
        abort_after_search: conf.abort_after_search,
        pgconf: conf.pgconf,
        scyconf: conf.scyconf,
        array_truncate: conf.array_truncate.unwrap_or(512),
        insert_worker_count: conf.insert_worker_count.unwrap_or(8),
        insert_scylla_sessions: conf.insert_scylla_sessions.unwrap_or(1),
        insert_queue_max: conf.insert_queue_max.unwrap_or(32),
        insert_item_queue_cap: conf.insert_item_queue_cap.unwrap_or(380000),
        api_bind: conf.api_bind.unwrap_or_else(|| "0.0.0.0:3011".into()),
        local_epics_hostname: conf.local_epics_hostname,
    })
}

pub struct CaConnectOpts {
    pub channels: Vec<String>,
    pub search: Vec<String>,
    pub addr_bind: Ipv4Addr,
    pub addr_conn: Ipv4Addr,
    pub timeout: u64,
    pub abort_after_search: u32,
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
    insert_item_queue: &CommonInsertItemQueue,
    insert_frac: Arc<AtomicU64>,
    pg_client: Arc<PgClient>,
    store_stats: Arc<stats::CaConnStats>,
) -> Result<(), Error> {
    let mut data_stores = vec![];
    for _ in 0..insert_scylla_sessions {
        let scy = scylla::SessionBuilder::new()
            .known_nodes(&scyconf.hosts)
            .default_consistency(Consistency::One)
            .use_keyspace(&scyconf.keyspace, true)
            .build()
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let scy = Arc::new(scy);
        let data_store = Arc::new(DataStore::new(pg_client.clone(), scy.clone()).await?);
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
                            (item.series & 0xff) as i32,
                            item.series as i64,
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
                            (item.series & 0xff) as i32,
                            item.series as i64,
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
        };
        tokio::spawn(fut);
    }
    Ok(())
}

pub struct CommandQueueSet {
    queues: tokio::sync::Mutex<VecDeque<Sender<ConnCommand>>>,
}

impl CommandQueueSet {
    pub fn new() -> Self {
        Self {
            queues: tokio::sync::Mutex::new(VecDeque::<Sender<ConnCommand>>::new()),
        }
    }

    pub fn queues(&self) -> &tokio::sync::Mutex<VecDeque<Sender<ConnCommand>>> {
        &self.queues
    }
}

pub async fn ca_connect(opts: ListenFromFileOpts) -> Result<(), Error> {
    let facility = "scylla";
    let insert_frac = Arc::new(AtomicU64::new(1000));
    let insert_ivl_min = Arc::new(AtomicU64::new(8800));
    let opts = parse_config(opts.config).await?;
    let scyconf = opts.scyconf.clone();

    let command_queue_set = Arc::new(CommandQueueSet::new());
    tokio::spawn(crate::metrics::start_metrics_service(
        opts.api_bind.clone(),
        insert_frac.clone(),
        insert_ivl_min.clone(),
        command_queue_set.clone(),
    ));
    let d = Database {
        name: opts.pgconf.name.clone(),
        host: opts.pgconf.host.clone(),
        port: opts.pgconf.port.clone(),
        user: opts.pgconf.user.clone(),
        pass: opts.pgconf.pass.clone(),
    };
    let (pg_client, pg_conn) = tokio_postgres::connect(
        &format!("postgresql://{}:{}@{}:{}/{}", d.user, d.pass, d.host, d.port, d.name),
        tokio_postgres::tls::NoTls,
    )
    .await
    .unwrap();
    // TODO allow clean shutdown on ctrl-c and join the pg_conn in the end:
    tokio::spawn(pg_conn);
    let pg_client = Arc::new(pg_client);

    let scy = scylla::SessionBuilder::new()
        .known_nodes(&scyconf.hosts)
        .default_consistency(Consistency::One)
        .use_keyspace(scyconf.keyspace, true)
        .build()
        .await
        .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
    let scy = Arc::new(scy);
    // TODO use new struct:
    let local_stats = Arc::new(CaConnStats::new());
    // TODO factor the find loop into a separate Stream.
    let qu_find_addr = pg_client
        .prepare("with q1 as (select t1.facility, t1.channel, t1.addr from ioc_by_channel t1 where t1.facility = $1 and t1.channel in ($2, $3, $4, $5, $6, $7, $8, $9) and t1.addr != '' order by t1.tsmod desc) select distinct on (q1.facility, q1.channel) q1.facility, q1.channel, q1.addr from q1")
        .await
        .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
    let mut channels_by_host = BTreeMap::new();
    let mut chns_todo = &opts.channels[..];
    let mut chstmp = ["__NONE__"; 8];
    let mut ix = 0;
    while chns_todo.len() > 0 {
        for (s1, s2) in chns_todo.iter().zip(chstmp.iter_mut()) {
            *s2 = s1;
        }
        chns_todo = &chns_todo[chstmp.len().min(chns_todo.len())..];
        let rows = pg_client
            .query(
                &qu_find_addr,
                &[
                    &facility, &chstmp[0], &chstmp[1], &chstmp[2], &chstmp[3], &chstmp[4], &chstmp[5], &chstmp[6],
                    &chstmp[7],
                ],
            )
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("pg lookup error: {e:?}")))?;
        if rows.is_empty() {
            error!("can not find any addresses of channels {:?}", chstmp);
        } else {
            for row in rows {
                let ch: &str = row.get(1);
                let addr: &str = row.get(2);
                if addr == "" {
                    // TODO the address was searched before but could not be found.
                } else {
                    let addr: SocketAddrV4 = match addr.parse() {
                        Ok(k) => {
                            local_stats.ioc_lookup_inc();
                            k
                        }
                        Err(e) => {
                            error!("can not parse {addr:?} for channel {ch:?}  {e:?}");
                            continue;
                        }
                    };
                    ix += 1;
                    if ix % 1000 == 0 {
                        info!("{} of {}   {}  {:?}", ix, opts.channels.len(), ch, addr);
                    }
                    if !channels_by_host.contains_key(&addr) {
                        channels_by_host.insert(addr, vec![ch.to_string()]);
                    } else {
                        channels_by_host.get_mut(&addr).unwrap().push(ch.to_string());
                    }
                }
            }
        }
    }
    if opts.abort_after_search == 1 {
        return Ok(());
    }
    let data_store = Arc::new(DataStore::new(pg_client.clone(), scy.clone()).await?);
    let insert_item_queue = CommonInsertItemQueue::new(opts.insert_item_queue_cap);

    // TODO use a new stats struct
    let store_stats = Arc::new(CaConnStats::new());

    spawn_scylla_insert_workers(
        opts.scyconf.clone(),
        opts.insert_scylla_sessions,
        opts.insert_worker_count,
        &insert_item_queue,
        insert_frac.clone(),
        pg_client.clone(),
        store_stats.clone(),
    )
    .await?;

    let mut conn_jhs = vec![];
    let mut conn_stats = vec![];
    info!("channels_by_host len {}", channels_by_host.len());
    for (host, channels) in channels_by_host {
        let data_store = data_store.clone();
        let addr = SocketAddrV4::new(host.ip().clone(), host.port());
        let mut conn = CaConn::new(
            addr,
            opts.local_epics_hostname.clone(),
            data_store.clone(),
            insert_item_queue.sender(),
            opts.array_truncate,
            opts.insert_queue_max,
            insert_ivl_min.clone(),
        );
        conn_stats.push(conn.stats());
        for c in channels {
            conn.channel_add(c);
        }
        let stats2 = conn.stats();
        let conn_command_tx = conn.conn_command_tx();
        command_queue_set.queues().lock().await.push_back(conn_command_tx);
        let conn_block = async move {
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
    }
    let mut agg_last = CaConnStatsAgg::new();
    loop {
        tokio::time::sleep(Duration::from_millis(671)).await;
        let agg = CaConnStatsAgg::new();
        agg.push(&local_stats);
        agg.push(&store_stats);
        for g in &conn_stats {
            agg.push(&g);
        }
        let m = get_metrics();
        *m = Some(agg.clone());
        if false {
            let diff = CaConnStatsAggDiff::diff_from(&agg_last, &agg);
            info!("{}", diff.display());
        }
        for _s1 in &conn_stats {}
        agg_last = agg;
        if false {
            break;
        }
    }
    for jh in conn_jhs {
        match jh.await {
            Ok(k) => match k {
                Ok(_) => {}
                Err(e) => {
                    error!("{e:?}");
                }
            },
            Err(e) => {
                error!("{e:?}");
            }
        }
    }
    Ok(())
}
