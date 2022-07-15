pub mod conn;
pub mod proto;
pub mod store;

use self::conn::FindIocStream;
use self::store::DataStore;
use crate::store::{CommonInsertItemQueue, QueryItem};
use conn::CaConn;
use err::Error;
use futures_util::StreamExt;
use log::*;
use netpod::Database;
use scylla::batch::Consistency;
use serde::{Deserialize, Serialize};
use stats::{CaConnStats, CaConnStatsAgg, CaConnStatsAggDiff};
use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Once};
use std::time::{Duration, Instant};
use tokio::fs::OpenOptions;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio_postgres::Client as PgClient;

static mut METRICS: Option<Mutex<Option<CaConnStatsAgg>>> = None;
static METRICS_ONCE: Once = Once::new();

fn get_metrics() -> &'static mut Option<CaConnStatsAgg> {
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
    pg_pass: String,
    array_truncate: Option<usize>,
    insert_worker_count: Option<usize>,
    insert_scylla_sessions: Option<usize>,
    insert_queue_max: Option<usize>,
    insert_item_queue_cap: Option<usize>,
    api_bind: Option<String>,
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
    let re1 = regex::Regex::new(&conf.whitelist)?;
    let re2 = regex::Regex::new(&conf.blacklist)?;
    conf.channels = conf
        .channels
        .into_iter()
        .filter(|ch| {
            if let Some(_cs) = re1.captures(&ch) {
                //let m = cs.get(1).unwrap();
                true
            } else if re2.is_match(&ch) {
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
        pg_pass: conf.pg_pass,
        array_truncate: conf.array_truncate.unwrap_or(512),
        insert_worker_count: conf.insert_worker_count.unwrap_or(8),
        insert_scylla_sessions: conf.insert_scylla_sessions.unwrap_or(1),
        insert_queue_max: conf.insert_queue_max.unwrap_or(32),
        insert_item_queue_cap: conf.insert_item_queue_cap.unwrap_or(380000),
        api_bind: conf.api_bind.unwrap_or_else(|| "0.0.0.0:3011".into()),
    })
}

pub struct CaConnectOpts {
    pub channels: Vec<String>,
    pub search: Vec<String>,
    pub addr_bind: Ipv4Addr,
    pub addr_conn: Ipv4Addr,
    pub timeout: u64,
    pub abort_after_search: u32,
    pub pg_pass: String,
    pub array_truncate: usize,
    pub insert_worker_count: usize,
    pub insert_scylla_sessions: usize,
    pub insert_queue_max: usize,
    pub insert_item_queue_cap: usize,
    pub api_bind: String,
}

async fn resolve_address(addr_str: &str) -> Result<SocketAddrV4, Error> {
    const PORT_DEFAULT: u16 = 5064;
    let ac = match addr_str.parse::<SocketAddrV4>() {
        Ok(k) => k,
        Err(_) => match addr_str.parse::<Ipv4Addr>() {
            Ok(k) => SocketAddrV4::new(k, PORT_DEFAULT),
            Err(e) => match tokio::net::lookup_host(&addr_str).await {
                Ok(k) => {
                    let vs: Vec<_> = k
                        .filter_map(|x| match x {
                            SocketAddr::V4(k) => Some(k),
                            SocketAddr::V6(_) => None,
                        })
                        .collect();
                    if let Some(k) = vs.first() {
                        *k
                    } else {
                        error!("Can not understand name for {:?}  {:?}", addr_str, vs);
                        return Err(e.into());
                    }
                }
                Err(e) => {
                    error!("{e:?}");
                    return Err(e.into());
                }
            },
        },
    };
    Ok(ac)
}

pub async fn ca_search(opts: ListenFromFileOpts) -> Result<(), Error> {
    let facility = "scylla";
    let opts = parse_config(opts.config).await?;
    let d = Database {
        name: "daqbuffer".into(),
        host: "sf-nube-11".into(),
        port: 5432,
        user: "daqbuffer".into(),
        pass: opts.pg_pass.clone(),
    };
    let (pg_client, pg_conn) = tokio_postgres::connect(
        &format!("postgresql://{}:{}@{}:{}/{}", d.user, d.pass, d.host, 5432, d.name),
        tokio_postgres::tls::NoTls,
    )
    .await
    .unwrap();
    // TODO join pg_conn in the end:
    tokio::spawn(pg_conn);
    let pg_client = Arc::new(pg_client);
    let qu_insert = {
        const TEXT: tokio_postgres::types::Type = tokio_postgres::types::Type::TEXT;
        pg_client
            .prepare_typed(
                "insert into ioc_by_channel (facility, channel, searchaddr, addr) values ($1, $2, $3, $4)",
                &[TEXT, TEXT, TEXT, TEXT],
            )
            .await
            .unwrap()
    };
    let qu_select = pg_client
        .prepare("select addr from ioc_by_channel where facility = $1 and channel = $2 and searchaddr = $3")
        .await
        .unwrap();
    let qu_update = pg_client
        .prepare("update ioc_by_channel set addr = $4 where facility = $1 and channel = $2 and searchaddr = $3")
        .await
        .unwrap();
    let mut addrs = vec![];
    for s in &opts.search {
        let x = resolve_address(s).await?;
        addrs.push(x);
    }
    let mut finder = FindIocStream::new(addrs);
    for ch in &opts.channels {
        finder.push(ch.into());
    }
    let mut ts_last = Instant::now();
    loop {
        let ts_now = Instant::now();
        if ts_now.duration_since(ts_last) >= Duration::from_millis(1000) {
            ts_last = ts_now;
            info!("{}", finder.quick_state());
        }
        let k = tokio::time::timeout(Duration::from_millis(200), finder.next()).await;
        let item = match k {
            Ok(Some(k)) => k,
            Ok(None) => {
                info!("Search stream exhausted");
                break;
            }
            Err(_) => {
                continue;
            }
        };
        let item = match item {
            Ok(k) => k,
            Err(e) => {
                error!("ca_search {e:?}");
                continue;
            }
        };
        for item in item {
            let searchaddr = item.src.to_string();
            let addr = item.addr.map(|x| x.to_string()).unwrap_or(String::new());
            let rows = pg_client
                .query(&qu_select, &[&facility, &item.channel, &searchaddr])
                .await
                .unwrap();
            if rows.is_empty() {
                pg_client
                    .execute(&qu_insert, &[&facility, &item.channel, &searchaddr, &addr])
                    .await
                    .unwrap();
            } else {
                let addr2: &str = rows[0].get(0);
                if addr2 != addr {
                    pg_client
                        .execute(&qu_update, &[&facility, &item.channel, &searchaddr, &addr])
                        .await
                        .unwrap();
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    Ok(())
}

async fn spawn_scylla_insert_workers(
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
            .known_node("sf-nube-14:19042")
            .default_consistency(Consistency::One)
            .use_keyspace("ks1", true)
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

pub async fn ca_connect(opts: ListenFromFileOpts) -> Result<(), Error> {
    let facility = "scylla";
    let insert_frac = Arc::new(AtomicU64::new(1000));
    let insert_ivl_min = Arc::new(AtomicU64::new(8800));
    let opts = parse_config(opts.config).await?;
    tokio::spawn(start_metrics_service(
        opts.api_bind.clone(),
        insert_frac.clone(),
        insert_ivl_min.clone(),
    ));
    let d = Database {
        name: "daqbuffer".into(),
        host: "sf-nube-11".into(),
        port: 5432,
        user: "daqbuffer".into(),
        pass: opts.pg_pass.clone(),
    };
    let (pg_client, pg_conn) = tokio_postgres::connect(
        &format!("postgresql://{}:{}@{}:{}/{}", d.user, d.pass, d.host, 5432, d.name),
        tokio_postgres::tls::NoTls,
    )
    .await
    .unwrap();
    // TODO allow clean shutdown on ctrl-c and join the pg_conn in the end:
    tokio::spawn(pg_conn);
    let pg_client = Arc::new(pg_client);

    let scy = scylla::SessionBuilder::new()
        .known_node("sf-nube-14:19042")
        .default_consistency(Consistency::One)
        .use_keyspace("ks1", true)
        .build()
        .await
        .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
    let scy = Arc::new(scy);

    // TODO use new struct:
    let local_stats = Arc::new(CaConnStats::new());

    // TODO factor the find loop into a separate Stream.
    info!("FIND IOCS");
    let qu_find_addr = pg_client
        .prepare("select t2.channel, t2.addr from ioc_by_channel t1, ioc_by_channel t2 where t2.facility = t1.facility and t2.channel = t1.channel and t1.facility = $1 and t1.channel in ($2, $3, $4, $5, $6, $7, $8, $9)")
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
            .map_err(|e| Error::with_msg_no_trace(format!("PG error: {e:?}")))?;
        if rows.is_empty() {
            error!("can not find any addresses of channels {:?}", chstmp);
        } else {
            for row in rows {
                let ch: &str = row.get(0);
                let addr: &str = row.get(1);
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
        if false && host.ip() != &"172.26.24.76".parse::<Ipv4Addr>().unwrap() {
            continue;
        }
        let data_store = data_store.clone();
        //debug!("Create TCP connection to {:?}", (host.ip(), host.port()));
        let addr = SocketAddrV4::new(host.ip().clone(), host.port());
        // TODO establish the connection in the future SM.
        let tcp = match tokio::time::timeout(Duration::from_millis(500), TcpStream::connect(addr)).await {
            Ok(Ok(k)) => k,
            Ok(Err(e)) => {
                error!("Can not connect to {addr:?} {e:?}");
                continue;
            }
            Err(e) => {
                error!("Can not connect to {addr:?} {e:?}");
                continue;
            }
        };
        local_stats.tcp_connected_inc();
        let mut conn = CaConn::new(
            tcp,
            addr,
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
        let conn_block = async move {
            while let Some(item) = conn.next().await {
                match item {
                    Ok(_) => {
                        stats2.conn_item_count_inc();
                        // TODO test if performance can be noticed:
                        //trace!("CaConn gives item: {k:?}");
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

async fn start_metrics_service(bind_to: String, insert_frac: Arc<AtomicU64>, insert_ivl_min: Arc<AtomicU64>) {
    let app = axum::Router::new()
        .route(
            "/metrics",
            axum::routing::get(|| async {
                let stats = get_metrics();
                match stats {
                    Some(s) => {
                        trace!("Metrics");
                        s.prometheus()
                    }
                    None => {
                        trace!("Metrics empty");
                        String::new()
                    }
                }
            }),
        )
        .route(
            "/insert_frac",
            axum::routing::put(|v: axum::extract::Json<u64>| async move {
                insert_frac.store(v.0, Ordering::Release);
            }),
        )
        .route(
            "/insert_ivl_min",
            axum::routing::put(|v: axum::extract::Json<u64>| async move {
                insert_ivl_min.store(v.0, Ordering::Release);
            }),
        );
    axum::Server::bind(&bind_to.parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap()
}
