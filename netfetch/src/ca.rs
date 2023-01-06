pub mod conn;
pub mod connset;
pub mod findioc;
pub mod proto;
pub mod search;
pub mod store;

use self::store::DataStore;
use crate::ca::conn::ConnCommand;
use crate::ca::connset::CaConnSet;
use crate::conf::CaIngestOpts;
use crate::errconv::ErrConv;
use crate::insertworker::spawn_scylla_insert_workers;
use crate::metrics::{metrics_agg_task, ExtraInsertsConf};
use crate::rt::TokMx;
use crate::store::CommonInsertItemQueue;
use err::Error;
use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, StreamExt};
use log::*;
use netpod::Database;
use stats::{CaConnStats, CaConnStatsAgg};
use std::collections::BTreeMap;
use std::net::{SocketAddr, SocketAddrV4};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_postgres::Client as PgClient;

pub static SIGINT: AtomicU32 = AtomicU32::new(0);

lazy_static::lazy_static! {
    pub static ref METRICS: Mutex<Option<CaConnStatsAgg>> = Mutex::new(None);
}

pub struct IngestCommons {
    pub pgconf: Arc<Database>,
    pub backend: String,
    pub local_epics_hostname: String,
    pub insert_item_queue: Arc<CommonInsertItemQueue>,
    pub data_store: Arc<DataStore>,
    pub insert_ivl_min: Arc<AtomicU64>,
    pub extra_inserts_conf: TokMx<ExtraInsertsConf>,
    pub insert_frac: AtomicU64,
    pub store_workers_rate: AtomicU64,
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
            "select t1.facility, t1.channel, t1.addr from ioc_by_channel_log t1 where t1.facility = $1 and t1.channel = $2 and addr is not null order by tsmod desc limit 1",
        )
        .await
        .err_conv()?;
    let rows = pg_client.query(&qu_find_addr, &[&backend, &name]).await.err_conv()?;
    if rows.is_empty() {
        error!("can not find any addresses of channels {:?}", name);
        Err(Error::with_msg_no_trace(format!("no address for channel {}", name)))
    } else {
        for row in rows {
            match row.try_get::<_, &str>(2) {
                Ok(addr) => match addr.parse::<SocketAddrV4>() {
                    Ok(addr) => return Ok(Some(addr)),
                    Err(e) => {
                        error!("can not parse  {e:?}");
                        return Err(Error::with_msg_no_trace(format!("no address for channel {}", name)));
                    }
                },
                Err(e) => {
                    error!("can not find addr for {name}  {e:?}");
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
        .prepare("with q1 as (select t1.facility, t1.channel, t1.addr from ioc_by_channel_log t1 where t1.facility = $1 and t1.channel in ($2, $3, $4, $5, $6, $7, $8, $9) and t1.addr is not null order by t1.tsmod desc) select distinct on (q1.facility, q1.channel) q1.facility, q1.channel, q1.addr from q1")
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

pub async fn ca_connect(opts: CaIngestOpts, channels: &Vec<String>) -> Result<(), Error> {
    crate::linuxhelper::set_signal_handler()?;
    let extra_inserts_conf = TokMx::new(ExtraInsertsConf { copies: Vec::new() });
    let insert_ivl_min = Arc::new(AtomicU64::new(8800));
    let scyconf = opts.scylla().clone();
    let pgconf = opts.postgresql().clone();
    let d = &pgconf;
    let (pg_client, pg_conn) = tokio_postgres::connect(
        &format!("postgresql://{}:{}@{}:{}/{}", d.user, d.pass, d.host, d.port, d.name),
        tokio_postgres::tls::NoTls,
    )
    .await
    .err_conv()?;
    // TODO allow clean shutdown on ctrl-c and join the pg_conn in the end:
    tokio::spawn(pg_conn);
    let pg_client = Arc::new(pg_client);

    // TODO use a new type:
    let local_stats = Arc::new(CaConnStats::new());

    info!("fetch phonebook begin");
    // Fetch all addresses for all channels.
    let rows = pg_client
        .query(
            "select distinct on (facility, channel) channel, addr from ioc_by_channel_log where channel is not null and addr is not null order by facility, channel, tsmod desc",
            &[],
        )
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
    info!("fetch phonebook done");

    let mut channels_by_host = BTreeMap::new();

    let data_store = Arc::new(DataStore::new(&scyconf, pg_client.clone()).await?);
    let insert_item_queue = CommonInsertItemQueue::new(opts.insert_item_queue_cap());
    let insert_item_queue = Arc::new(insert_item_queue);

    let ingest_commons = IngestCommons {
        pgconf: Arc::new(pgconf.clone()),
        backend: opts.backend().into(),
        local_epics_hostname: opts.local_epics_hostname().clone(),
        insert_item_queue: insert_item_queue.clone(),
        data_store: data_store.clone(),
        insert_ivl_min: insert_ivl_min.clone(),
        extra_inserts_conf,
        store_workers_rate: AtomicU64::new(opts.store_workers_rate()),
        insert_frac: AtomicU64::new(opts.insert_frac()),
        ca_conn_set: CaConnSet::new(),
    };
    let ingest_commons = Arc::new(ingest_commons);

    // TODO use a new stats type:
    let store_stats = Arc::new(CaConnStats::new());
    let ttls = crate::insertworker::Ttls {
        index: opts.ttl_index(),
        d0: opts.ttl_d0(),
        d1: opts.ttl_d1(),
    };
    let jh_insert_workers = spawn_scylla_insert_workers(
        opts.scylla().clone(),
        opts.insert_scylla_sessions(),
        opts.insert_worker_count(),
        insert_item_queue.clone(),
        ingest_commons.clone(),
        pg_client.clone(),
        store_stats.clone(),
        opts.use_rate_limit_queue(),
        ttls,
    )
    .await?;

    if true {
        tokio::spawn(crate::metrics::start_metrics_service(
            opts.api_bind().clone(),
            ingest_commons.clone(),
        ));
    }

    let metrics_agg_fut = metrics_agg_task(ingest_commons.clone(), local_stats.clone(), store_stats.clone());
    let metrics_agg_jh = tokio::spawn(metrics_agg_fut);

    let mut chns_todo = &channels[..];
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
                .add_channel_to_addr(
                    opts.backend().into(),
                    SocketAddr::V4(addr.clone()),
                    ch.clone(),
                    ingest_commons.clone(),
                )
                .await?;
        }
        ix += 1;
        if ix % 1000 == 0 {
            info!("{} of {}   {}", ix, channels.len(), ch);
        }
    }
    info!("channels_by_host len {}", channels_by_host.len());

    // Periodic tasks triggered by commands:
    let mut iper = 0;
    loop {
        if SIGINT.load(Ordering::Acquire) != 0 {
            break;
        }
        // TODO remove magic number, make adaptive:
        if ingest_commons.insert_item_queue.receiver().len() < 10000 {
            let addr = ingest_commons.ca_conn_set.addr_nth_mod(iper).await;
            if let Some(addr) = addr {
                //info!("channel info for addr {addr}");
                fn cmdgen() -> (ConnCommand, async_channel::Receiver<bool>) {
                    ConnCommand::check_channels_alive()
                }
                // TODO race between getting nth address and command send, so ignore error so far.
                let _res = ingest_commons.ca_conn_set.send_command_to_addr(&addr, cmdgen).await;
                let cmdgen = || ConnCommand::save_conn_info();
                // TODO race between getting nth address and command send, so ignore error so far.
                let _res = ingest_commons.ca_conn_set.send_command_to_addr(&addr, cmdgen).await;
            } else {
                //info!("nothing to save iper {iper}");
            }
            iper += 1;
        }
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
