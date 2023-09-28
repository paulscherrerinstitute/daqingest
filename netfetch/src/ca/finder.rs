use super::connset::CaConnSetEvent;
use super::connset::IocAddrQuery;
use super::connset::CURRENT_SEARCH_PENDING_MAX;
use super::connset::SEARCH_BATCH_MAX;
use super::search::ca_search_workers_start;
use crate::ca::findioc::FindIocRes;
use crate::ca::findioc::FindIocStream;
use crate::conf::CaIngestOpts;
use crate::daemon_common::DaemonEvent;
use async_channel::Receiver;
use async_channel::Sender;
use dbpg::conn::make_pg_client;
use dbpg::iocindex::IocItem;
use dbpg::iocindex::IocSearchIndexWorker;
use dbpg::postgres::Row as PgRow;
use err::Error;
use futures_util::FutureExt;
use futures_util::StreamExt;
use log::*;
use netpod::Database;
use stats::IocFinderStats;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::net::SocketAddrV4;
use std::sync::atomic;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use taskrun::tokio;
use tokio::task::JoinHandle;

const SEARCH_DB_PIPELINE_LEN: usize = 4;
const FINDER_JOB_QUEUE_LEN_MAX: usize = 10;
const FINDER_BATCH_SIZE: usize = 8;
const FINDER_IN_FLIGHT_MAX: usize = 800;
const FINDER_TIMEOUT: Duration = Duration::from_millis(100);

#[allow(unused)]
macro_rules! debug_batch {
    ($($arg:tt)*) => (if false {
        debug!($($arg)*);
    });
}

#[allow(unused)]
macro_rules! trace_batch {
    ($($arg:tt)*) => (if false {
        trace!($($arg)*);
    });
}

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

pub fn start_finder(
    tx: Sender<VecDeque<FindIocRes>>,
    backend: String,
    opts: CaIngestOpts,
    stats: Arc<IocFinderStats>,
) -> Result<(Sender<IocAddrQuery>, JoinHandle<Result<(), Error>>), Error> {
    let (qtx, qrx) = async_channel::bounded(CURRENT_SEARCH_PENDING_MAX);
    let jh = taskrun::spawn(finder_full(qrx, tx, backend, opts, stats));
    Ok((qtx, jh))
}

async fn finder_full(
    qrx: Receiver<IocAddrQuery>,
    tx: Sender<VecDeque<FindIocRes>>,
    backend: String,
    opts: CaIngestOpts,
    stats: Arc<IocFinderStats>,
) -> Result<(), Error> {
    let (tx1, rx1) = async_channel::bounded(20);
    let jh1 = taskrun::spawn(finder_worker(
        qrx,
        tx1,
        backend,
        opts.postgresql_config().clone(),
        stats.clone(),
    ));
    let jh2 = taskrun::spawn(finder_network_if_not_found(rx1, tx, opts.clone(), stats));
    jh1.await??;
    jh2.await??;
    Ok(())
}

async fn finder_worker(
    qrx: Receiver<IocAddrQuery>,
    tx: Sender<VecDeque<FindIocRes>>,
    backend: String,
    db: Database,
    stats: Arc<IocFinderStats>,
) -> Result<(), Error> {
    // TODO do something with join handle
    let (batch_rx, jh) = batchtools::batcher::batch(
        SEARCH_BATCH_MAX,
        Duration::from_millis(200),
        SEARCH_DB_PIPELINE_LEN,
        qrx,
    );
    for _ in 0..SEARCH_DB_PIPELINE_LEN {
        // TODO use join handle
        tokio::spawn(finder_worker_single(
            batch_rx.clone(),
            tx.clone(),
            backend.clone(),
            db.clone(),
            stats.clone(),
        ));
    }
    Ok(())
}

async fn finder_worker_single(
    inp: Receiver<Vec<IocAddrQuery>>,
    tx: Sender<VecDeque<FindIocRes>>,
    backend: String,
    db: Database,
    stats: Arc<IocFinderStats>,
) -> Result<(), Error> {
    let (pg, jh) = make_pg_client(&db)
        .await
        .map_err(|e| Error::with_msg_no_trace(e.to_string()))?;
    let sql = concat!(
        "with q1 as (select * from unnest($2::text[]) as unn (ch))",
        " select distinct on (tt.facility, tt.channel) tt.channel, tt.addr",
        " from ioc_by_channel_log tt join q1 on tt.channel = q1.ch and tt.facility = $1 and tt.addr is not null",
        " order by tt.facility, tt.channel, tsmod desc",
    );
    let qu_select_multi = pg
        .prepare(sql)
        .await
        .map_err(|e| Error::with_msg_no_trace(e.to_string()))?;
    let mut resdiff = 0;
    loop {
        match inp.recv().await {
            Ok(batch) => {
                stats.dbsearcher_batch_recv().inc();
                stats.dbsearcher_item_recv().add(batch.len() as _);
                let ts1 = Instant::now();
                debug_batch!("run  query batch  len {}", batch.len());
                let names: Vec<_> = batch.iter().filter(|x| x.use_cache()).map(|x| x.name()).collect();
                let qres = pg.query(&qu_select_multi, &[&backend, &names]).await;
                let dt = ts1.elapsed();
                debug_batch!(
                    "done query batch  len {}: {}  {:.3}ms",
                    batch.len(),
                    qres.is_ok(),
                    dt.as_secs_f32() * 1e3
                );
                if dt > Duration::from_millis(5000) {
                    let mut out = String::from("[");
                    for e in &batch {
                        if out.len() > 1 {
                            out.push_str(", ");
                        }
                        out.push('\'');
                        out.push_str(e.name());
                        out.push('\'');
                    }
                    out.push(']');
                    trace!("very slow query\n{out}");
                }
                match qres {
                    Ok(rows) => {
                        stats.dbsearcher_select_res_0().add(rows.len() as _);
                        if rows.len() > batch.len() {
                            stats.dbsearcher_select_error_len_mismatch().inc();
                        } else if rows.len() < batch.len() {
                            resdiff += batch.len() - rows.len();
                        }
                        let nbatch = batch.len();
                        trace_batch!("received results {}  resdiff {}", rows.len(), resdiff);
                        let items = transform_pgres(rows);
                        let names: HashMap<_, _> = items.iter().map(|x| (&x.channel, true)).collect();
                        let mut to_add = Vec::new();
                        for e in batch {
                            if !names.contains_key(e.name_string()) {
                                let item = FindIocRes {
                                    channel: e.name().into(),
                                    response_addr: None,
                                    addr: None,
                                    dt: Duration::from_millis(0),
                                };
                                to_add.push(item);
                            }
                        }
                        let mut items = items;
                        items.extend(to_add.into_iter());
                        let items = items;
                        for e in &items {
                            if crate::ca::connset::trigger.contains(&e.channel.as_str()) {
                                debug!("found in database: {e:?}");
                            }
                        }
                        let items_len = items.len();
                        if items_len != nbatch {
                            stats.dbsearcher_select_error_len_mismatch().inc();
                        }
                        match tx.send(items).await {
                            Ok(_) => {
                                stats.dbsearcher_batch_send().inc();
                                stats.dbsearcher_item_send().add(items_len as _);
                            }
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
            Err(_e) => break,
        }
    }
    debug!("finder_worker_single done");
    jh.await?.map_err(|e| Error::from_string(e))?;
    Ok(())
}

async fn finder_network_if_not_found(
    mut rx: Receiver<VecDeque<FindIocRes>>,
    tx: Sender<VecDeque<FindIocRes>>,
    opts: CaIngestOpts,
    stats: Arc<IocFinderStats>,
) -> Result<(), Error> {
    let (net_tx, net_rx, jh, jhs) = ca_search_workers_start(&opts, stats.clone()).await.unwrap();
    let jh2 = taskrun::spawn(process_net_result(net_rx, tx.clone(), opts.clone()));
    'outer: while let Some(item) = rx.next().await {
        let mut res = VecDeque::new();
        let mut net = VecDeque::new();
        for e in item {
            if e.addr.is_none() {
                net.push_back(e.channel);
            } else {
                res.push_back(e);
            }
        }
        if let Err(_) = tx.send(res).await {
            break;
        }
        for ch in net {
            if let Err(_) = net_tx.send(ch).await {
                break 'outer;
            }
        }
    }
    for jh in jhs {
        jh.await??;
    }
    jh.await??;
    jh2.await??;
    debug!("finder_network_if_not_found done");
    Ok(())
}

async fn process_net_result(
    mut net_rx: Receiver<Result<VecDeque<FindIocRes>, Error>>,
    tx: Sender<VecDeque<FindIocRes>>,
    opts: CaIngestOpts,
) -> Result<(), Error> {
    const IOC_SEARCH_INDEX_WORKER_COUNT: usize = 1;
    let (dbtx, dbrx) = async_channel::bounded(64);
    let mut ioc_search_index_worker_jhs = Vec::new();
    let mut index_worker_pg_jh = Vec::new();
    for _ in 0..IOC_SEARCH_INDEX_WORKER_COUNT {
        let backend = opts.backend().into();
        let (pg, jh) = dbpg::conn::make_pg_client(opts.postgresql_config()).await.unwrap();
        index_worker_pg_jh.push(jh);
        let worker = IocSearchIndexWorker::prepare(dbrx.clone(), backend, pg).await.unwrap();
        let jh = tokio::spawn(async move { worker.worker().await });
        ioc_search_index_worker_jhs.push(jh);
    }
    drop(dbrx);
    while let Some(item) = net_rx.next().await {
        match item {
            Ok(item) => {
                for e in item.iter() {
                    let cacheitem =
                        IocItem::new(e.channel.clone(), e.response_addr.clone(), e.addr.clone(), e.dt.clone());
                    if let Err(_) = dbtx.send(cacheitem).await {
                        break;
                    }
                }
                if let Err(_) = tx.send(item).await {
                    break;
                }
            }
            Err(e) => {
                warn!("error during network search: {e}");
                break;
            }
        }
    }
    Ok(())
}

#[cfg(DISABLED)]
#[allow(unused)]
fn start_finder_ca(tx: Sender<DaemonEvent>, tgts: Vec<SocketAddrV4>) -> (Sender<String>, JoinHandle<()>) {
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
                todo!("send the result item");
            }
            warn!("search res fwd inp closed");
        }
    });
    (qtx, ioc_finder_jh)
}
