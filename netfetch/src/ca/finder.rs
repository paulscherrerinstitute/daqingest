use super::connset::IocAddrQuery;
use super::connset::CURRENT_SEARCH_PENDING_MAX;
use super::connset::SEARCH_BATCH_MAX;
use super::search::ca_search_workers_start;
use crate::ca::findioc::FindIocRes;
use crate::conf::CaIngestOpts;
use async_channel::Receiver;
use async_channel::Sender;
use dbpg::conn::make_pg_client;
use dbpg::iocindex::IocItem;
use dbpg::iocindex::IocSearchIndexWorker;
use dbpg::postgres::Row as PgRow;
use hashbrown::HashMap;
use log::*;
use netpod::Database;
use stats::IocFinderStats;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use taskrun::tokio;
use tokio::task::JoinHandle;

const SEARCH_DB_WORKER_CNT: usize = 2;

macro_rules! debug_batch { ($($arg:tt)*) => ( if false { debug!($($arg)*); } ) }

macro_rules! trace_batch { ($($arg:tt)*) => ( if false { trace!($($arg)*); } ) }

autoerr::create_error_v1!(
    name(Error, "Finder"),
    enum variants {
        Join(#[from] tokio::task::JoinError),
        DbPg(#[from] dbpg::err::Error),
        Postgres(#[from] dbpg::postgres::Error),
        IocSearch(#[from] crate::ca::search::Error),
    },
);

fn transform_pgres(rows: Vec<PgRow>) -> VecDeque<FindIocRes> {
    let mut ret = VecDeque::new();
    for row in rows {
        let n: Result<i32, _> = row.try_get(0);
        let ch: Result<String, _> = row.try_get(1);
        match (n, ch) {
            (Ok(n), Ok(ch)) => {
                if let Some(addr) = row.get::<_, Option<String>>(3) {
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
            }
            (_, Err(e)) => {
                error!("bad string from pg: {}", e);
            }
            (Err(e), _) => {
                error!("bad int from pg: {}", e);
            }
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
    trace!("finder::finder_full  awaited A");
    jh2.await??;
    trace!("finder::finder_full  awaited B");
    trace!("finder::finder_full  done");
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
    let (batch_rx, jh_batch) =
        batchtools::batcher::batch(SEARCH_BATCH_MAX, Duration::from_millis(200), SEARCH_DB_WORKER_CNT, qrx);
    let mut jhs = Vec::new();
    for _ in 0..SEARCH_DB_WORKER_CNT {
        let jh = tokio::spawn(finder_worker_single(
            batch_rx.clone(),
            tx.clone(),
            backend.clone(),
            db.clone(),
            stats.clone(),
        ));
        jhs.push(jh);
    }
    jh_batch.await?;
    trace!("finder_worker  jh_batch awaited");
    for (i, jh) in jhs.into_iter().enumerate() {
        jh.await??;
        trace!("finder_worker  single {i} awaited");
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
    debug!("finder_worker_single  make_pg_client");
    let (pg, jh) = make_pg_client(&db).await?;
    let sql = concat!(
        "with q1 as (select * from unnest($2::int[], $3::text[]) as unn (n, ch))",
        " select distinct on (q1.n) q1.n, q1.ch, tt.channel, tt.addr, tt.tsmod",
        " from q1 left join ioc_by_channel_log tt",
        " on tt.channel = q1.ch and tt.facility = $1 and tt.archived = 0 and tt.addr is not null",
        " order by q1.n, tsmod desc",
    );
    let qu_select_multi = pg.prepare(sql).await?;
    loop {
        match inp.recv().await {
            Ok(batch) => {
                for e in batch.iter().filter(|x| series::dbg::dbg_chn(x.name())) {
                    info!("searching database for  {:?}", e);
                }
                stats.dbsearcher_batch_recv().inc();
                stats.dbsearcher_item_recv().add(batch.len() as _);
                let ts1 = Instant::now();
                let (batch, pass_through) = batch.into_iter().fold((Vec::new(), Vec::new()), |(mut a, mut b), x| {
                    if x.use_cache() {
                        a.push(x);
                    } else {
                        b.push(x);
                    }
                    (a, b)
                });
                debug_batch!("run  query batch  len {}", batch.len());
                let names: Vec<_> = batch
                    .iter()
                    .map(|x| if x.use_cache() { x.name() } else { "---------------" })
                    .collect();
                let ns: Vec<_> = names.iter().enumerate().map(|(i, _)| i as i32).collect();
                let qres = pg.query(&qu_select_multi, &[&backend, &ns, &names]).await;
                let dt = ts1.elapsed();
                debug_batch!(
                    "done query batch  len {}: {}  {:.3}ms",
                    batch.len(),
                    qres.is_ok(),
                    dt.as_secs_f32() * 1e3
                );
                if dt > Duration::from_millis(5000) {
                    warn!("very slow query");
                }
                match qres {
                    Ok(rows) => {
                        stats.dbsearcher_select_res_0().add(rows.len() as _);
                        if rows.len() != batch.len() {
                            stats.dbsearcher_select_error_len_mismatch().inc();
                            error!("query result len {}  batch len {}", rows.len(), batch.len());
                            tokio::time::sleep(Duration::from_millis(1000)).await;
                            continue;
                        }
                        let items = transform_pgres(rows);
                        for e in items.iter() {
                            if series::dbg::dbg_chn(&e.channel) {
                                info!("found in database {:?}", e);
                            }
                        }
                        let mut items = items;
                        for e in pass_through {
                            let x = FindIocRes {
                                channel: e.name().into(),
                                response_addr: None,
                                addr: None,
                                dt: Duration::from_millis(0),
                            };
                            items.push_back(x);
                        }
                        let items_len = items.len();
                        match tx.send(items).await {
                            Ok(_) => {
                                stats.dbsearcher_batch_send().inc();
                                stats.dbsearcher_item_send().add(items_len as _);
                            }
                            Err(e) => {
                                error!("finder sees: {}", e);
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        error!("finder sees error: {}", e);
                        tokio::time::sleep(Duration::from_millis(1000)).await;
                    }
                }
            }
            Err(_e) => break,
        }
    }
    drop(pg);
    jh.await??;
    trace!("finder_worker_single done");
    Ok(())
}

async fn finder_network_if_not_found(
    rx: Receiver<VecDeque<FindIocRes>>,
    tx: Sender<VecDeque<FindIocRes>>,
    opts: CaIngestOpts,
    stats: Arc<IocFinderStats>,
) -> Result<(), Error> {
    let self_name = "finder_network_if_not_found";
    let (net_tx, net_rx, jh_ca_search) = ca_search_workers_start(&opts, stats.clone()).await?;
    let jh2 = taskrun::spawn(process_net_result(net_rx, tx.clone(), opts.clone()));
    'outer: while let Ok(item) = rx.recv().await {
        let mut res = VecDeque::new();
        let mut net = VecDeque::new();
        for e in item {
            trace!("{self_name}  sees {e:?}");
            if e.addr.is_none() {
                net.push_back(e.channel);
            } else {
                res.push_back(e);
            }
        }
        if let Err(_) = tx.send(res).await {
            debug!("{self_name}  res send error, break");
            break;
        }
        for ch in net {
            if let Err(_) = net_tx.send(ch).await {
                debug!("{self_name}  net ch send error, break");
                break 'outer;
            }
        }
    }
    drop(net_tx);
    trace!("{self_name}  loop end");
    jh_ca_search.await??;
    trace!("{self_name}  jh_ca_search  awaited");
    jh2.await??;
    trace!("{self_name}  process_net_result  awaited");
    Ok(())
}

async fn process_net_result(
    net_rx: Receiver<Result<VecDeque<FindIocRes>, crate::ca::findioc::Error>>,
    tx: Sender<VecDeque<FindIocRes>>,
    opts: CaIngestOpts,
) -> Result<(), Error> {
    const IOC_SEARCH_INDEX_WORKER_COUNT: usize = 1;
    let (dbtx, dbrx) = async_channel::bounded(64);
    let mut ioc_search_index_worker_jhs = Vec::new();
    let mut index_worker_pg_jh = Vec::new();
    for _ in 0..IOC_SEARCH_INDEX_WORKER_COUNT {
        let backend = opts.backend().into();
        let (pg, jh) = dbpg::conn::make_pg_client(opts.postgresql_config()).await?;
        index_worker_pg_jh.push(jh);
        let worker = IocSearchIndexWorker::prepare(dbrx.clone(), backend, pg).await?;
        let jh = tokio::spawn(async move { worker.worker().await });
        ioc_search_index_worker_jhs.push(jh);
    }
    drop(dbrx);
    while let Ok(item) = net_rx.recv().await {
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
    trace!("process_net_result  break loop");
    dbtx.close();
    trace!("process_net_result  dbtx closed");
    for (i, jh) in ioc_search_index_worker_jhs.into_iter().enumerate() {
        jh.await?;
        trace!("process_net_result  search index worker {i} awaited");
    }
    Ok(())
}

#[cfg(feature = "disabled")]
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
