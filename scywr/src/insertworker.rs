use crate::config::ScyllaIngestConfig;
use crate::iteminsertqueue::insert_item_fut;
use crate::iteminsertqueue::insert_msp_fut;
use crate::iteminsertqueue::Accounting;
use crate::iteminsertqueue::AccountingRecv;
use crate::iteminsertqueue::InsertFut;
use crate::iteminsertqueue::InsertItem;
use crate::iteminsertqueue::MspItem;
use crate::iteminsertqueue::QueryItem;
use crate::iteminsertqueue::TimeBinSimpleF32V02;
use crate::store::DataStore;
use async_channel::Receiver;
use atomic::AtomicU64;
use err::Error;
use futures_util::Stream;
use futures_util::StreamExt;
use log::*;
use netpod::ttl::RetentionTime;
use smallvec::smallvec;
use smallvec::SmallVec;
use stats::InsertWorkerStats;
use std::collections::VecDeque;
use std::sync::atomic;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use taskrun::tokio;
use tokio::task::JoinHandle;

#[allow(unused)]
macro_rules! trace2 {
    ($($arg:tt)*) => {
        if false {
            trace!($($arg)*);
        }
    };
}

#[allow(unused)]
macro_rules! trace3 {
    ($($arg:tt)*) => {
        if false {
            trace!($($arg)*);
        }
    };
}

#[allow(unused)]
macro_rules! trace_item_execute {
    ($($arg:tt)*) => {
        if false {
            trace!($($arg)*);
        }
    };
}

#[allow(unused)]
macro_rules! debug_setup {
    ($($arg:tt)*) => {
        if false {
            debug!($($arg)*);
        }
    };
}

fn stats_inc_for_err(stats: &stats::InsertWorkerStats, err: &crate::iteminsertqueue::Error) {
    use crate::iteminsertqueue::Error;
    match err {
        Error::DbOverload => {
            stats.db_overload().inc();
        }
        Error::DbTimeout => {
            stats.db_timeout().inc();
        }
        Error::DbUnavailable => {
            stats.db_unavailable().inc();
        }
        Error::DbError(_) => {
            if true {
                warn!("db error {err}");
            }
            stats.db_error().inc();
        }
        Error::QueryError(_) => {
            stats.query_error().inc();
        }
        Error::GetValHelpTodoWaveform => {
            stats.logic_error().inc();
        }
        Error::GetValHelpInnerTypeMismatch => {
            stats.logic_error().inc();
        }
    }
}

#[allow(unused)]
fn back_off_next(backoff_dt: &mut Duration) {
    *backoff_dt = *backoff_dt + (*backoff_dt) * 3 / 2;
    let dtmax = Duration::from_millis(4000);
    if *backoff_dt > dtmax {
        *backoff_dt = dtmax;
    }
}

#[allow(unused)]
async fn back_off_sleep(backoff_dt: &mut Duration) {
    back_off_next(backoff_dt);
    tokio::time::sleep(*backoff_dt).await;
}

pub struct InsertWorkerOpts {
    pub store_workers_rate: Arc<AtomicU64>,
    pub insert_workers_running: Arc<AtomicU64>,
    pub insert_frac: Arc<AtomicU64>,
    pub array_truncate: Arc<AtomicU64>,
}

pub async fn spawn_scylla_insert_workers(
    rett: RetentionTime,
    scyconf: ScyllaIngestConfig,
    insert_scylla_sessions: usize,
    insert_worker_count: usize,
    insert_worker_concurrency: usize,
    item_inp: Receiver<VecDeque<QueryItem>>,
    insert_worker_opts: Arc<InsertWorkerOpts>,
    store_stats: Arc<stats::InsertWorkerStats>,
    use_rate_limit_queue: bool,
) -> Result<Vec<JoinHandle<Result<(), Error>>>, Error> {
    let item_inp = if use_rate_limit_queue {
        crate::ratelimit::rate_limiter(insert_worker_opts.store_workers_rate.clone(), item_inp)
    } else {
        item_inp
    };
    let mut jhs = Vec::new();
    let mut data_stores = Vec::new();
    for _ in 0..insert_scylla_sessions {
        let data_store = Arc::new(
            DataStore::new(&scyconf, rett.clone())
                .await
                .map_err(|e| Error::from(e.to_string()))?,
        );
        data_stores.push(data_store);
    }
    for worker_ix in 0..insert_worker_count {
        let data_store = data_stores[worker_ix * data_stores.len() / insert_worker_count].clone();
        let jh = tokio::spawn(worker_streamed(
            worker_ix,
            insert_worker_concurrency,
            item_inp.clone(),
            insert_worker_opts.clone(),
            Some(data_store),
            store_stats.clone(),
        ));
        jhs.push(jh);
    }
    Ok(jhs)
}

pub async fn spawn_scylla_insert_workers_dummy(
    insert_worker_count: usize,
    insert_worker_concurrency: usize,
    item_inp: Receiver<VecDeque<QueryItem>>,
    insert_worker_opts: Arc<InsertWorkerOpts>,
    store_stats: Arc<stats::InsertWorkerStats>,
) -> Result<Vec<JoinHandle<Result<(), Error>>>, Error> {
    let mut jhs = Vec::new();
    for worker_ix in 0..insert_worker_count {
        let data_store = None;
        let jh = tokio::spawn(worker_streamed(
            worker_ix,
            insert_worker_concurrency,
            item_inp.clone(),
            insert_worker_opts.clone(),
            data_store,
            store_stats.clone(),
        ));
        jhs.push(jh);
    }
    Ok(jhs)
}

async fn worker_streamed(
    worker_ix: usize,
    concurrency: usize,
    item_inp: Receiver<VecDeque<QueryItem>>,
    insert_worker_opts: Arc<InsertWorkerOpts>,
    data_store: Option<Arc<DataStore>>,
    stats: Arc<InsertWorkerStats>,
) -> Result<(), Error> {
    debug_setup!("worker_streamed  begin");
    stats.worker_start().inc();
    insert_worker_opts
        .insert_workers_running
        .fetch_add(1, atomic::Ordering::AcqRel);
    let stream = item_inp;
    let worker_name = data_store
        .as_ref()
        .map_or_else(|| format!("dummy"), |x| x.rett.debug_tag().to_string());
    let stream = inspect_items(stream, worker_name.clone());
    if let Some(data_store) = data_store {
        let stream = transform_to_db_futures(stream, data_store, stats.clone());
        let stream = stream
            .map(|x| futures_util::stream::iter(x))
            .flatten_unordered(Some(1))
            .buffer_unordered(concurrency);
        let mut stream = Box::pin(stream);
        debug_setup!("waiting for item");
        while let Some(item) = stream.next().await {
            trace_item_execute!("see item");
            match item {
                Ok(_) => {
                    stats.inserted_values().inc();
                    // TODO compute the insert latency bin and count.
                }
                Err(e) => {
                    use scylla::transport::errors::QueryError;
                    let e = match e {
                        QueryError::TimeoutError => crate::iteminsertqueue::Error::DbTimeout,
                        // TODO use `msg`
                        QueryError::DbError(e, _msg) => match e {
                            scylla::transport::errors::DbError::Overloaded => crate::iteminsertqueue::Error::DbOverload,
                            _ => e.into(),
                        },
                        _ => e.into(),
                    };
                    stats_inc_for_err(&stats, &e);
                }
            }
        }
    } else {
        let mut stream = Box::pin(stream);
        while let Some(item) = stream.next().await {
            drop(item);
        }
    };
    stats.worker_finish().inc();
    insert_worker_opts
        .insert_workers_running
        .fetch_sub(1, atomic::Ordering::AcqRel);
    debug_setup!("insert worker {worker_ix} done");
    Ok(())
}

fn transform_to_db_futures<S>(
    item_inp: S,
    data_store: Arc<DataStore>,
    stats: Arc<InsertWorkerStats>,
) -> impl Stream<Item = Vec<InsertFut>>
where
    S: Stream<Item = VecDeque<QueryItem>>,
{
    trace!("transform_to_db_futures  begin");
    // TODO possible without box?
    // let item_inp = Box::pin(item_inp);
    item_inp.map(move |batch| {
        stats.item_recv.inc();
        trace!("transform_to_db_futures  have batch  len {}", batch.len());
        let tsnow = Instant::now();
        let mut res = Vec::with_capacity(32);
        for item in batch {
            let futs = match item {
                QueryItem::Insert(item) => prepare_query_insert_futs(item, &data_store, &stats, tsnow),
                QueryItem::Msp(item) => prepare_msp_insert_futs(item, &data_store, &stats, tsnow),
                QueryItem::TimeBinSimpleF32V02(item) => {
                    prepare_timebin_v02_insert_futs(item, &data_store, &stats, tsnow)
                }
                QueryItem::Accounting(item) => prepare_accounting_insert_futs(item, &data_store, &stats, tsnow),
                QueryItem::AccountingRecv(item) => {
                    prepare_accounting_recv_insert_futs(item, &data_store, &stats, tsnow)
                }
            };
            trace!("prepared futs  len {}", futs.len());
            res.extend(futs.into_iter());
        }
        res
    })
}

fn inspect_items(
    item_inp: Receiver<VecDeque<QueryItem>>,
    worker_name: String,
) -> impl Stream<Item = VecDeque<QueryItem>> {
    trace!("transform_to_db_futures  begin");
    // TODO possible without box?
    // let item_inp = Box::pin(item_inp);
    item_inp.inspect(move |batch| {
        for item in batch {
            match &item {
                QueryItem::Insert(item) => {
                    trace_item_execute!("execute  {worker_name}  Insert  {}", item.string_short());
                }
                QueryItem::Msp(item) => {
                    trace_item_execute!("execute  {worker_name}  Msp  {}", item.string_short());
                }
                QueryItem::TimeBinSimpleF32V02(_) => {
                    trace_item_execute!("execute  {worker_name}  TimeBinSimpleF32V02");
                }
                QueryItem::Accounting(_) => {
                    trace_item_execute!("execute  {worker_name}  Accounting  {item:?}");
                }
                QueryItem::AccountingRecv(_) => {
                    trace_item_execute!("execute  {worker_name}  Accounting  {item:?}");
                }
            }
        }
    })
}

fn prepare_msp_insert_futs(
    item: MspItem,
    data_store: &Arc<DataStore>,
    stats: &Arc<InsertWorkerStats>,
    tsnow: Instant,
) -> SmallVec<[InsertFut; 4]> {
    trace2!("execute  MSP bump");
    stats.inserts_msp().inc();
    {
        let dt = tsnow.saturating_duration_since(item.ts_net());
        let dt_ms = 1000 * dt.as_secs() as u32 + dt.subsec_millis();
        stats.item_lat_net_worker().ingest(dt_ms);
    }
    let fut = insert_msp_fut(
        item.series(),
        item.ts_msp(),
        item.ts_net(),
        data_store.scy.clone(),
        data_store.qu_insert_ts_msp.clone(),
        stats.clone(),
    );
    let futs = smallvec![fut];
    futs
}

fn prepare_query_insert_futs(
    item: InsertItem,
    data_store: &Arc<DataStore>,
    stats: &Arc<InsertWorkerStats>,
    tsnow: Instant,
) -> SmallVec<[InsertFut; 4]> {
    stats.inserts_value().inc();
    let item_ts_net = item.ts_net;
    let dt = tsnow.saturating_duration_since(item_ts_net);
    let dt_ms = 1000 * dt.as_secs() as u32 + dt.subsec_millis();
    stats.item_lat_net_worker().ingest(dt_ms);
    let do_insert = true;
    let fut = insert_item_fut(item, &data_store, do_insert, stats);
    let futs = smallvec![fut];
    futs
}

fn prepare_timebin_v02_insert_futs(
    item: TimeBinSimpleF32V02,
    data_store: &Arc<DataStore>,
    stats: &Arc<InsertWorkerStats>,
    tsnow: Instant,
) -> SmallVec<[InsertFut; 4]> {
    let params = (
        item.series.id() as i64,
        item.binlen,
        item.msp,
        item.off,
        item.cnt,
        item.min,
        item.max,
        item.avg,
        item.dev,
    );
    // TODO would be better to count inserts only on completed insert
    stats.inserted_binned().inc();
    let fut = InsertFut::new(
        data_store.scy.clone(),
        data_store.qu_insert_binned_scalar_f32_v02.clone(),
        params,
        tsnow,
        stats.clone(),
    );
    let futs = smallvec![fut];

    // TODO match on the query result:
    // match qres {
    //     Ok(_) => {
    //         backoff = backoff_0;
    //     }
    //     Err(e) => {
    //         stats_inc_for_err(&stats, &crate::iteminsertqueue::Error::QueryError(e));
    //         back_off_sleep(&mut backoff).await;
    //     }
    // }

    futs
}

fn prepare_accounting_insert_futs(
    item: Accounting,
    data_store: &Arc<DataStore>,
    stats: &Arc<InsertWorkerStats>,
    tsnow: Instant,
) -> SmallVec<[InsertFut; 4]> {
    let params = (
        item.part,
        item.ts.sec() as i64,
        item.series.id() as i64,
        item.count,
        item.bytes,
    );
    let fut = InsertFut::new(
        data_store.scy.clone(),
        data_store.qu_account_00.clone(),
        params,
        tsnow,
        stats.clone(),
    );
    let futs = smallvec![fut];
    futs
}

fn prepare_accounting_recv_insert_futs(
    item: AccountingRecv,
    data_store: &Arc<DataStore>,
    stats: &Arc<InsertWorkerStats>,
    tsnow: Instant,
) -> SmallVec<[InsertFut; 4]> {
    let params = (
        item.part,
        item.ts.sec() as i64,
        item.series.id() as i64,
        item.count,
        item.bytes,
    );
    let fut = InsertFut::new(
        data_store.scy.clone(),
        data_store.qu_account_recv_00.clone(),
        params,
        tsnow,
        stats.clone(),
    );
    let futs = smallvec![fut];
    futs
}
