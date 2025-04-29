use crate::config::ScyllaIngestConfig;
use crate::iteminsertqueue::Accounting;
use crate::iteminsertqueue::AccountingRecv;
use crate::iteminsertqueue::BinWriteIndexV03;
use crate::iteminsertqueue::InsertFut;
use crate::iteminsertqueue::InsertItem;
use crate::iteminsertqueue::MspItem;
use crate::iteminsertqueue::QueryItem;
use crate::iteminsertqueue::TimeBinSimpleF32V02;
use crate::iteminsertqueue::insert_item_fut;
use crate::iteminsertqueue::insert_msp_fut;
use crate::store::DataStore;
use async_channel::Receiver;
use async_channel::Sender;
use atomic::AtomicU64;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use log;
use netpod::ttl::RetentionTime;
use smallvec::SmallVec;
use smallvec::smallvec;
use stats::InsertWorkerStats;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;
use taskrun::tokio;
use tokio::task::JoinHandle;

macro_rules! error { ($($arg:expr),*) => ( if true { log::error!($($arg),*); } ); }

macro_rules! warn { ($($arg:expr),*) => ( if true { log::warn!($($arg),*); } ); }

macro_rules! trace2 { ($($arg:expr),*) => ( if false { log::trace!($($arg),*); } ); }

macro_rules! trace_transform { ($($arg:expr),*) => ( if false { log::trace!($($arg),*); } ); }

macro_rules! trace_inspect { ($($arg:expr),*) => ( if false { log::trace!($($arg),*); } ); }

macro_rules! trace_item_execute { ($($arg:expr),*) => ( if false { log::trace!($($arg),*); } ); }

macro_rules! debug_setup { ($($arg:expr),*) => ( if false { log::debug!($($arg),*); } ); }

autoerr::create_error_v1!(
    name(Error, "ScyllaInsertWorker"),
    enum variants {
        Store(#[from] crate::store::Error),
    },
);

fn stats_inc_for_err(stats: &stats::InsertWorkerStats, err: &crate::iteminsertqueue::InsertFutError) {
    use crate::iteminsertqueue::InsertFutError;
    match err {
        InsertFutError::Execution(e) => match e {
            scylla::errors::ExecutionError::RequestTimeout(_) => {
                stats.db_timeout().inc();
            }
            _ => {
                if true {
                    warn!("db error {}", err);
                }
                stats.db_error().inc();
            }
        },
        InsertFutError::NoFuture => {
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

#[derive(Debug)]
pub enum InsertWorkerOutputItem {
    Metrics(stats::mett::ScyllaInsertWorker),
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
    ignore_writes: bool,
    tx: Sender<InsertWorkerOutputItem>,
) -> Result<Vec<JoinHandle<Result<(), Error>>>, Error> {
    let item_inp = if use_rate_limit_queue {
        crate::ratelimit::rate_limiter(insert_worker_opts.store_workers_rate.clone(), item_inp)
    } else {
        item_inp
    };
    let mut jhs = Vec::new();
    let mut data_stores = Vec::new();
    for _ in 0..insert_scylla_sessions {
        let data_store = Arc::new(DataStore::new(&scyconf, rett.clone()).await?);
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
            ignore_writes,
            store_stats.clone(),
            tx.clone(),
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
    tx: Sender<InsertWorkerOutputItem>,
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
            true,
            store_stats.clone(),
            tx.clone(),
        ));
        jhs.push(jh);
    }
    Ok(jhs)
}

struct FutTrackDt<F> {
    ts1: Instant,
    ts2: Instant,
    ts_net: Instant,
    poll1: bool,
    fut: F,
}

impl FutTrackDt<InsertFut> {
    fn from_fut_job(job: FutJob) -> Self {
        let tsnow = Instant::now();
        Self {
            ts1: tsnow,
            ts2: tsnow,
            ts_net: job.ts_net,
            poll1: false,
            fut: job.fut,
        }
    }
}

impl<F> Future for FutTrackDt<F>
where
    F: Future + Unpin,
{
    type Output = (Instant, Instant, Instant, F::Output);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        if self.poll1 == false {
            self.poll1 = true;
            self.ts2 = Instant::now();
        }
        match self.as_mut().fut.poll_unpin(cx) {
            Ready(x) => Ready((self.ts_net, self.ts1, self.ts2, x)),
            Pending => Pending,
        }
    }
}

async fn worker_streamed(
    worker_ix: usize,
    concurrency: usize,
    item_inp: Receiver<VecDeque<QueryItem>>,
    insert_worker_opts: Arc<InsertWorkerOpts>,
    data_store: Option<Arc<DataStore>>,
    ignore_writes: bool,
    stats: Arc<InsertWorkerStats>,
    tx: Sender<InsertWorkerOutputItem>,
) -> Result<(), Error> {
    debug_setup!("worker_streamed  begin");
    let tsnow = Instant::now();
    let mut mett = stats::mett::ScyllaInsertWorker::new();
    let mut mett_emit_last = tsnow;
    let metrics_ivl = Duration::from_millis(1000);
    insert_worker_opts
        .insert_workers_running
        .fetch_add(1, atomic::Ordering::AcqRel);
    let stream = item_inp;
    let worker_name = data_store
        .as_ref()
        .map_or_else(|| format!("dummy"), |x| x.rett.debug_tag().to_string());
    let stream = inspect_items(stream, worker_name.clone());
    if let Some(data_store) = data_store {
        let stream = transform_to_db_futures(stream, data_store, ignore_writes, stats.clone());
        let stream = stream
            .map(|x| futures_util::stream::iter(x))
            .flatten_unordered(Some(1))
            .map(|x| FutTrackDt::from_fut_job(x))
            .buffer_unordered(concurrency);
        let mut stream = Box::pin(stream);
        debug_setup!("waiting for item");
        while let Some((ts_net, ts1, ts2, item)) = stream.next().await {
            trace_item_execute!("see item");
            let tsnow = Instant::now();
            match item {
                Ok(_) => {
                    mett.job_ok().inc();
                    let dt1 = tsnow.saturating_duration_since(ts1);
                    let dt2 = tsnow.saturating_duration_since(ts2);
                    let dt_net = tsnow.saturating_duration_since(ts_net);
                    mett.job_dt1().push_dur_100us(dt1);
                    mett.job_dt2().push_dur_100us(dt2);
                    mett.job_dt_net().push_dur_100us(dt_net);
                }
                Err(e) => {
                    mett.job_err().inc();
                    stats_inc_for_err(&stats, &e);
                }
            }
            if mett_emit_last + metrics_ivl <= tsnow {
                mett_emit_last = tsnow;
                let m = mett.take_and_reset();
                let item = InsertWorkerOutputItem::Metrics(m);
                match tx.send(item).await {
                    Ok(()) => {}
                    Err(_) => {
                        error!("insert worker can not emit metrics");
                        break;
                    }
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

struct FutJob {
    fut: InsertFut,
    ts_net: Instant,
}

fn transform_to_db_futures<S>(
    item_inp: S,
    data_store: Arc<DataStore>,
    ignore_writes: bool,
    stats: Arc<InsertWorkerStats>,
) -> impl Stream<Item = Vec<FutJob>>
where
    S: Stream<Item = VecDeque<QueryItem>>,
{
    trace_transform!("transform_to_db_futures  begin");
    // TODO possible without box?
    // let item_inp = Box::pin(item_inp);
    item_inp.map(move |batch| {
        stats.item_recv.inc();
        trace_transform!("transform_to_db_futures  have batch  len {}", batch.len());
        let tsnow = Instant::now();
        let mut res = Vec::with_capacity(32);
        for item in batch {
            let futs = match item {
                QueryItem::Insert(item) => {
                    if ignore_writes {
                        SmallVec::new()
                    } else {
                        prepare_query_insert_futs(item, &data_store)
                    }
                }
                QueryItem::Msp(item) => {
                    if ignore_writes {
                        SmallVec::new()
                    } else {
                        prepare_msp_insert_futs(item, &data_store)
                    }
                }
                QueryItem::TimeBinSimpleF32V02(item) => {
                    if ignore_writes {
                        SmallVec::new()
                    } else {
                        prepare_timebin_v02_insert_futs(item, &data_store, tsnow)
                    }
                }
                QueryItem::BinWriteIndexV03(item) => {
                    if ignore_writes {
                        SmallVec::new()
                    } else {
                        prepare_bin_write_index_v03_insert_futs(item, &data_store, tsnow)
                    }
                }
                QueryItem::Accounting(item) => {
                    if ignore_writes {
                        SmallVec::new()
                    } else {
                        prepare_accounting_insert_futs(item, &data_store, tsnow)
                    }
                }
                QueryItem::AccountingRecv(item) => {
                    if ignore_writes {
                        SmallVec::new()
                    } else {
                        prepare_accounting_recv_insert_futs(item, &data_store, tsnow)
                    }
                }
            };
            trace_transform!("prepared futs  len {}", futs.len());
            res.extend(futs.into_iter());
        }
        res
    })
}

fn inspect_items(
    item_inp: Receiver<VecDeque<QueryItem>>,
    worker_name: String,
) -> impl Stream<Item = VecDeque<QueryItem>> {
    trace_inspect!("transform_to_db_futures  begin");
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
                QueryItem::BinWriteIndexV03(_) => {
                    trace_item_execute!("execute  {worker_name}  BinWriteIndexV03");
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

fn prepare_msp_insert_futs(item: MspItem, data_store: &Arc<DataStore>) -> SmallVec<[FutJob; 4]> {
    trace2!("execute  MSP bump");
    let fut = insert_msp_fut(
        item.series(),
        item.ts_msp(),
        data_store.scy.clone(),
        data_store.qu_insert_ts_msp.clone(),
    );
    let fut = FutJob {
        fut,
        ts_net: item.ts_net(),
    };
    let futs = smallvec![fut];
    futs
}

fn prepare_query_insert_futs(item: InsertItem, data_store: &Arc<DataStore>) -> SmallVec<[FutJob; 4]> {
    let item_ts_net = item.ts_net;
    let do_insert = true;
    let fut = insert_item_fut(item, &data_store, do_insert);
    let fut = FutJob {
        fut,
        ts_net: item_ts_net,
    };
    let futs = smallvec![fut];
    futs
}

fn prepare_timebin_v02_insert_futs(
    item: TimeBinSimpleF32V02,
    data_store: &Arc<DataStore>,
    tsnow: Instant,
) -> SmallVec<[FutJob; 4]> {
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
        item.lst,
    );
    let fut = InsertFut::new(
        data_store.scy.clone(),
        data_store.qu_insert_binned_scalar_f32_v02.clone(),
        params,
    );
    let fut = FutJob { fut, ts_net: tsnow };
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

fn prepare_bin_write_index_v03_insert_futs(
    item: BinWriteIndexV03,
    data_store: &Arc<DataStore>,
    tsnow: Instant,
) -> SmallVec<[FutJob; 4]> {
    let params = (item.series, item.pbp, item.msp, item.rt, item.lsp, item.binlen);
    let fut = InsertFut::new(
        data_store.scy.clone(),
        data_store.qu_insert_bin_write_index_v03.clone(),
        params,
    );
    let fut = FutJob { fut, ts_net: tsnow };
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
    tsnow: Instant,
) -> SmallVec<[FutJob; 4]> {
    let params = (
        item.part,
        item.ts.sec() as i64,
        item.series.id() as i64,
        item.count,
        item.bytes,
    );
    let fut = InsertFut::new(data_store.scy.clone(), data_store.qu_account_00.clone(), params);
    let fut = FutJob { fut, ts_net: tsnow };
    let futs = smallvec![fut];
    futs
}

fn prepare_accounting_recv_insert_futs(
    item: AccountingRecv,
    data_store: &Arc<DataStore>,
    tsnow: Instant,
) -> SmallVec<[FutJob; 4]> {
    let params = (
        item.part,
        item.ts.sec() as i64,
        item.series.id() as i64,
        item.count,
        item.bytes,
    );
    let fut = InsertFut::new(data_store.scy.clone(), data_store.qu_account_recv_00.clone(), params);
    let fut = FutJob { fut, ts_net: tsnow };
    let futs = smallvec![fut];
    futs
}
