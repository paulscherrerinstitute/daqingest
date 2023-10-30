use crate::iteminsertqueue::insert_channel_status;
use crate::iteminsertqueue::insert_channel_status_fut;
use crate::iteminsertqueue::insert_connection_status;
use crate::iteminsertqueue::insert_connection_status_fut;
use crate::iteminsertqueue::insert_item;
use crate::iteminsertqueue::insert_item_fut;
use crate::iteminsertqueue::insert_msp_fut;
use crate::iteminsertqueue::ConnectionStatusItem;
use crate::iteminsertqueue::InsertFut;
use crate::iteminsertqueue::InsertItem;
use crate::iteminsertqueue::QueryItem;
use crate::store::DataStore;
use async_channel::Receiver;
use async_channel::Sender;
use err::Error;
use futures_util::Future;
use futures_util::TryFutureExt;
use log::*;
use netpod::timeunits::MS;
use netpod::timeunits::SEC;
use netpod::ScyllaConfig;
use smallvec::smallvec;
use smallvec::SmallVec;
use stats::InsertWorkerStats;
use std::pin::Pin;
use std::sync::atomic;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use taskrun::tokio;
use taskrun::tokio::task::JoinHandle;

#[allow(unused)]
macro_rules! trace2 {
    ($($arg:tt)*) => {
        if false {
            trace!($($arg)*);
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
        Error::DbError(e) => {
            if false {
                warn!("db error {e}");
            }
            stats.db_error().inc();
        }
        Error::QueryError(_) => {
            stats.query_error().inc();
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

#[derive(Debug, Clone)]
pub struct Ttls {
    pub index: Duration,
    pub d0: Duration,
    pub d1: Duration,
    pub binned: Duration,
}

pub struct InsertWorkerOpts {
    pub store_workers_rate: Arc<AtomicU64>,
    pub insert_workers_running: Arc<AtomicU64>,
    pub insert_frac: Arc<AtomicU64>,
    pub array_truncate: Arc<AtomicU64>,
}

pub async fn spawn_scylla_insert_workers(
    scyconf: ScyllaConfig,
    insert_scylla_sessions: usize,
    insert_worker_count: usize,
    item_inp: Receiver<QueryItem>,
    insert_worker_opts: Arc<InsertWorkerOpts>,
    store_stats: Arc<stats::InsertWorkerStats>,
    use_rate_limit_queue: bool,
    ttls: Ttls,
) -> Result<Vec<JoinHandle<Result<(), Error>>>, Error> {
    let item_inp = if use_rate_limit_queue {
        crate::ratelimit::rate_limiter(insert_worker_opts.store_workers_rate.clone(), item_inp)
    } else {
        item_inp
    };
    let mut jhs = Vec::new();
    let mut data_stores = Vec::new();
    for _ in 0..insert_scylla_sessions {
        let data_store = Arc::new(DataStore::new(&scyconf).await.map_err(|e| Error::from(e.to_string()))?);
        data_stores.push(data_store);
    }
    for worker_ix in 0..insert_worker_count {
        let data_store = data_stores[worker_ix * data_stores.len() / insert_worker_count].clone();
        #[cfg(DISABLED)]
        let jh = tokio::spawn(worker(
            worker_ix,
            item_inp.clone(),
            ttls.clone(),
            insert_worker_opts.clone(),
            data_store,
            store_stats.clone(),
        ));
        let jh = tokio::spawn(worker_streamed(
            worker_ix,
            insert_worker_count * 3,
            item_inp.clone(),
            ttls.clone(),
            insert_worker_opts.clone(),
            data_store,
            store_stats.clone(),
        ));
        jhs.push(jh);
    }
    Ok(jhs)
}

#[allow(unused)]
async fn worker(
    worker_ix: usize,
    item_inp: Receiver<QueryItem>,
    ttls: Ttls,
    insert_worker_opts: Arc<InsertWorkerOpts>,
    data_store: Arc<DataStore>,
    stats: Arc<InsertWorkerStats>,
) -> Result<(), Error> {
    stats.worker_start().inc();
    insert_worker_opts
        .insert_workers_running
        .fetch_add(1, atomic::Ordering::AcqRel);
    let backoff_0 = Duration::from_millis(10);
    let mut backoff = backoff_0.clone();
    let mut i1 = 0;
    loop {
        let item = if let Ok(item) = item_inp.recv().await {
            stats.item_recv.inc();
            item
        } else {
            break;
        };
        match item {
            QueryItem::ConnectionStatus(item) => match insert_connection_status(item, ttls.index, &data_store).await {
                Ok(_) => {
                    stats.inserted_connection_status().inc();
                    backoff = backoff_0;
                }
                Err(e) => {
                    stats_inc_for_err(&stats, &e);
                    back_off_sleep(&mut backoff).await;
                }
            },
            QueryItem::ChannelStatus(item) => match insert_channel_status(item, ttls.index, &data_store).await {
                Ok(_) => {
                    stats.inserted_channel_status().inc();
                    backoff = backoff_0;
                }
                Err(e) => {
                    stats_inc_for_err(&stats, &e);
                    back_off_sleep(&mut backoff).await;
                }
            },
            QueryItem::Insert(item) => {
                let item_ts_local = item.ts_local.clone();
                let tsnow = {
                    let ts = SystemTime::now();
                    let epoch = ts.duration_since(std::time::UNIX_EPOCH).unwrap();
                    epoch.as_secs() * SEC + epoch.subsec_nanos() as u64
                };
                let dt = ((tsnow / 1000000) as u32).saturating_sub((item_ts_local / 1000000) as u32);
                stats.item_lat_net_worker().ingest(dt);
                let insert_frac = insert_worker_opts.insert_frac.load(Ordering::Acquire);
                let do_insert = i1 % 1000 < insert_frac;
                match insert_item(item, &ttls, &data_store, do_insert, &stats).await {
                    Ok(_) => {
                        stats.inserted_values().inc();
                        let tsnow = {
                            let ts = SystemTime::now();
                            let epoch = ts.duration_since(std::time::UNIX_EPOCH).unwrap();
                            epoch.as_secs() * SEC + epoch.subsec_nanos() as u64
                        };
                        let dt = ((tsnow / 1000000) as u32).saturating_sub((item_ts_local / 1000000) as u32);
                        stats.item_lat_net_store().ingest(dt);
                        backoff = backoff_0;
                    }
                    Err(e) => {
                        stats_inc_for_err(&stats, &e);
                        back_off_sleep(&mut backoff).await;
                    }
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
                    ttls.index.as_secs() as i32,
                );
                let qres = data_store.scy.execute(&data_store.qu_insert_muted, values).await;
                match qres {
                    Ok(_) => {
                        stats.inserted_mute().inc();
                        backoff = backoff_0;
                    }
                    Err(e) => {
                        stats_inc_for_err(&stats, &crate::iteminsertqueue::Error::QueryError(e));
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
                    ttls.index.as_secs() as i32,
                );
                let qres = data_store
                    .scy
                    .execute(&data_store.qu_insert_item_recv_ivl, values)
                    .await;
                match qres {
                    Ok(_) => {
                        stats.inserted_interval().inc();
                        backoff = backoff_0;
                    }
                    Err(e) => {
                        stats_inc_for_err(&stats, &crate::iteminsertqueue::Error::QueryError(e));
                        back_off_sleep(&mut backoff).await;
                    }
                }
            }
            QueryItem::ChannelInfo(item) => {
                let params = (
                    (item.series.id() & 0xff) as i32,
                    item.ts_msp as i32,
                    item.series.id() as i64,
                    item.ivl,
                    item.interest,
                    item.evsize as i32,
                    ttls.index.as_secs() as i32,
                );
                let qres = data_store.scy.execute(&data_store.qu_insert_channel_ping, params).await;
                match qres {
                    Ok(_) => {
                        stats.inserted_channel_info().inc();
                        backoff = backoff_0;
                    }
                    Err(e) => {
                        stats_inc_for_err(&stats, &crate::iteminsertqueue::Error::QueryError(e));
                        back_off_sleep(&mut backoff).await;
                    }
                }
            }
            QueryItem::TimeBinPatchSimpleF32(item) => {
                info!("have time bin patch to insert: {item:?}");
                let params = (
                    item.series.id() as i64,
                    item.bin_len_sec as i32,
                    item.bin_count as i32,
                    item.off_msp as i32,
                    item.off_lsp as i32,
                    item.counts,
                    item.mins,
                    item.maxs,
                    item.avgs,
                    ttls.binned.as_secs() as i32,
                );
                let qres = data_store
                    .scy
                    .execute(&data_store.qu_insert_binned_scalar_f32_v01, params)
                    .await;
                match qres {
                    Ok(_) => {
                        stats.inserted_binned().inc();
                        backoff = backoff_0;
                    }
                    Err(e) => {
                        stats_inc_for_err(&stats, &crate::iteminsertqueue::Error::QueryError(e));
                        back_off_sleep(&mut backoff).await;
                    }
                }
            }
        }
    }
    stats.worker_finish().inc();
    insert_worker_opts
        .insert_workers_running
        .fetch_sub(1, atomic::Ordering::AcqRel);
    trace2!("insert worker {worker_ix} done");
    Ok(())
}

async fn worker_streamed(
    worker_ix: usize,
    concurrency: usize,
    item_inp: Receiver<QueryItem>,
    ttls: Ttls,
    insert_worker_opts: Arc<InsertWorkerOpts>,
    data_store: Arc<DataStore>,
    stats: Arc<InsertWorkerStats>,
) -> Result<(), Error> {
    use futures_util::StreamExt;
    stats.worker_start().inc();
    insert_worker_opts
        .insert_workers_running
        .fetch_add(1, atomic::Ordering::AcqRel);
    // TODO possible without box?
    let item_inp = Box::pin(item_inp);
    let mut stream = item_inp
        .map(|item| {
            stats.item_recv.inc();
            let tsnow_u64 = {
                let ts = SystemTime::now();
                let epoch = ts.duration_since(std::time::UNIX_EPOCH).unwrap();
                epoch.as_secs() * SEC + epoch.subsec_nanos() as u64
            };
            match item {
                QueryItem::Insert(item) => prepare_query_insert_futs(item, &ttls, &data_store, &stats, tsnow_u64),
                QueryItem::ConnectionStatus(item) => {
                    stats.inserted_connection_status().inc();
                    let fut = insert_connection_status_fut(item, &ttls, &data_store, stats.clone());
                    smallvec![fut]
                }
                QueryItem::ChannelStatus(item) => {
                    stats.inserted_channel_status().inc();
                    insert_channel_status_fut(item, &ttls, &data_store, stats.clone())
                }
                _ => {
                    // TODO
                    SmallVec::new()
                }
            }
        })
        .map(|x| futures_util::stream::iter(x))
        .flatten_unordered(Some(1))
        .buffer_unordered(concurrency);
    while let Some(item) = stream.next().await {
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
    stats.worker_finish().inc();
    insert_worker_opts
        .insert_workers_running
        .fetch_sub(1, atomic::Ordering::AcqRel);
    trace2!("insert worker {worker_ix} done");
    Ok(())
}

fn prepare_query_insert_futs(
    item: InsertItem,
    ttls: &Ttls,
    data_store: &Arc<DataStore>,
    stats: &Arc<InsertWorkerStats>,
    tsnow_u64: u64,
) -> SmallVec<[InsertFut; 4]> {
    stats.inserts_value().inc();
    let item_ts_local = item.ts_local;
    let dt = ((tsnow_u64 / 1000000) as u32).saturating_sub((item_ts_local / 1000000) as u32);
    stats.item_lat_net_worker().ingest(dt);
    let msp_bump = item.msp_bump;
    let series = item.series.clone();
    let ts_msp = item.ts_msp;
    let do_insert = true;
    let fut = insert_item_fut(item, &ttls, &data_store, do_insert, stats);
    let mut futs = smallvec![fut];
    if msp_bump {
        stats.inserts_msp().inc();
        let fut = insert_msp_fut(
            series,
            ts_msp,
            item_ts_local,
            ttls,
            data_store.scy.clone(),
            data_store.qu_insert_ts_msp.clone(),
            stats.clone(),
        );
        futs.push(fut);
    }
    #[cfg(DISABLED)]
    if let Some(ts_msp_grid) = item.ts_msp_grid {
        let params = (
            (item.series.id() as i32) & 0xff,
            ts_msp_grid as i32,
            if item.shape.to_scylla_vec().is_empty() { 0 } else { 1 } as i32,
            item.scalar_type.to_scylla_i32(),
            item.series.id() as i64,
            ttls.index.as_secs() as i32,
        );
        data_store
            .scy
            .execute(&data_store.qu_insert_series_by_ts_msp, params)
            .await?;
        stats.inserts_msp_grid().inc();
    }
    futs
}
