use crate::iteminsertqueue::insert_channel_status;
use crate::iteminsertqueue::insert_connection_status;
use crate::iteminsertqueue::insert_item;
use crate::iteminsertqueue::QueryItem;
use crate::store::DataStore;
use async_channel::Receiver;
use async_channel::Sender;
use err::Error;
use log::*;
use netpod::timeunits::MS;
use netpod::timeunits::SEC;
use netpod::ScyllaConfig;
use stats::InsertWorkerStats;
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

async fn rate_limiter_worker(
    rate: Arc<AtomicU64>,
    inp: Receiver<QueryItem>,
    tx: Sender<QueryItem>,
    stats: Arc<stats::InsertWorkerStats>,
) {
    let mut ts_forward_last = Instant::now();
    let mut ivl_ema = stats::Ema64::with_k(0.00001);
    loop {
        let item = if let Ok(x) = inp.recv().await {
            x
        } else {
            break;
        };
        let ts_received = Instant::now();
        let allowed_to_drop = match &item {
            QueryItem::Insert(_) => true,
            _ => false,
        };
        let dt_min = {
            let rate2 = rate.load(Ordering::Acquire);
            Duration::from_nanos(SEC / rate2)
        };
        let mut ema2 = ivl_ema.clone();
        {
            let dt = ts_received.duration_since(ts_forward_last);
            let dt_ns = SEC * dt.as_secs() + dt.subsec_nanos() as u64;
            ema2.update(dt_ns.min(MS * 100) as f32);
        }
        let ivl2 = Duration::from_nanos(ema2.ema() as u64);
        if allowed_to_drop && ivl2 < dt_min {
            //tokio::time::sleep_until(ts_recv_last.checked_add(dt_min).unwrap().into()).await;
            stats.ratelimit_drop().inc();
        } else {
            if tx.send(item).await.is_err() {
                break;
            } else {
                let tsnow = Instant::now();
                let dt = tsnow.duration_since(ts_forward_last);
                let dt_ns = SEC * dt.as_secs() + dt.subsec_nanos() as u64;
                ivl_ema.update(dt_ns.min(MS * 100) as f32);
                ts_forward_last = tsnow;
                // stats.inter_ivl_ema.set(ivl_ema.ema() as u64);
            }
        }
    }
    info!("rate limiter done");
}

fn rate_limiter(
    inp: Receiver<QueryItem>,
    opts: Arc<InsertWorkerOpts>,
    stats: Arc<stats::InsertWorkerStats>,
) -> Receiver<QueryItem> {
    let (tx, rx) = async_channel::bounded(inp.capacity().unwrap_or(256));
    tokio::spawn(rate_limiter_worker(opts.store_workers_rate.clone(), inp, tx, stats));
    rx
}

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
            QueryItem::ConnectionStatus(item) => {
                match insert_connection_status(item, ttls.index, &data_store, &stats).await {
                    Ok(_) => {
                        stats.inserted_connection_status().inc();
                        backoff = backoff_0;
                    }
                    Err(e) => {
                        stats_inc_for_err(&stats, &e);
                        back_off_sleep(&mut backoff).await;
                    }
                }
            }
            QueryItem::ChannelStatus(item) => {
                match insert_channel_status(item, ttls.index, &data_store, &stats).await {
                    Ok(_) => {
                        stats.inserted_channel_status().inc();
                        backoff = backoff_0;
                    }
                    Err(e) => {
                        stats_inc_for_err(&stats, &e);
                        back_off_sleep(&mut backoff).await;
                    }
                }
            }
            QueryItem::Insert(item) => {
                let tsnow = {
                    let ts = SystemTime::now();
                    let epoch = ts.duration_since(std::time::UNIX_EPOCH).unwrap();
                    epoch.as_secs() * SEC + epoch.subsec_nanos() as u64
                };
                let dt = (tsnow / 1000) as i64 - (item.ts_local / 1000) as i64;
                if dt < 0 {
                    stats.item_latency_neg().inc();
                } else if dt <= 1000 * 25 {
                    stats.item_latency_025ms().inc();
                } else if dt <= 1000 * 50 {
                    stats.item_latency_050ms().inc();
                } else if dt <= 1000 * 100 {
                    stats.item_latency_100ms().inc();
                } else if dt <= 1000 * 200 {
                    stats.item_latency_200ms().inc();
                } else if dt <= 1000 * 400 {
                    stats.item_latency_400ms().inc();
                } else if dt <= 1000 * 800 {
                    stats.item_latency_800ms().inc();
                } else {
                    stats.item_latency_large().inc();
                }
                if false {
                    stats.inserted_values().inc();
                } else {
                    let insert_frac = insert_worker_opts.insert_frac.load(Ordering::Acquire);
                    let do_insert = i1 % 1000 < insert_frac;
                    match insert_item(item, ttls.index, ttls.d0, ttls.d1, &data_store, &stats, do_insert).await {
                        Ok(_) => {
                            stats.inserted_values().inc();
                            backoff = backoff_0;
                        }
                        Err(e) => {
                            stats_inc_for_err(&stats, &e);
                            back_off_sleep(&mut backoff).await;
                        }
                    }
                    i1 += 1;
                }
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
        rate_limiter(item_inp, insert_worker_opts.clone(), store_stats.clone())
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
        let jh = tokio::spawn(worker(
            worker_ix,
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
