use crate::ca::store::DataStore;
use crate::ca::IngestCommons;
use crate::rt::JoinHandle;
use crate::store::{CommonInsertItemQueue, IntoSimplerError, QueryItem};
use err::Error;
use log::*;
use netpod::ScyllaConfig;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_postgres::Client as PgClient;

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
        Error::DbError(e) => {
            warn!("db error {e}");
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

pub async fn spawn_scylla_insert_workers(
    scyconf: ScyllaConfig,
    insert_scylla_sessions: usize,
    insert_worker_count: usize,
    insert_item_queue: Arc<CommonInsertItemQueue>,
    ingest_commons: Arc<IngestCommons>,
    pg_client: Arc<PgClient>,
    store_stats: Arc<stats::CaConnStats>,
) -> Result<Vec<JoinHandle<()>>, Error> {
    let (q2_tx, q2_rx) = async_channel::bounded(insert_item_queue.receiver().capacity().unwrap_or(20000));
    {
        let ingest_commons = ingest_commons.clone();
        let stats = store_stats.clone();
        let recv = insert_item_queue.receiver();
        let mut ts_recv_last = Instant::now();
        let fut = async move {
            loop {
                let item = if let Ok(x) = recv.recv().await {
                    x
                } else {
                    break;
                };
                let allowed_to_drop = match &item {
                    QueryItem::Insert(_) => true,
                    _ => false,
                };
                let dt_min = {
                    let rate = ingest_commons.store_workers_rate.load(Ordering::Acquire);
                    Duration::from_nanos(1000000000 / rate)
                };
                let tsnow = Instant::now();
                let dt = tsnow.duration_since(ts_recv_last);
                ts_recv_last = tsnow;
                if allowed_to_drop && dt < dt_min {
                    //tokio::time::sleep_until(ts_recv_last.checked_add(dt_min).unwrap().into()).await;
                    stats.store_worker_ratelimit_drop_inc();
                } else {
                    if q2_tx.send(item).await.is_err() {
                        break;
                    } else {
                        stats.store_worker_item_recv_inc();
                    }
                }
            }
            info!("intermediate queue done");
        };
        tokio::spawn(fut);
    }

    let mut jhs = Vec::new();
    let mut data_stores = Vec::new();
    for _ in 0..insert_scylla_sessions {
        let data_store = Arc::new(DataStore::new(&scyconf, pg_client.clone()).await?);
        data_stores.push(data_store);
    }
    for i1 in 0..insert_worker_count {
        let data_store = data_stores[i1 * data_stores.len() / insert_worker_count].clone();
        let stats = store_stats.clone();
        //let recv = insert_item_queue.receiver();
        let recv = q2_rx.clone();
        let ingest_commons = ingest_commons.clone();
        let fut = async move {
            let backoff_0 = Duration::from_millis(10);
            let mut backoff = backoff_0.clone();
            let mut i1 = 0;
            loop {
                let item = if let Ok(item) = recv.recv().await {
                    item
                } else {
                    break;
                };
                match item {
                    QueryItem::ConnectionStatus(item) => {
                        match crate::store::insert_connection_status(item, &data_store, &stats).await {
                            Ok(_) => {
                                stats.connection_status_insert_done_inc();
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
                                stats.channel_status_insert_done_inc();
                                backoff = backoff_0;
                            }
                            Err(e) => {
                                stats_inc_for_err(&stats, &e);
                                back_off_sleep(&mut backoff).await;
                            }
                        }
                    }
                    QueryItem::Insert(item) => {
                        let insert_frac = ingest_commons.insert_frac.load(Ordering::Acquire);
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
                            stats.store_worker_fraction_drop_inc();
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
                                stats.mute_insert_done_inc();
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
                                stats.ivl_insert_done_inc();
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
                            (item.series.id() & 0xff) as i32,
                            item.ts_msp as i32,
                            item.series.id() as i64,
                            item.ivl,
                            item.interest,
                            item.evsize as i32,
                        );
                        let qres = data_store.scy.execute(&data_store.qu_insert_channel_ping, params).await;
                        match qres {
                            Ok(_) => {
                                stats.channel_info_insert_done_inc();
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