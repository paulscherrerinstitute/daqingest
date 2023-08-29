use crate::daemon::CURRENT_SEARCH_PENDING_MAX;
use crate::daemon::SEARCH_BATCH_MAX;
use crate::daemon::SEARCH_DB_PIPELINE_LEN;
use crate::daemon::SEARCH_REQ_BATCH_RECV_COUNT;
use crate::daemon::SEARCH_RES_0_COUNT;
use crate::daemon::SEARCH_RES_1_COUNT;
use crate::daemon::SEARCH_RES_2_COUNT;
use crate::daemon::SEARCH_RES_3_COUNT;
use async_channel::Sender;
use dbpg::conn::PgClient;
use dbpg::postgres::Row as PgRow;
use futures_util::StreamExt;
use log::*;
use netfetch::ca::findioc::FindIocRes;
use netfetch::daemon_common::DaemonEvent;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::atomic;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use taskrun::tokio;

#[allow(unused)]
macro_rules! debug_batch {
    // (D$($arg:tt)*) => ();
    ($($arg:tt)*) => (if false {
        debug!($($arg)*);
    });
}

#[allow(unused)]
macro_rules! trace_batch {
    // (D$($arg:tt)*) => ();
    ($($arg:tt)*) => (if false {
        trace!($($arg)*);
    });
}

pub fn start_finder(
    tx: Sender<DaemonEvent>,
    backend: String,
    pgcs: Vec<Arc<PgClient>>,
) -> (Sender<String>, tokio::task::JoinHandle<()>) {
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
    let (qtx, qrx) = async_channel::bounded(CURRENT_SEARCH_PENDING_MAX);
    let fut = async move {
        let (batch_rx, _jh) = batchtools::batcher::batch(
            SEARCH_BATCH_MAX,
            Duration::from_millis(200),
            SEARCH_DB_PIPELINE_LEN,
            qrx,
        );
        let (pgc_tx, pgc_rx) = async_channel::bounded(128);
        for pgc in pgcs {
            let sql = concat!(
                "with q1 as (select * from unnest($2::text[]) as unn (ch))",
                " select distinct on (tt.facility, tt.channel) tt.channel, tt.addr",
                " from ioc_by_channel_log tt join q1 on tt.channel = q1.ch and tt.facility = $1 and tt.addr is not null",
                " order by tt.facility, tt.channel, tsmod desc",
            );
            let qu_select_multi = pgc.prepare(sql).await.unwrap();
            let qu_select_multi = Arc::new(qu_select_multi);
            match pgc_tx.send((pgc, qu_select_multi)).await {
                Ok(_) => {}
                Err(e) => {
                    error!("can not enqueue pgc {e}");
                }
            }
        }
        let backend = Arc::new(backend.clone());
        let stream = batch_rx
            .map(|batch: Vec<String>| {
                let pgc_tx = pgc_tx.clone();
                let pgc_rx = pgc_rx.clone();
                let backend = backend.clone();
                SEARCH_REQ_BATCH_RECV_COUNT.fetch_add(batch.len(), atomic::Ordering::AcqRel);
                async move {
                    let ts1 = Instant::now();
                    let (pgc, qu_select_multi) = pgc_rx.recv().await.unwrap();
                    debug_batch!("run  query batch  len {}", batch.len());
                    let qres = pgc.query(qu_select_multi.as_ref(), &[backend.as_ref(), &batch]).await;
                    let dt = ts1.elapsed();
                    debug_batch!(
                        "done query batch  len {}: {}  {:.3}ms",
                        batch.len(),
                        qres.is_ok(),
                        dt.as_secs_f32() * 1e3
                    );
                    if dt > Duration::from_millis(5000) {
                        let mut out = String::from("[");
                        for s in &batch {
                            if out.len() > 1 {
                                out.push_str(", ");
                            }
                            out.push('\'');
                            out.push_str(s);
                            out.push('\'');
                        }
                        out.push(']');
                        eprintln!("VERY SLOW QUERY\n{out}");
                    }
                    pgc_tx.send((pgc, qu_select_multi)).await.unwrap();
                    (batch, qres)
                }
            })
            .buffer_unordered(SEARCH_DB_PIPELINE_LEN);
        let mut resdiff = 0;
        let mut stream = Box::pin(stream);
        while let Some((batch, pgres)) = stream.next().await {
            match pgres {
                Ok(rows) => {
                    if rows.len() > batch.len() {
                        error!("MORE RESULTS THAN INPUT");
                    } else if rows.len() < batch.len() {
                        resdiff += batch.len() - rows.len();
                    }
                    let nbatch = batch.len();
                    trace_batch!("received results {}  resdiff {}", rows.len(), resdiff);
                    SEARCH_RES_0_COUNT.fetch_add(rows.len(), atomic::Ordering::AcqRel);
                    let items = transform_pgres(rows);
                    let names: HashMap<_, _> = items.iter().map(|x| (&x.channel, true)).collect();
                    let mut to_add = Vec::new();
                    for s in batch {
                        if !names.contains_key(&s) {
                            let item = FindIocRes {
                                channel: s,
                                response_addr: None,
                                addr: None,
                                dt: Duration::from_millis(0),
                            };
                            to_add.push(item);
                        }
                    }
                    SEARCH_RES_1_COUNT.fetch_add(items.len(), atomic::Ordering::AcqRel);
                    SEARCH_RES_2_COUNT.fetch_add(to_add.len(), atomic::Ordering::AcqRel);
                    let mut items = items;
                    items.extend(to_add.into_iter());
                    if items.len() != nbatch {
                        error!("STILL NOT MATCHING LEN");
                    }
                    SEARCH_RES_3_COUNT.fetch_add(items.len(), atomic::Ordering::AcqRel);
                    let x = tx.send(DaemonEvent::SearchDone(Ok(items))).await;
                    match x {
                        Ok(_) => {}
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
    };
    let jh = taskrun::spawn(fut);
    (qtx, jh)
}