use crate::daemon::CURRENT_SEARCH_PENDING_MAX;
use crate::daemon::SEARCH_BATCH_MAX;
use crate::daemon::SEARCH_DB_PIPELINE_LEN;
use crate::daemon::SEARCH_REQ_BATCH_RECV_COUNT;
use crate::daemon::SEARCH_RES_0_COUNT;
use crate::daemon::SEARCH_RES_1_COUNT;
use crate::daemon::SEARCH_RES_2_COUNT;
use crate::daemon::SEARCH_RES_3_COUNT;
use async_channel::Receiver;
use async_channel::Sender;
use dbpg::conn::make_pg_client;
use dbpg::postgres::Row as PgRow;
use err::Error;
use log::*;
use netfetch::ca::findioc::FindIocRes;
use netfetch::daemon_common::DaemonEvent;
use netpod::Database;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::atomic;
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

async fn finder_worker_single(
    inp: Receiver<Vec<String>>,
    tx: Sender<DaemonEvent>,
    backend: String,
    db: Database,
) -> Result<(), Error> {
    let pg = make_pg_client(&db)
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
                SEARCH_REQ_BATCH_RECV_COUNT.fetch_add(batch.len(), atomic::Ordering::AcqRel);
                let ts1 = Instant::now();
                debug_batch!("run  query batch  len {}", batch.len());
                let qres = pg.query(&qu_select_multi, &[&backend, &batch]).await;
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
                match qres {
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
            Err(_e) => break,
        }
    }
    Ok(())
}

async fn finder_worker(
    qrx: Receiver<String>,
    tx: Sender<DaemonEvent>,
    backend: String,
    db: Database,
) -> Result<(), Error> {
    // TODO do something with join handle
    let (batch_rx, _jh) = batchtools::batcher::batch(
        SEARCH_BATCH_MAX,
        Duration::from_millis(200),
        SEARCH_DB_PIPELINE_LEN,
        qrx,
    );
    for _ in 0..SEARCH_DB_PIPELINE_LEN {
        tokio::spawn(finder_worker_single(
            batch_rx.clone(),
            tx.clone(),
            backend.clone(),
            db.clone(),
        ));
    }
    Ok(())
}

pub fn start_finder(
    tx: Sender<DaemonEvent>,
    backend: String,
    db: Database,
) -> (Sender<String>, tokio::task::JoinHandle<Result<(), Error>>) {
    let (qtx, qrx) = async_channel::bounded(CURRENT_SEARCH_PENDING_MAX);
    let jh = taskrun::spawn(finder_worker(qrx, tx, backend, db));
    (qtx, jh)
}
