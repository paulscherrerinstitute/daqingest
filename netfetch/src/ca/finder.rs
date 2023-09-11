use super::connset::CaConnSetEvent;
use super::connset::IocAddrQuery;
use super::connset::CURRENT_SEARCH_PENDING_MAX;
use super::connset::SEARCH_BATCH_MAX;
use crate::ca::findioc::FindIocRes;
use crate::ca::findioc::FindIocStream;
use crate::daemon_common::DaemonEvent;
use async_channel::Receiver;
use async_channel::Sender;
use dbpg::conn::make_pg_client;
use dbpg::postgres::Row as PgRow;
use err::Error;
use futures_util::FutureExt;
use futures_util::StreamExt;
use log::*;
use netpod::Database;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::net::SocketAddrV4;
use std::sync::atomic;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;
use std::time::Instant;
use taskrun::tokio;
use tokio::task::JoinHandle;

const SEARCH_DB_PIPELINE_LEN: usize = 4;
const FINDER_JOB_QUEUE_LEN_MAX: usize = 10;
const FINDER_BATCH_SIZE: usize = 8;
const FINDER_IN_FLIGHT_MAX: usize = 800;
const FINDER_TIMEOUT: Duration = Duration::from_millis(100);

// TODO pull out into a stats
static SEARCH_REQ_BATCH_RECV_COUNT: AtomicUsize = AtomicUsize::new(0);
static SEARCH_RES_0_COUNT: AtomicUsize = AtomicUsize::new(0);
static SEARCH_RES_1_COUNT: AtomicUsize = AtomicUsize::new(0);
static SEARCH_RES_2_COUNT: AtomicUsize = AtomicUsize::new(0);
static SEARCH_RES_3_COUNT: AtomicUsize = AtomicUsize::new(0);

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
    inp: Receiver<Vec<IocAddrQuery>>,
    tx: Sender<VecDeque<FindIocRes>>,
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
                let names: Vec<_> = batch.iter().map(|x| x.name.as_str()).collect();
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
                        out.push_str(&e.name);
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
                        for e in batch {
                            let s = e.name;
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
                        let x = tx.send(items).await;
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
    qrx: Receiver<IocAddrQuery>,
    tx: Sender<VecDeque<FindIocRes>>,
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
        // TODO use join handle
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
    tx: Sender<VecDeque<FindIocRes>>,
    backend: String,
    db: Database,
) -> (Sender<IocAddrQuery>, JoinHandle<Result<(), Error>>) {
    let (qtx, qrx) = async_channel::bounded(CURRENT_SEARCH_PENDING_MAX);
    let jh = taskrun::spawn(finder_worker(qrx, tx, backend, db));
    (qtx, jh)
}

struct OptFut<F> {
    fut: Option<F>,
}

impl<F> OptFut<F> {
    fn empty() -> Self {
        Self { fut: None }
    }

    fn new(fut: F) -> Self {
        Self { fut: Some(fut) }
    }

    fn is_enabled(&self) -> bool {
        self.fut.is_some()
    }
}

impl<F> futures_util::Future for OptFut<F>
where
    F: futures_util::Future + std::marker::Unpin,
{
    type Output = <F as futures_util::Future>::Output;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context) -> std::task::Poll<Self::Output> {
        match self.fut.as_mut() {
            Some(fut) => fut.poll_unpin(cx),
            None => std::task::Poll::Pending,
        }
    }
}

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
