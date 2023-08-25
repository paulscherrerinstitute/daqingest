pub mod conn;
pub mod connset;
pub mod findioc;
pub mod proto;
pub mod search;

use crate::ca::connset::CaConnSet;
use crate::errconv::ErrConv;
use crate::metrics::ExtraInsertsConf;
use crate::rt::TokMx;
use err::Error;
use futures_util::Future;
use futures_util::FutureExt;
use log::*;
use netpod::Database;
use scywr::insertworker::InsertWorkerOpts;
use scywr::iteminsertqueue::CommonInsertItemQueue;
use scywr::store::DataStore;
use stats::CaConnStatsAgg;
use std::net::SocketAddrV4;
use std::pin::Pin;
use std::sync::atomic;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;
use taskrun::tokio;
use tokio_postgres::Client as PgClient;

pub static SIGINT: AtomicU32 = AtomicU32::new(0);

lazy_static::lazy_static! {
    pub static ref METRICS: Mutex<Option<CaConnStatsAgg>> = Mutex::new(None);
}

pub struct IngestCommons {
    pub pgconf: Arc<Database>,
    pub backend: String,
    pub local_epics_hostname: String,
    pub insert_item_queue: Arc<CommonInsertItemQueue>,
    pub data_store: Arc<DataStore>,
    pub insert_ivl_min: Arc<AtomicU64>,
    pub extra_inserts_conf: TokMx<ExtraInsertsConf>,
    pub insert_frac: Arc<AtomicU64>,
    pub store_workers_rate: Arc<AtomicU64>,
    pub ca_conn_set: CaConnSet,
    pub insert_workers_running: Arc<AtomicU64>,
}

impl From<&IngestCommons> for InsertWorkerOpts {
    fn from(val: &IngestCommons) -> Self {
        Self {
            store_workers_rate: val.store_workers_rate.clone(),
            insert_workers_running: val.insert_workers_running.clone(),
            insert_frac: val.insert_frac.clone(),
        }
    }
}

pub trait SlowWarnable {
    fn slow_warn(self, ms: u64) -> SlowWarn<Pin<Box<Self>>>
    where
        Self: Sized;
}

impl<F> SlowWarnable for F
where
    F: Future,
{
    fn slow_warn(self, ms: u64) -> SlowWarn<Pin<Box<Self>>>
    where
        Self: Sized,
    {
        SlowWarn::new(ms, Box::pin(self))
    }
}

pub struct SlowWarn<F> {
    ms: u64,
    fut: F,
    timeout: Option<Option<Pin<Box<tokio::time::Sleep>>>>,
    first_poll: Option<Instant>,
}

impl<F> SlowWarn<F>
where
    F: Future + Unpin,
{
    pub fn new(ms: u64, fut: F) -> Self {
        Self {
            ms,
            fut,
            timeout: None,
            first_poll: None,
        }
    }

    fn poll_fut(mut self: Pin<&mut Self>, cx: &mut std::task::Context) -> Poll<<F as Future>::Output> {
        use Poll::*;
        match self.fut.poll_unpin(cx) {
            Ready(x) => {
                if let Some(None) = &self.timeout {
                    let dt = self.first_poll.take().unwrap().elapsed();
                    warn!("---------   Completed in {}ms   ----------", dt.as_secs_f32());
                }
                Ready(x)
            }
            Pending => Pending,
        }
    }
}

impl<F> Future for SlowWarn<F>
where
    F: Future + Unpin,
{
    type Output = <F as Future>::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context) -> Poll<Self::Output> {
        use Poll::*;
        if self.first_poll.is_none() {
            self.first_poll = Some(Instant::now());
        }
        let a = self.timeout.as_mut();
        match a {
            Some(x) => match x {
                Some(x) => match x.poll_unpin(cx) {
                    Ready(()) => {
                        warn!("----------------   SlowWarn   ---------------------");
                        self.timeout = Some(None);
                        Self::poll_fut(self, cx)
                    }
                    Pending => Self::poll_fut(self, cx),
                },
                None => Self::poll_fut(self, cx),
            },
            None => {
                self.timeout = Some(Some(Box::pin(tokio::time::sleep(Duration::from_millis(self.ms)))));
                cx.waker().wake_by_ref();
                Self::poll_fut(self, cx)
            }
        }
    }
}

pub async fn find_channel_addr(
    backend: String,
    name: String,
    pgconf: &Database,
) -> Result<Option<SocketAddrV4>, Error> {
    // TODO also here, provide a db pool.
    let d = pgconf;
    let (pg_client, pg_conn) = tokio_postgres::connect(
        &format!("postgresql://{}:{}@{}:{}/{}", d.user, d.pass, d.host, d.port, d.name),
        tokio_postgres::tls::NoTls,
    )
    .await
    .unwrap();
    // TODO allow clean shutdown on ctrl-c and join the pg_conn in the end:
    tokio::spawn(async {
        pg_conn.await.unwrap();
        info!("drop pg conn after find_channel_addr");
    });
    let pg_client = Arc::new(pg_client);
    let qu_find_addr = pg_client
        .prepare(
            "select t1.facility, t1.channel, t1.addr from ioc_by_channel_log t1 where t1.facility = $1 and t1.channel = $2 and addr is not null order by tsmod desc limit 1",
        )
        .await
        .err_conv()?;
    let rows = pg_client.query(&qu_find_addr, &[&backend, &name]).await.err_conv()?;
    if rows.is_empty() {
        error!("can not find any addresses of channels {:?}", name);
        Err(Error::with_msg_no_trace(format!("no address for channel {}", name)))
    } else {
        for row in rows {
            match row.try_get::<_, &str>(2) {
                Ok(addr) => match addr.parse::<SocketAddrV4>() {
                    Ok(addr) => return Ok(Some(addr)),
                    Err(e) => {
                        error!("can not parse  {e:?}");
                        return Err(Error::with_msg_no_trace(format!("no address for channel {}", name)));
                    }
                },
                Err(e) => {
                    error!("can not find addr for {name}  {e:?}");
                }
            }
        }
        Ok(None)
    }
}

#[allow(unused)]
async fn query_addr_multiple(pg_client: &PgClient) -> Result<(), Error> {
    let backend: &String = err::todoval();
    // TODO factor the find loop into a separate Stream.
    let qu_find_addr = pg_client
        .prepare("with q1 as (select t1.facility, t1.channel, t1.addr from ioc_by_channel_log t1 where t1.facility = $1 and t1.channel in ($2, $3, $4, $5, $6, $7, $8, $9) and t1.addr is not null order by t1.tsmod desc) select distinct on (q1.facility, q1.channel) q1.facility, q1.channel, q1.addr from q1")
        .await
        .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
    let mut chns_todo: &[String] = err::todoval();
    let mut chstmp = ["__NONE__"; 8];
    for (s1, s2) in chns_todo.iter().zip(chstmp.iter_mut()) {
        *s2 = s1;
    }
    chns_todo = &chns_todo[chstmp.len().min(chns_todo.len())..];
    let rows = pg_client
        .query(
            &qu_find_addr,
            &[
                &backend, &chstmp[0], &chstmp[1], &chstmp[2], &chstmp[3], &chstmp[4], &chstmp[5], &chstmp[6],
                &chstmp[7],
            ],
        )
        .await
        .map_err(|e| Error::with_msg_no_trace(format!("pg lookup error: {e:?}")))?;
    for row in rows {
        let ch: &str = row.get(1);
        let addr: &str = row.get(2);
        if addr == "" {
            // TODO the address was searched before but could not be found.
        } else {
            let addr: SocketAddrV4 = match addr.parse() {
                Ok(k) => k,
                Err(e) => {
                    error!("can not parse {addr:?} for channel {ch:?}  {e:?}");
                    continue;
                }
            };
            let _ = addr;
        }
    }
    Ok(())
}

fn handler_sigaction(_a: libc::c_int, _b: *const libc::siginfo_t, _c: *const libc::c_void) {
    crate::ca::SIGINT.store(1, Ordering::Release);
    let _ = crate::linuxhelper::unset_signal_handler(libc::SIGINT);
}
