pub mod conn;
pub mod connset;
pub mod connset_consume;
pub mod findioc;
pub mod proto;
pub mod search;

use self::connset::CaConnSetCtrl;
use crate::ca::connset::CaConnSet;
use crate::metrics::ExtraInsertsConf;
use crate::rt::TokMx;
use futures_util::Future;
use futures_util::FutureExt;
use log::*;
use netpod::Database;
use scywr::insertworker::InsertWorkerOpts;
use scywr::iteminsertqueue::CommonInsertItemQueue;
use scywr::store::DataStore;
use stats::CaConnStatsAgg;
use std::pin::Pin;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;
use taskrun::tokio;

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

fn handler_sigaction(_a: libc::c_int, _b: *const libc::siginfo_t, _c: *const libc::c_void) {
    crate::ca::SIGINT.store(1, Ordering::Release);
    let _ = ingest_linux::signal::unset_signal_handler(libc::SIGINT);
}
