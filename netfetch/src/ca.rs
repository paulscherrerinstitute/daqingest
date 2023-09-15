pub mod conn;
pub mod connset;
pub mod connset_input_merge;
pub mod finder;
pub mod findioc;
pub mod proto;
pub mod search;
pub mod statemap;

use crate::metrics::ExtraInsertsConf;
use futures_util::Future;
use futures_util::FutureExt;
use log::*;
use std::pin::Pin;
use std::sync::atomic::AtomicU32;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;
use taskrun::tokio;

pub static SIGINT: AtomicU32 = AtomicU32::new(0);

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
