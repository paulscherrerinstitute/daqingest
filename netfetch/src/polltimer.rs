use futures_util::Stream;
use futures_util::StreamExt;
use log::*;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;

pub struct PollTimer<INP> {
    inp: INP,
    timeout_warn: Duration,
}

impl<INP> PollTimer<INP> {
    pub fn new(inp: INP, timeout_warn: Duration) -> Self {
        Self { inp, timeout_warn }
    }
}

impl<INP> Stream for PollTimer<INP>
where
    INP: Stream + Unpin,
{
    type Item = <INP as Stream>::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let poll_ts1 = Instant::now();
        let inp = &mut self.inp;
        let ret = inp.poll_next_unpin(cx);
        let poll_ts2 = Instant::now();
        let dt = poll_ts2.saturating_duration_since(poll_ts1);
        if dt > self.timeout_warn {
            warn!("long poll duration {:.0} ms", dt.as_secs_f32() * 1e3)
        }
        ret
    }
}
