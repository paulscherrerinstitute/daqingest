use async_channel::Receiver;
use async_channel::Sender;
use log::*;
use netpod::timeunits::MS;
use netpod::timeunits::SEC;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

// TODO allow a trait to tell worker whether drop is allowed or not.

async fn rate_limiter_worker<T>(
    rate: Arc<AtomicU64>,
    inp: Receiver<T>,
    tx: Sender<T>,
    // stats: Arc<stats::InsertWorkerStats>,
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
        let allowed_to_drop = false;
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
            // stats.ratelimit_drop().inc();
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

pub fn rate_limiter<T: Send + 'static>(
    rate: Arc<AtomicU64>,
    inp: Receiver<T>,
    // stats: Arc<stats::InsertWorkerStats>,
) -> Receiver<T> {
    let (tx, rx) = async_channel::bounded(inp.capacity().unwrap_or(256));
    taskrun::spawn(rate_limiter_worker(rate, inp, tx));
    rx
}
