use async_channel::Receiver;
use async_channel::Sender;
use log::*;
use std::time::Duration;
use taskrun::tokio;

pub fn batch<T>(
    batch_limit: usize,
    timeout: Duration,
    outcap: usize,
    rx: Receiver<T>,
) -> (Receiver<Vec<T>>, tokio::task::JoinHandle<()>)
where
    T: Send + 'static,
{
    let (batch_tx, batch_rx) = async_channel::bounded(outcap);
    (batch_rx, tokio::spawn(run_batcher(rx, batch_tx, batch_limit, timeout)))
}

async fn run_batcher<T>(rx: Receiver<T>, batch_tx: Sender<Vec<T>>, batch_limit: usize, timeout: Duration) {
    let mut all = Vec::new();
    let mut do_emit = false;
    loop {
        use tokio::time::error::Elapsed;
        if do_emit {
            do_emit = false;
            let batch = std::mem::replace(&mut all, Vec::new());
            match tokio::time::timeout(Duration::from_millis(1000), batch_tx.send(batch)).await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    error!("can not send batch");
                    all = e.0;
                }
                Err(_) => {
                    trace!("--------------------------   send timeout")
                }
            }
        }
        match tokio::time::timeout(timeout, rx.recv()).await {
            Ok(k) => match k {
                Ok(item) => {
                    all.push(item);
                    if all.len() >= batch_limit {
                        do_emit = true;
                    }
                }
                Err(_e) => {
                    break;
                }
            },
            Err(e) => {
                let _: Elapsed = e;
                // trace!("--------------------------    batcher timeout  rx len {}", rx.len());
                if all.len() > 0 {
                    do_emit = true;
                }
            }
        }
    }
    debug!("--------   batcher is done   --------------");
}
