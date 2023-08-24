use async_channel::Receiver;
use netpod::log::*;
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
    let fut2 = async move {
        let mut all = Vec::new();
        let mut do_emit = false;
        loop {
            if do_emit {
                do_emit = false;
                let batch = std::mem::replace(&mut all, Vec::new());
                match batch_tx.send(batch).await {
                    Ok(_) => {}
                    Err(e) => {
                        error!("can not send batch");
                        all = e.0;
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
                    Err(e) => {
                        error!("error in batcher, no more input {e}");
                        break;
                    }
                },
                Err(e) => {
                    let _e: tokio::time::error::Elapsed = e;
                    if all.len() > 0 {
                        do_emit = true;
                    }
                }
            }
        }
        warn!("--------   batcher is done   --------------");
    };
    (batch_rx, tokio::spawn(fut2))
}
