use crate::daemon::PRINT_ACTIVE_INTERVAL;
use async_channel::Receiver;
use async_channel::Sender;
use log::*;
use netpod::Shape;
use scywr::iteminsertqueue::QueryItem;
use std::collections::BTreeMap;
use std::time::Instant;
use taskrun::tokio;

pub async fn active_channel_insert_hook_worker(rx: Receiver<QueryItem>, tx: Sender<QueryItem>) {
    // let rx = common_insert_item_queue
    //     .receiver()
    //     .ok_or_else(|| Error::with_msg_no_trace("can not derive receiver for insert queue adapter"))?;
    // let tx = common_insert_item_queue_2
    //     .sender()
    //     .ok_or_else(|| Error::with_msg_no_trace("can not derive sender for insert queue adapter"))?;
    // let insert_queue_counter = insert_queue_counter.clone();
    // let common_insert_item_queue_2 = common_insert_item_queue_2.clone();
    let mut printed_last = Instant::now();
    let mut histo = BTreeMap::new();
    while let Ok(item) = rx.recv().await {
        // TODO collect stats
        // insert_queue_counter.fetch_add(1, atomic::Ordering::AcqRel);
        //trace!("insert queue item {item:?}");
        match &item {
            QueryItem::Insert(item) => {
                let shape_kind = match &item.shape {
                    Shape::Scalar => 0 as u32,
                    Shape::Wave(_) => 1,
                    Shape::Image(_, _) => 2,
                };
                histo
                    .entry(item.series.clone())
                    .and_modify(|(c, msp, lsp, pulse, _shape_kind)| {
                        *c += 1;
                        *msp = item.ts_msp;
                        *lsp = item.ts_lsp;
                        *pulse = item.pulse;
                        // TODO should check that shape_kind stays the same.
                    })
                    .or_insert((0 as usize, item.ts_msp, item.ts_lsp, item.pulse, shape_kind));
            }
            _ => {}
        }
        match tx.send(item).await {
            Ok(_) => {}
            Err(e) => {
                error!("insert queue hook send {e}");
                break;
            }
        }
        let tsnow = Instant::now();
        if tsnow.duration_since(printed_last) >= PRINT_ACTIVE_INTERVAL {
            printed_last = tsnow;
            let mut all: Vec<_> = histo
                .iter()
                .map(|(k, (c, msp, lsp, pulse, shape_kind))| {
                    (usize::MAX - *c, k.clone(), *msp, *lsp, *pulse, *shape_kind)
                })
                .collect();
            all.sort_unstable();
            info!("Active scalar");
            for (c, sid, msp, lsp, pulse, _shape_kind) in all.iter().filter(|x| x.5 == 0).take(6) {
                info!("{:10}  {:20}  {:14}  {:20}  {:?}", usize::MAX - c, msp, lsp, pulse, sid);
            }
            info!("Active wave");
            for (c, sid, msp, lsp, pulse, _shape_kind) in all.iter().filter(|x| x.5 == 1).take(6) {
                info!("{:10}  {:20}  {:14}  {:20}  {:?}", usize::MAX - c, msp, lsp, pulse, sid);
            }
            histo.clear();
        }
    }
    info!("insert queue adapter ended");
}

pub fn active_channel_insert_hook(inp: Receiver<QueryItem>) -> Receiver<QueryItem> {
    let (tx, rx) = async_channel::bounded(256);
    tokio::spawn(active_channel_insert_hook_worker(inp, tx));
    rx
}
