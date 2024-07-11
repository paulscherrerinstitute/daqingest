use crate::daemon::PRINT_ACTIVE_INTERVAL;
use async_channel::Receiver;
use async_channel::Sender;
use log::*;
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
                // TODO match on the QueryItem itself
                let shape_kind = 0 as u8;
                histo
                    .entry(item.series.clone())
                    .and_modify(|(c, msp, lsp, _shape_kind)| {
                        *c += 1;
                        *msp = item.ts_msp;
                        *lsp = item.ts_lsp;
                        // TODO should check that shape_kind stays the same.
                    })
                    .or_insert((1 as u64, item.ts_msp, item.ts_lsp, shape_kind));
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
                .map(|(k, (c, msp, lsp, shape_kind))| (u64::MAX - *c, k.clone(), *msp, *lsp, *shape_kind))
                .collect();
            all.sort_unstable();
            info!("Active scalar");
            for (c, sid, msp, lsp, _shape_kind) in all.iter().filter(|x| x.4 == 0).take(6) {
                info!("{:10}  {:20}  {:14}  {:?}", u64::MAX - c, msp.to_u64(), lsp.ns(), sid);
            }
            info!("Active wave");
            for (c, sid, msp, lsp, _shape_kind) in all.iter().filter(|x| x.4 == 1).take(6) {
                info!("{:10}  {:20}  {:14}  {:?}", u64::MAX - c, msp.to_u64(), lsp.ns(), sid);
            }
            histo.clear();
        }
    }
    info!("insert queue adapter ended");
}

pub fn active_channel_insert_hook(inp: Receiver<QueryItem>) -> Receiver<QueryItem> {
    let (tx, rx) = async_channel::bounded(inp.capacity().unwrap_or(256));
    tokio::spawn(active_channel_insert_hook_worker(inp, tx));
    rx
}
