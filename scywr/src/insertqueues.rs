use crate::iteminsertqueue::QueryItem;
use async_channel::Receiver;
use async_channel::Sender;
use std::collections::VecDeque;

#[derive(Clone)]
pub struct InsertQueuesTx {
    pub st_rf1_tx: Sender<VecDeque<QueryItem>>,
    pub st_rf3_tx: Sender<VecDeque<QueryItem>>,
    pub mt_rf3_tx: Sender<VecDeque<QueryItem>>,
    pub lt_rf3_tx: Sender<VecDeque<QueryItem>>,
}

impl InsertQueuesTx {
    /// Send all accumulated batches
    pub async fn send_all(&mut self, iqdqs: &mut InsertDeques) -> Result<(), ()> {
        // Send each buffer down the corresponding channel
        let item = core::mem::replace(&mut iqdqs.st_rf1_rx, VecDeque::new());
        self.st_rf1_tx.send(item).await.map_err(|_| ())?;
        let item = core::mem::replace(&mut iqdqs.st_rf3_rx, VecDeque::new());
        self.st_rf3_tx.send(item).await.map_err(|_| ())?;
        let item = core::mem::replace(&mut iqdqs.mt_rf3_rx, VecDeque::new());
        self.mt_rf3_tx.send(item).await.map_err(|_| ())?;
        let item = core::mem::replace(&mut iqdqs.lt_rf3_rx, VecDeque::new());
        self.lt_rf3_tx.send(item).await.map_err(|_| ())?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct InsertQueuesRx {
    pub st_rf1_rx: Receiver<VecDeque<QueryItem>>,
    pub st_rf3_rx: Receiver<VecDeque<QueryItem>>,
    pub mt_rf3_rx: Receiver<VecDeque<QueryItem>>,
    pub lt_rf3_rx: Receiver<VecDeque<QueryItem>>,
}

pub struct InsertDeques {
    pub st_rf1_rx: VecDeque<QueryItem>,
    pub st_rf3_rx: VecDeque<QueryItem>,
    pub mt_rf3_rx: VecDeque<QueryItem>,
    pub lt_rf3_rx: VecDeque<QueryItem>,
}

impl InsertDeques {
    pub fn new() -> Self {
        Self {
            st_rf1_rx: VecDeque::new(),
            st_rf3_rx: VecDeque::new(),
            mt_rf3_rx: VecDeque::new(),
            lt_rf3_rx: VecDeque::new(),
        }
    }

    /// Total number of items cumulated over all queues.
    pub fn len(&self) -> usize {
        self.st_rf1_rx.len() + self.st_rf3_rx.len() + self.mt_rf3_rx.len() + self.lt_rf3_rx.len()
    }

    ///
    pub fn clear(&mut self) {
        self.st_rf1_rx.clear();
        self.st_rf3_rx.clear();
        self.mt_rf3_rx.clear();
        self.lt_rf3_rx.clear();
    }
}
