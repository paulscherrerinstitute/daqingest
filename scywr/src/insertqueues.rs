use crate::iteminsertqueue::QueryItem;
use async_channel::Receiver;
use async_channel::Sender;
use std::collections::VecDeque;

#[derive(Clone)]
pub struct InsertQueuesTx {
    pub st_rf3_tx: Sender<VecDeque<QueryItem>>,
    pub st_rf1_tx: Sender<VecDeque<QueryItem>>,
    pub mt_rf3_tx: Sender<VecDeque<QueryItem>>,
}

#[derive(Clone)]
pub struct InsertQueuesRx {
    pub st_rf3_rx: Receiver<VecDeque<QueryItem>>,
    pub st_rf1_rx: Receiver<VecDeque<QueryItem>>,
    pub mt_rf3_rx: Receiver<VecDeque<QueryItem>>,
}
