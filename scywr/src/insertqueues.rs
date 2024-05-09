use crate::iteminsertqueue::QueryItem;
use crate::senderpolling::SenderPolling;
use async_channel::Receiver;
use async_channel::Sender;
use err::thiserror;
use err::ThisError;
use netpod::log::*;
use pin_project::pin_project;
use std::collections::VecDeque;
use std::pin::Pin;

#[derive(Debug, ThisError)]
pub enum Error {
    QueuePush,
}

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

    pub fn clone2(&self) -> Self {
        self.clone()
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

    pub fn log_summary(&self) {
        let summ = InsertDequesSummary {
            st_rf1_len: self.st_rf1_rx.len(),
            st_rf3_len: self.st_rf3_rx.len(),
            mt_rf3_len: self.mt_rf3_rx.len(),
            lt_rf3_len: self.lt_rf3_rx.len(),
        };
        info!("{summ:?}");
    }

    // Should be used only for connection and channel status items.
    // It encapsulates the decision to which queue(s) we want to send these kind of items.
    pub fn emit_status_item(&mut self, item: QueryItem) -> Result<(), Error> {
        self.lt_rf3_rx.push_back(item);
        Ok(())
    }
}

#[derive(Debug)]
#[allow(unused)]
struct InsertDequesSummary {
    st_rf1_len: usize,
    st_rf3_len: usize,
    mt_rf3_len: usize,
    lt_rf3_len: usize,
}

#[pin_project]
pub struct InsertSenderPolling {
    #[pin]
    pub st_rf1_sp: SenderPolling<VecDeque<QueryItem>>,
    #[pin]
    pub st_rf3_sp: SenderPolling<VecDeque<QueryItem>>,
    #[pin]
    pub mt_rf3_sp: SenderPolling<VecDeque<QueryItem>>,
    #[pin]
    pub lt_rf3_sp: SenderPolling<VecDeque<QueryItem>>,
}

impl InsertSenderPolling {
    pub fn new(iqtx: InsertQueuesTx) -> Self {
        Self {
            st_rf1_sp: SenderPolling::new(iqtx.st_rf1_tx),
            st_rf3_sp: SenderPolling::new(iqtx.st_rf3_tx),
            mt_rf3_sp: SenderPolling::new(iqtx.mt_rf3_tx),
            lt_rf3_sp: SenderPolling::new(iqtx.lt_rf3_tx),
        }
    }

    pub fn is_idle(&self) -> bool {
        self.st_rf1_sp.is_idle() && self.st_rf3_sp.is_idle() && self.mt_rf3_sp.is_idle() && self.lt_rf3_sp.is_idle()
    }

    pub fn st_rf1_sp_pin(self: Pin<&mut Self>) -> Pin<&mut SenderPolling<VecDeque<QueryItem>>> {
        self.project().st_rf1_sp
    }

    pub fn st_rf3_sp_pin(self: Pin<&mut Self>) -> Pin<&mut SenderPolling<VecDeque<QueryItem>>> {
        self.project().st_rf3_sp
    }

    pub fn mt_rf3_sp_pin(self: Pin<&mut Self>) -> Pin<&mut SenderPolling<VecDeque<QueryItem>>> {
        self.project().mt_rf3_sp
    }

    pub fn lt_rf3_sp_pin(self: Pin<&mut Self>) -> Pin<&mut SenderPolling<VecDeque<QueryItem>>> {
        self.project().lt_rf3_sp
    }

    pub fn __st_rf1_sp_pin(self: Pin<&mut Self>) -> Pin<&mut SenderPolling<VecDeque<QueryItem>>> {
        if true {
            panic!("encapsulated by pin_project");
        }
        unsafe { self.map_unchecked_mut(|x| &mut x.st_rf1_sp) }
    }

    pub fn log_summary(&self) {
        let summ = InsertSenderPollingSummary {
            st_rf1_idle: self.st_rf1_sp.is_idle(),
            st_rf3_idle: self.st_rf3_sp.is_idle(),
            mt_rf3_idle: self.mt_rf3_sp.is_idle(),
            lt_rf3_idle: self.lt_rf3_sp.is_idle(),
        };
        info!("{summ:?}");
    }
}

#[derive(Debug)]
#[allow(unused)]
struct InsertSenderPollingSummary {
    st_rf1_idle: bool,
    st_rf3_idle: bool,
    mt_rf3_idle: bool,
    lt_rf3_idle: bool,
}
