use crate::iteminsertqueue::Accounting;
use crate::iteminsertqueue::AccountingRecv;
use crate::iteminsertqueue::QueryItem;
use crate::senderpolling::SenderPolling;
use async_channel::Receiver;
use async_channel::Sender;
use core::fmt;
use netpod::ttl::RetentionTime;
use pin_project::pin_project;
use std::collections::VecDeque;
use std::pin::Pin;

autoerr::create_error_v1!(
    name(Error, "ScyllaInsertQueue"),
    enum variants {
        QueuePush,
        ChannelSend(RetentionTime, u8),
    },
);

#[derive(Clone)]
pub struct InsertQueuesTx {
    pub st_rf1_tx: Sender<VecDeque<QueryItem>>,
    pub st_rf3_tx: Sender<VecDeque<QueryItem>>,
    pub mt_rf3_tx: Sender<VecDeque<QueryItem>>,
    pub lt_rf3_tx: Sender<VecDeque<QueryItem>>,
}

impl InsertQueuesTx {
    /// Send all accumulated batches
    pub async fn send_all(&mut self, iqdqs: &mut InsertDeques) -> Result<(), Error> {
        // Send each buffer down the corresponding channel
        if false {
            let item = core::mem::replace(&mut iqdqs.st_rf1_qu, VecDeque::new());
            self.st_rf1_tx
                .send(item)
                .await
                .map_err(|_| Error::ChannelSend(RetentionTime::Short, 1))?;
        }
        {
            let item = core::mem::replace(&mut iqdqs.st_rf3_qu, VecDeque::new());
            self.st_rf3_tx
                .send(item)
                .await
                .map_err(|_| Error::ChannelSend(RetentionTime::Short, 3))?;
        }
        {
            let item = core::mem::replace(&mut iqdqs.mt_rf3_qu, VecDeque::new());
            self.mt_rf3_tx
                .send(item)
                .await
                .map_err(|_| Error::ChannelSend(RetentionTime::Medium, 3))?;
        }
        {
            let item = core::mem::replace(&mut iqdqs.lt_rf3_qu, VecDeque::new());
            self.lt_rf3_tx
                .send(item)
                .await
                .map_err(|_| Error::ChannelSend(RetentionTime::Long, 3))?;
        }
        Ok(())
    }

    pub fn clone2(&self) -> Self {
        self.clone()
    }

    pub fn summary(&self) -> InsertQueuesTxSummary {
        InsertQueuesTxSummary { obj: self }
    }
}

pub struct InsertQueuesTxSummary<'a> {
    obj: &'a InsertQueuesTx,
}

impl<'a> fmt::Display for InsertQueuesTxSummary<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let obj = self.obj;
        write!(
            fmt,
            "InsertQueuesTx {{ st_rf1_tx: {} {} {}, st_rf3_tx: {} {} {}, mt_rf3_tx: {} {} {}, lt_rf3_tx: {} {} {} }}",
            obj.st_rf1_tx.is_closed(),
            obj.st_rf1_tx.is_full(),
            obj.st_rf1_tx.len(),
            obj.st_rf3_tx.is_closed(),
            obj.st_rf3_tx.is_full(),
            obj.st_rf3_tx.len(),
            obj.mt_rf3_tx.is_closed(),
            obj.mt_rf3_tx.is_full(),
            obj.mt_rf3_tx.len(),
            obj.lt_rf3_tx.is_closed(),
            obj.lt_rf3_tx.is_full(),
            obj.lt_rf3_tx.len(),
        )
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
    pub st_rf1_qu: VecDeque<QueryItem>,
    pub st_rf3_qu: VecDeque<QueryItem>,
    pub mt_rf3_qu: VecDeque<QueryItem>,
    pub lt_rf3_qu: VecDeque<QueryItem>,
}

impl InsertDeques {
    pub fn new() -> Self {
        Self {
            st_rf1_qu: VecDeque::new(),
            st_rf3_qu: VecDeque::new(),
            mt_rf3_qu: VecDeque::new(),
            lt_rf3_qu: VecDeque::new(),
        }
    }

    /// Total number of items cumulated over all queues.
    pub fn len(&self) -> usize {
        self.st_rf1_qu.len() + self.st_rf3_qu.len() + self.mt_rf3_qu.len() + self.lt_rf3_qu.len()
    }

    pub fn clear(&mut self) {
        self.st_rf1_qu.clear();
        self.st_rf3_qu.clear();
        self.mt_rf3_qu.clear();
        self.lt_rf3_qu.clear();
    }

    pub fn summary(&self) -> InsertDequesSummary {
        InsertDequesSummary { obj: self }
    }

    // Should be used only for connection and channel status items.
    // It encapsulates the decision to which queue(s) we want to send these kind of items.
    pub fn emit_accounting_item(&mut self, rt: RetentionTime, item: Accounting) -> Result<(), Error> {
        self.deque(rt).push_back(QueryItem::Accounting(item));
        Ok(())
    }

    // Should be used only for connection and channel status items.
    // It encapsulates the decision to which queue(s) we want to send these kind of items.
    pub fn emit_accounting_recv(&mut self, item: AccountingRecv) -> Result<(), Error> {
        self.deque(RetentionTime::Short)
            .push_back(QueryItem::AccountingRecv(item));
        Ok(())
    }

    pub fn deque(&mut self, rt: RetentionTime) -> &mut VecDeque<QueryItem> {
        match rt {
            RetentionTime::Short => &mut self.st_rf3_qu,
            RetentionTime::Medium => &mut self.mt_rf3_qu,
            RetentionTime::Long => &mut self.lt_rf3_qu,
        }
    }

    pub fn housekeeping(&mut self) {
        let qus = [
            &mut self.st_rf1_qu,
            &mut self.st_rf3_qu,
            &mut self.mt_rf3_qu,
            &mut self.lt_rf3_qu,
        ];
        for qu in qus {
            if qu.len() * 2 < qu.capacity() {
                qu.truncate(qu.capacity() * 3 / 4);
            }
        }
    }
}

pub struct InsertDequesSummary<'a> {
    obj: &'a InsertDeques,
}

impl<'a> fmt::Display for InsertDequesSummary<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let obj = self.obj;
        write!(
            fmt,
            "InsertDeques {{ st_rf1_len: {}, st_rf3_len: {}, mt_rf3_len: {}, lt_rf3_len: {} }}",
            obj.st_rf1_qu.len(),
            obj.st_rf3_qu.len(),
            obj.mt_rf3_qu.len(),
            obj.lt_rf3_qu.len()
        )
    }
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

    pub fn summary(&self) -> InsertSenderPollingSummary {
        InsertSenderPollingSummary { obj: self }
    }
}

pub struct InsertSenderPollingSummary<'a> {
    obj: &'a InsertSenderPolling,
}

impl<'a> fmt::Display for InsertSenderPollingSummary<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let obj = self.obj;
        write!(
            fmt,
            "InsertSenderPolling {{ st_rf1_idle_len: {:?} {:?}, st_rf3_idle_len: {:?} {:?}, mt_rf3_idle_len: {:?} {:?}, lt_rf3_idle_len: {:?} {:?} }}",
            obj.st_rf1_sp.is_idle(),
            obj.st_rf1_sp.len(),
            obj.st_rf3_sp.is_idle(),
            obj.st_rf3_sp.len(),
            obj.mt_rf3_sp.is_idle(),
            obj.mt_rf3_sp.len(),
            obj.lt_rf3_sp.is_idle(),
            obj.lt_rf3_sp.len(),
        )
    }
}
