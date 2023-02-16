use super::conn::CaConnEvent;
use super::conn::ChannelSetOps;
use super::conn::ConnCommand;
use super::store::DataStore;
use super::SlowWarnable;
use crate::batchquery::series_by_channel::ChannelInfoQuery;
use crate::ca::conn::CaConn;
use crate::ca::conn::CaConnEventValue;
use crate::errconv::ErrConv;
use crate::rt::JoinHandle;
use crate::rt::TokMx;
use crate::series::ChannelStatusSeriesId;
use crate::store::CommonInsertItemQueue;
use crate::store::CommonInsertItemQueueSender;
use async_channel::Receiver;
use async_channel::Sender;
use err::Error;
use futures_util::FutureExt;
use futures_util::StreamExt;
use netpod::log::*;
use stats::CaConnStats;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::net::SocketAddrV4;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tracing::info_span;
use tracing::Instrument;

#[derive(Debug, PartialEq, Eq)]
pub struct CmdId(SocketAddrV4, usize);

pub struct CaConnRess {
    sender: Sender<ConnCommand>,
    channel_set_ops: Arc<ChannelSetOps>,
    stats: Arc<CaConnStats>,
    jh: JoinHandle<Result<(), Error>>,
}

impl CaConnRess {
    pub fn stats(&self) -> &Arc<CaConnStats> {
        &self.stats
    }
}

// TODO
// Resources belonging to the same CaConn also belong together here.
// Only add or remove them from the set at once.
// That means, they should go together.
// Does not hold the actual CaConn, because that struct is in a task.
// Always create the CaConn via a common code path which also takes care
// to add it to the correct list.
// There, make spawning part of this function?
pub struct CaConnSet {
    ca_conn_ress: Arc<TokMx<BTreeMap<SocketAddrV4, CaConnRess>>>,
    conn_item_tx: Sender<(SocketAddrV4, CaConnEvent)>,
    conn_item_rx: Receiver<(SocketAddrV4, CaConnEvent)>,
    channel_info_query_tx: Sender<ChannelInfoQuery>,
}

impl CaConnSet {
    pub fn new(channel_info_query_tx: Sender<ChannelInfoQuery>) -> Self {
        let (conn_item_tx, conn_item_rx) = async_channel::bounded(10000);
        Self {
            ca_conn_ress: Arc::new(TokMx::new(BTreeMap::new())),
            conn_item_tx,
            conn_item_rx,
            channel_info_query_tx,
        }
    }

    pub fn conn_item_rx(&self) -> Receiver<(SocketAddrV4, CaConnEvent)> {
        self.conn_item_rx.clone()
    }

    pub fn ca_conn_ress(&self) -> &TokMx<BTreeMap<SocketAddrV4, CaConnRess>> {
        &self.ca_conn_ress
    }

    pub fn create_ca_conn_2(
        &self,
        backend: String,
        addr: SocketAddrV4,
        local_epics_hostname: String,
        array_truncate: usize,
        insert_queue_max: usize,
        insert_item_queue_sender: CommonInsertItemQueueSender,
        data_store: Arc<DataStore>,
    ) -> Result<CaConnRess, Error> {
        // TODO should we save this as event?
        trace!("create new CaConn  {:?}", addr);
        let conn = CaConn::new(
            backend.clone(),
            addr,
            local_epics_hostname,
            self.channel_info_query_tx.clone(),
            data_store.clone(),
            insert_item_queue_sender,
            array_truncate,
            insert_queue_max,
        );
        let conn = conn;
        let conn_tx = conn.conn_command_tx();
        let conn_stats = conn.stats();
        let channel_set_ops = conn.get_channel_set_ops_map();
        let conn_item_tx = self.conn_item_tx.clone();
        let ca_conn_ress = self.ca_conn_ress.clone();
        let conn_fut = async move {
            let stats = conn.stats();
            let mut conn = conn;
            let mut ret = Ok(());
            while let Some(item) = conn.next().await {
                match item {
                    Ok(item) => {
                        stats.conn_item_count_inc();
                        conn_item_tx.send((addr, item)).await?;
                    }
                    Err(e) => {
                        error!("CaConn gives error: {e:?}");
                        ret = Err(e);
                    }
                }
            }
            info!("CaConn stream ended {}", addr);
            Self::conn_remove(&ca_conn_ress, addr).await?;
            conn_item_tx
                .send((
                    addr,
                    CaConnEvent {
                        ts: Instant::now(),
                        value: CaConnEventValue::EndOfStream,
                    },
                ))
                .await?;
            ret
        };
        let jh = tokio::spawn(conn_fut);
        let ca_conn_ress = CaConnRess {
            sender: conn_tx,
            channel_set_ops,
            stats: conn_stats,
            jh,
        };
        Ok(ca_conn_ress)
    }

    pub async fn enqueue_command_to_all<F>(&self, cmdgen: F) -> Result<Vec<CmdId>, Error>
    where
        F: Fn() -> ConnCommand,
    {
        let mut senders = Vec::new();
        for (addr, ress) in &*self.ca_conn_ress.lock().await {
            senders.push((*addr, ress.sender.clone()));
        }
        let mut cmdids = Vec::new();
        for (addr, sender) in senders {
            let cmd = cmdgen();
            let cmdid = cmd.id();
            match sender.send(cmd).await {
                Ok(()) => {
                    cmdids.push(CmdId(addr, cmdid));
                }
                Err(e) => {
                    error!("can not send command {e:?}");
                }
            }
        }
        Ok(cmdids)
    }

    #[allow(unused)]
    async fn send_command_to_addr_disabled<F, R>(&self, addr: &SocketAddrV4, cmdgen: F) -> Result<R, Error>
    where
        F: Fn() -> (ConnCommand, async_channel::Receiver<R>),
    {
        if let Some(ress) = self.ca_conn_ress.lock().await.get(addr) {
            let (cmd, rx) = cmdgen();
            ress.sender.send(cmd).await.err_conv()?;
            let ret = rx.recv().await.err_conv()?;
            Ok(ret)
        } else {
            Err(Error::with_msg_no_trace(format!("addr not found")))
        }
    }

    #[allow(unused)]
    async fn send_command_inner_disabled<'a, IT, F, R>(it: &mut IT, cmdgen: F) -> Vec<async_channel::Receiver<R>>
    where
        IT: Iterator<Item = (&'a SocketAddrV4, &'a async_channel::Sender<ConnCommand>)>,
        F: Fn() -> (ConnCommand, async_channel::Receiver<R>),
    {
        let mut rxs = Vec::new();
        for (_, tx) in it {
            let (cmd, rx) = cmdgen();
            match tx.send(cmd).await {
                Ok(()) => {
                    rxs.push(rx);
                }
                Err(e) => {
                    error!("can not send command {e:?}");
                }
            }
        }
        rxs
    }

    pub async fn wait_stopped(&self) -> Result<(), Error> {
        warn!("Lock for wait_stopped");
        let mut g = self.ca_conn_ress.lock().await;
        let mm = std::mem::replace(&mut *g, BTreeMap::new());
        let mut jhs: VecDeque<_> = VecDeque::new();
        for t in mm {
            jhs.push_back(t.1.jh.fuse());
        }
        loop {
            let mut jh = if let Some(x) = jhs.pop_front() {
                x
            } else {
                break;
            };
            futures_util::select! {
                a = jh => match a {
                    Ok(k) => match k {
                        Ok(_) => {}
                        Err(e) => {
                            error!("{e:?}");
                        }
                    },
                    Err(e) => {
                        error!("{e:?}");
                    }
                },
                _b = crate::rt::sleep(Duration::from_millis(1000)).fuse() => {
                    jhs.push_back(jh);
                    info!("waiting for {} connections", jhs.len());
                }
            };
        }
        Ok(())
    }

    /// Add channel, or create a new CaConn and add the channel.
    pub async fn add_channel_to_addr(
        &self,
        backend: String,
        addr: SocketAddrV4,
        name: String,
        cssid: ChannelStatusSeriesId,
        insert_item_queue: &CommonInsertItemQueue,
        data_store: &Arc<DataStore>,
        insert_queue_max: usize,
        array_truncate: usize,
        local_epics_hostname: String,
    ) -> Result<(), Error> {
        let mut g = self
            .ca_conn_ress
            .lock()
            .slow_warn(500)
            .instrument(info_span!("conn_ress.lock"))
            .await;
        if !g.contains_key(&addr) {
            let ca_conn_ress = self.create_ca_conn_2(
                backend.clone(),
                addr,
                local_epics_hostname,
                array_truncate,
                insert_queue_max,
                insert_item_queue
                    .sender()
                    .ok_or_else(|| Error::with_msg_no_trace("can not derive sender"))?,
                data_store.clone(),
            )?;
            g.insert(addr, ca_conn_ress);
        }
        match g.get(&addr) {
            Some(ca_conn) => {
                if true {
                    let op = super::conn::ChannelSetOp::Add(cssid);
                    ca_conn.channel_set_ops.insert(name, op);
                    Ok(())
                } else {
                    let cmd = ConnCommand::channel_add(name, cssid);
                    let _cmdid = CmdId(addr, cmd.id());
                    ca_conn
                        .sender
                        .send(cmd)
                        .slow_warn(500)
                        .instrument(info_span!("ca_conn.send"))
                        .await
                        .err_conv()?;
                    Ok(())
                }
            }
            None => {
                error!("expected to find matching CaConn");
                Err(Error::with_msg_no_trace("CaConn not found"))
            }
        }
    }

    async fn conn_remove(
        ca_conn_ress: &TokMx<BTreeMap<SocketAddrV4, CaConnRess>>,
        addr: SocketAddrV4,
    ) -> Result<bool, Error> {
        // TODO make this lock-free.
        //warn!("Lock for conn_remove");
        if let Some(_caconn) = ca_conn_ress.lock().await.remove(&addr) {
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
