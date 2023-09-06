use super::conn::CaConnEvent;
use super::conn::ConnCommand;
use super::connset_consume::ConnSetConsume;
use super::SlowWarnable;
use crate::ca::conn::CaConn;
use crate::ca::conn::CaConnEventValue;
use crate::ca::conn::CaConnOpts;
use crate::errconv::ErrConv;
use crate::rt::JoinHandle;
use crate::rt::TokMx;
use async_channel::Receiver;
use async_channel::Sender;
use dbpg::seriesbychannel::ChannelInfoQuery;
use err::Error;
use futures_util::FutureExt;
use futures_util::StreamExt;
use netpod::log::*;
use scywr::iteminsertqueue::CommonInsertItemQueue;
use scywr::iteminsertqueue::CommonInsertItemQueueSender;
use scywr::store::DataStore;
use series::ChannelStatusSeriesId;
use stats::CaConnStats;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::net::SocketAddrV4;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use taskrun::tokio;
use tracing::info_span;
use tracing::Instrument;

#[derive(Debug, PartialEq, Eq)]
pub struct CmdId(SocketAddrV4, usize);

pub struct CaConnRes {
    sender: Sender<ConnCommand>,
    stats: Arc<CaConnStats>,
    jh: JoinHandle<Result<(), Error>>,
}

impl CaConnRes {
    pub fn stats(&self) -> &Arc<CaConnStats> {
        &self.stats
    }
}

#[derive(Debug, Clone)]
pub struct ChannelAdd {
    backend: String,
    name: String,
    addr: SocketAddr,
    cssid: ChannelStatusSeriesId,
    local_epics_hostname: String,
}

#[derive(Debug)]
pub enum ConnSetCmd {
    ChannelAdd(ChannelAdd),
    Shutdown,
}

#[derive(Clone)]
pub struct CaConnSetCtrl {
    cmd_tx: Sender<ConnSetCmd>,
}

impl CaConnSetCtrl {
    pub async fn add_channel(
        &self,
        backend: String,
        addr: SocketAddr,
        name: String,
        cssid: ChannelStatusSeriesId,
        local_epics_hostname: String,
    ) -> Result<(), Error> {
        let cmd = ChannelAdd {
            backend,
            name,
            addr,
            cssid,
            local_epics_hostname,
        };
        let cmd = ConnSetCmd::ChannelAdd(cmd);
        self.cmd_tx.send(cmd).await?;
        Ok(())
    }
}

pub struct CaConnSet {
    cmd_rx: Receiver<ConnSetCmd>,
    ca_conn_ress: BTreeMap<SocketAddr, CaConnRes>,
    conn_item_tx: Sender<(SocketAddr, CaConnEvent)>,
    channel_info_query_tx: Sender<ChannelInfoQuery>,
    conn_evs_collect_jh: JoinHandle<Result<(), crate::ca::connset_consume::Error>>,
    shutdown: bool,
}

impl CaConnSet {
    pub fn new(channel_info_query_tx: Sender<ChannelInfoQuery>) -> CaConnSetCtrl {
        let (cmd_tx, cmd_rx) = async_channel::bounded(32);
        let (conn_item_tx, conn_item_rx) = async_channel::bounded(10000);
        let (consume_jh,) = ConnSetConsume::new(conn_item_rx);
        // TODO where to use this?
        // TODO receive and use CaConn items, as well as receive and use ConnSetCmd
        err::todo();
        let connset = Self {
            cmd_rx,
            ca_conn_ress: BTreeMap::new(),
            conn_item_tx,
            channel_info_query_tx,
            conn_evs_collect_jh: consume_jh,
            shutdown: false,
        };
        CaConnSetCtrl { cmd_tx }
    }

    async fn run(mut self) -> Result<(), Error> {
        loop {
            let x = self.cmd_rx.recv().await;
            match x {
                Ok(ev) => self.handle_event(ev).await?,
                Err(_) => {
                    if self.shutdown {
                        // all fine
                        break Ok(());
                    } else {
                        error!("channel closed without shutdown");
                    }
                }
            }
        }
    }

    async fn handle_event(&mut self, ev: ConnSetCmd) -> Result<(), Error> {
        use ConnSetCmd::*;
        match ev {
            ChannelAdd(x) => self.add_channel_to_addr(x).await,
            Shutdown => {
                self.shutdown = true;
                Ok(())
            }
        }
    }

    async fn add_channel_to_addr(&mut self, add: ChannelAdd) -> Result<(), Error> {
        if !self.ca_conn_ress.contains_key(&add.addr) {
            let c = self.create_ca_conn(add.clone())?;
            self.ca_conn_ress.insert(add.addr, c);
        }
        let conn_ress = self.ca_conn_ress.get_mut(&add.addr).unwrap();
        let cmd = ConnCommand::channel_add(add.name, add.cssid);
        conn_ress.sender.send(cmd).await?;
        Ok(())
    }

    fn create_ca_conn(&self, add: ChannelAdd) -> Result<CaConnRes, Error> {
        // TODO should we save this as event?
        let opts = CaConnOpts::default();
        let addr = add.addr;
        let addr_v4 = if let SocketAddr::V4(x) = add.addr {
            x
        } else {
            return Err(Error::with_msg_no_trace("only ipv4 for epics"));
        };
        debug!("create new CaConn  {:?}", addr);
        let conn = CaConn::new(
            opts,
            add.backend.clone(),
            addr_v4,
            add.local_epics_hostname,
            self.channel_info_query_tx.clone(),
        );
        let conn_tx = conn.conn_command_tx();
        let conn_stats = conn.stats();
        let conn_item_tx = self.conn_item_tx.clone();
        let jh = tokio::spawn(Self::ca_conn_item_merge(conn, conn_item_tx, addr_v4));
        let ca_conn_res = CaConnRes {
            sender: conn_tx,
            stats: conn_stats,
            jh,
        };
        Ok(ca_conn_res)
    }

    async fn ca_conn_item_merge(
        conn: CaConn,
        conn_item_tx: Sender<(SocketAddr, CaConnEvent)>,
        addr: SocketAddrV4,
    ) -> Result<(), Error> {
        debug!("ca_conn_consumer  begin  {}", addr);
        let stats = conn.stats();
        let mut conn = conn;
        let mut ret = Ok(());
        while let Some(item) = conn.next().await {
            match item {
                Ok(item) => {
                    stats.conn_item_count_inc();
                    conn_item_tx.send((SocketAddr::V4(addr), item)).await?;
                }
                Err(e) => {
                    error!("CaConn gives error: {e:?}");
                    ret = Err(e);
                }
            }
        }
        debug!("ca_conn_consumer  ended {}", addr);
        conn_item_tx
            .send((
                SocketAddr::V4(addr),
                CaConnEvent {
                    ts: Instant::now(),
                    value: CaConnEventValue::EndOfStream,
                },
            ))
            .await?;
        debug!("ca_conn_consumer  signaled {}", addr);
        ret
    }

    #[allow(unused)]
    async fn __enqueue_command_to_all<F>(&self, cmdgen: F) -> Result<Vec<CmdId>, Error>
    where
        F: Fn() -> ConnCommand,
    {
        let mut senders: Vec<(SocketAddrV4, Sender<ConnCommand>)> = err::todoval();
        let mut cmdids = Vec::new();
        for (addr, sender) in senders {
            let cmd = cmdgen();
            let cmdid = cmd.id();
            match sender.send(cmd).await {
                Ok(()) => {
                    cmdids.push(CmdId(addr, cmdid));
                }
                Err(e) => {
                    error!("enqueue_command_to_all  can not send command {e:?}  {:?}", e.0);
                }
            }
        }
        Ok(cmdids)
    }

    #[allow(unused)]
    async fn __send_command_to_addr_disabled<F, R>(&self, addr: &SocketAddrV4, cmdgen: F) -> Result<R, Error>
    where
        F: Fn() -> (ConnCommand, async_channel::Receiver<R>),
    {
        let tx: Sender<ConnCommand> = err::todoval();
        let (cmd, rx) = cmdgen();
        tx.send(cmd).await.err_conv()?;
        let ret = rx.recv().await.err_conv()?;
        Ok(ret)
    }

    #[allow(unused)]
    async fn __send_command_inner_disabled<'a, IT, F, R>(it: &mut IT, cmdgen: F) -> Vec<async_channel::Receiver<R>>
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
        // let mut g = self.ca_conn_ress.lock().await;
        // let mm = std::mem::replace(&mut *g, BTreeMap::new());
        let mm: BTreeMap<SocketAddrV4, JoinHandle<Result<(), Error>>> = BTreeMap::new();
        let mut jhs: VecDeque<_> = VecDeque::new();
        for t in mm {
            jhs.push_back(t.1.fuse());
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

    async fn conn_remove(
        ca_conn_ress: &TokMx<BTreeMap<SocketAddrV4, CaConnRes>>,
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
