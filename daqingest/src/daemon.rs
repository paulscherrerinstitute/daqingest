use async_channel::Receiver;
use async_channel::Sender;
use err::Error;
use futures_util::FutureExt;
use futures_util::StreamExt;
use log::*;
use netfetch::ca::conn::CaConn;
use netfetch::ca::conn::ConnCommand;
use netfetch::ca::findioc::FindIocRes;
use netfetch::ca::findioc::FindIocStream;
use netfetch::conf::CaIngestOpts;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::net::IpAddr;
use std::net::SocketAddrV4;
use std::time::Duration;
use std::time::SystemTime;

const CHECK_CHANS_PER_TICK: usize = 10;

#[derive(Clone, Debug, Serialize, PartialEq, PartialOrd, Eq, Ord)]
pub struct Channel {
    id: String,
}

impl Channel {
    pub fn new(id: String) -> Self {
        Self { id }
    }

    pub fn id(&self) -> &str {
        &self.id
    }
}

#[derive(Clone, Debug, Serialize)]
pub enum ConnectionStateValue {
    Unconnected,
    Connected { since: SystemTime },
}

#[derive(Clone, Debug, Serialize)]
pub struct ConnectionState {
    updated: SystemTime,
    value: ConnectionStateValue,
}

#[derive(Clone, Debug, Serialize)]
pub enum WithAddressState {
    Unassigned { assign_at: SystemTime },
    Assigned(ConnectionState),
}

#[derive(Clone, Debug, Serialize)]
pub enum ActiveChannelState {
    UnknownAddress,
    SearchPending {
        since: SystemTime,
    },
    WithAddress {
        addr: SocketAddrV4,
        state: WithAddressState,
    },
    NoAddress,
}

#[derive(Clone, Debug, Serialize)]
pub enum ChannelState {
    Active(ActiveChannelState),
    ToRemove { addr: Option<SocketAddrV4> },
}

#[derive(Debug)]
pub enum DaemonEvent {
    TimerTick,
    ChannelAdd(Channel),
    ChannelRemove(Channel),
    SearchDone(Result<VecDeque<FindIocRes>, Error>),
}

pub struct DaemonOpts {
    search_tgts: Vec<SocketAddrV4>,
    search_excl: Vec<SocketAddrV4>,
}

struct OptFut<F> {
    fut: Option<F>,
}

impl<F> OptFut<F> {
    fn new(fut: Option<F>) -> Self {
        Self { fut }
    }
}

impl<F> futures_util::Future for OptFut<F>
where
    F: futures_util::Future + std::marker::Unpin,
{
    type Output = <F as futures_util::Future>::Output;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context) -> std::task::Poll<Self::Output> {
        match self.fut.as_mut() {
            Some(fut) => fut.poll_unpin(cx),
            None => std::task::Poll::Pending,
        }
    }
}

pub struct Daemon {
    opts: DaemonOpts,
    channel_states: BTreeMap<Channel, ChannelState>,
    tx: Sender<DaemonEvent>,
    rx: Receiver<DaemonEvent>,
    conns: BTreeMap<SocketAddrV4, CaConn>,
    chan_check_next: Option<Channel>,
    search_tx: Sender<String>,
    ioc_finder_jh: tokio::task::JoinHandle<()>,
}

impl Daemon {
    pub fn new(opts: DaemonOpts) -> Self {
        let (tx, rx) = async_channel::bounded(1);
        let tgts = opts.search_tgts.clone();
        let (search_tx, ioc_finder_jh) = {
            let (qtx, qrx) = async_channel::bounded(1);
            let (atx, arx) = async_channel::bounded(1);
            let ioc_finder_fut = async move {
                const FINDER_JOB_QUEUE_LEN_MAX: usize = 1;
                let mut finder = FindIocStream::new(tgts);
                let mut fut1 = finder.next();
                let mut fut2 = qrx.recv().fuse();
                let mut fut_tick = Box::pin(tokio::time::sleep(Duration::from_millis(2000)).fuse());
                let mut asend = OptFut::new(None).fuse();
                loop {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    futures_util::select! {
                        _ = asend => {
                            info!("asend done");
                        }
                        r1 = fut1 => {
                            match r1 {
                                Some(item) => {
                                    asend = OptFut::new(Some(atx.send(item))).fuse();
                                }
                                None => {
                                    // TODO finder has stopped, do no longer poll on it
                                }
                            }
                            if finder.job_queue_len() < FINDER_JOB_QUEUE_LEN_MAX {
                                fut2 = qrx.recv().fuse();
                            }
                            fut1 = finder.next();
                            fut_tick = Box::pin(tokio::time::sleep(Duration::from_millis(2000)).fuse());
                        }
                        r2 = fut2 => {
                            match r2 {
                                Ok(item) => {
                                    info!("Push to finder: {item:?}");
                                    finder.push(item);
                                }
                                Err(e) => {
                                    // TODO input is done... ignore from here on.
                                    error!("{e}");
                                    break;
                                }
                            }
                            if finder.job_queue_len() < FINDER_JOB_QUEUE_LEN_MAX {
                                fut2 = qrx.recv().fuse();
                            }
                            fut1 = finder.next();
                            fut_tick = Box::pin(tokio::time::sleep(Duration::from_millis(2000)).fuse());
                        }
                        _ = fut_tick => {
                            if finder.job_queue_len() < FINDER_JOB_QUEUE_LEN_MAX {
                                //fut2 = qrx.recv().fuse();
                            }
                            fut1 = finder.next();
                            fut_tick = Box::pin(tokio::time::sleep(Duration::from_millis(2000)).fuse());
                        }
                    };
                }
            };
            let ioc_finder_jh = taskrun::spawn(ioc_finder_fut);
            taskrun::spawn({
                let tx = tx.clone();
                async move {
                    while let Ok(item) = arx.recv().await {
                        info!("forward search result item");
                        match tx.send(DaemonEvent::SearchDone(item)).await {
                            Ok(_) => {}
                            Err(e) => {
                                error!("search res fwd {e}");
                            }
                        }
                    }
                    warn!("search res fwd nput broken");
                }
            });
            (qtx, ioc_finder_jh)
        };
        Self {
            opts,
            channel_states: BTreeMap::new(),
            tx,
            rx,
            conns: BTreeMap::new(),
            chan_check_next: None,
            search_tx,
            ioc_finder_jh,
        }
    }

    fn check_chans(&mut self) -> Result<(), Error> {
        let tsnow = SystemTime::now();
        let k = self.chan_check_next.take();
        info!("check_chans  start at {:?}", k);
        let it = if let Some(last) = k {
            self.channel_states.range_mut(last..)
        } else {
            self.channel_states.range_mut(..)
        };
        for (i, (ch, st)) in it.enumerate() {
            info!("check chan {} {:?}", i, ch);
            use ActiveChannelState::*;
            use ChannelState::*;
            match st {
                Active(st2) => match st2 {
                    UnknownAddress => {
                        if self.search_tx.is_full() {
                            // TODO what to do if the queue is full?
                        } else {
                            match self.search_tx.try_send(ch.id().into()) {
                                Ok(_) => {
                                    *st = Active(SearchPending { since: tsnow });
                                }
                                Err(_) => {
                                    error!("can not send search query");
                                }
                            }
                        }
                    }
                    SearchPending { since } => {
                        // TODO handle Err
                        match tsnow.duration_since(*since) {
                            Ok(dt) => {
                                if dt >= Duration::from_millis(10000) {
                                    warn!("Search timeout for {ch:?}");
                                    *st = Active(ActiveChannelState::NoAddress);
                                }
                            }
                            Err(e) => {
                                error!("{e}");
                            }
                        }
                    }
                    WithAddress { addr, state } => {
                        use WithAddressState::*;
                        match state {
                            Unassigned { assign_at } => {
                                if *assign_at <= tsnow {
                                    match self.conns.get(addr) {
                                        Some(conn) => {
                                            let tx = conn.conn_command_tx();
                                            let (cmd, rx) = ConnCommand::channel_add(ch.id().into());
                                            // TODO how to send the command from this non-async context?
                                            //tx.send(cmd).await;
                                            // TODO if the send can be assumed to be on its way (it may still fail) then update state
                                            if true {
                                                let cs = ConnectionState {
                                                    updated: tsnow,
                                                    value: ConnectionStateValue::Unconnected,
                                                };
                                                *state = WithAddressState::Assigned(cs)
                                            }
                                        }
                                        None => {}
                                    }
                                }
                            }
                            Assigned(_) => {
                                // TODO check if channel is healthy and alive
                            }
                        }
                    }
                    NoAddress => {
                        // TODO try to find address again after some randomized timeout
                    }
                },
                ToRemove { .. } => {
                    // TODO if assigned to some address,
                }
            }
            if i >= CHECK_CHANS_PER_TICK {
                self.chan_check_next = Some(ch.clone());
                break;
            }
        }
        Ok(())
    }

    fn handle_timer_tick(&mut self) -> Result<(), Error> {
        self.check_chans()?;
        Ok(())
    }

    fn handle_channel_add(&mut self, ch: Channel) -> Result<(), Error> {
        if !self.channel_states.contains_key(&ch) {
            self.channel_states
                .insert(ch, ChannelState::Active(ActiveChannelState::UnknownAddress));
        }
        Ok(())
    }

    fn handle_channel_remove(&mut self, ch: Channel) -> Result<(), Error> {
        if let Some(k) = self.channel_states.get_mut(&ch) {
            match k {
                ChannelState::Active(j) => match j {
                    ActiveChannelState::UnknownAddress => {
                        *k = ChannelState::ToRemove { addr: None };
                    }
                    ActiveChannelState::SearchPending { .. } => {
                        *k = ChannelState::ToRemove { addr: None };
                    }
                    ActiveChannelState::WithAddress { addr, .. } => {
                        *k = ChannelState::ToRemove {
                            addr: Some(addr.clone()),
                        };
                    }
                    ActiveChannelState::NoAddress => {
                        *k = ChannelState::ToRemove { addr: None };
                    }
                },
                ChannelState::ToRemove { .. } => {}
            }
        }
        Ok(())
    }

    fn handle_event(&mut self, item: DaemonEvent) -> Result<(), Error> {
        use DaemonEvent::*;
        match item {
            TimerTick => self.handle_timer_tick(),
            ChannelAdd(ch) => self.handle_channel_add(ch),
            ChannelRemove(ch) => self.handle_channel_remove(ch),
            SearchDone(res) => {
                info!("handle SearchDone: {res:?}");
                match res {
                    Ok(a) => {
                        for res in a {
                            if let Some(addr) = &res.addr {
                                let addr = addr.clone();
                                let ch = Channel::new(res.channel);
                                if let Some(st) = self.channel_states.get_mut(&ch) {
                                    if let ChannelState::Active(ActiveChannelState::SearchPending { .. }) = st {
                                        let stnew = ChannelState::Active(ActiveChannelState::WithAddress {
                                            addr,
                                            state: WithAddressState::Unassigned {
                                                assign_at: SystemTime::now(),
                                            },
                                        });
                                        self.channel_states.insert(ch, stnew);
                                    } else {
                                        warn!("state for {ch:?} is not SearchPending");
                                    }
                                } else {
                                    warn!("can not find channel state for {ch:?}");
                                }
                            } else {
                                warn!("no addr from search in {res:?}");
                            }
                        }
                    }
                    Err(e) => {
                        error!("error from search: {e}");
                    }
                }
                Ok(())
            }
        }
    }

    pub async fn daemon(&mut self) -> Result<(), Error> {
        let ticker = {
            let tx = self.tx.clone();
            async move {
                let mut ticker = tokio::time::interval(Duration::from_millis(500));
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    ticker.tick().await;
                    if let Err(e) = tx.send(DaemonEvent::TimerTick).await {
                        error!("can not send TimerTick {e}");
                        break;
                    }
                }
            }
        };
        taskrun::spawn(ticker);
        loop {
            match self.rx.recv().await {
                Ok(item) => {
                    info!("got daemon event {item:?}");
                    match self.handle_event(item) {
                        Ok(_) => {}
                        Err(e) => {
                            error!("daemon: {e}");
                            break;
                        }
                    }
                }
                Err(e) => {
                    error!("{e}");
                    break;
                }
            }
        }
        Ok(())
    }
}

pub async fn run(opts: CaIngestOpts, channels: Vec<String>) -> Result<(), Error> {
    info!("start up {opts:?}");
    let mut search_tgts = Vec::new();
    for s in opts.search() {
        let addr: SocketAddrV4 = s.parse()?;
        search_tgts.push(addr);
    }
    info!("parsed search_tgts {search_tgts:?}");
    let opts2 = DaemonOpts {
        search_tgts,
        search_excl: Vec::new(),
    };
    let mut daemon = Daemon::new(opts2);
    let tx = daemon.tx.clone();
    let daemon_jh = taskrun::spawn(async move {
        // TODO handle Err
        daemon.daemon().await.unwrap();
    });
    for s in &channels {
        let ch = Channel::new(s.into());
        tx.send(DaemonEvent::ChannelAdd(ch)).await?;
    }
    info!("all channels sent to daemon");
    daemon_jh.await.unwrap();
    Ok(())
}
