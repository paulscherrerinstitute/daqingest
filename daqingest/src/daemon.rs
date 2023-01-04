use async_channel::Receiver;
use async_channel::Sender;
use err::Error;
use futures_util::Future;
use futures_util::FutureExt;
use futures_util::StreamExt;
use log::*;
use netfetch::ca::conn::CaConn;
use netfetch::ca::conn::ConnCommand;
use netfetch::ca::findioc::FindIocRes;
use netfetch::ca::findioc::FindIocStream;
use netfetch::ca::store::DataStore;
use netfetch::conf::CaIngestOpts;
use netfetch::errconv::ErrConv;
use netfetch::store::CommonInsertItemQueue;
use netpod::Database;
use netpod::ScyllaConfig;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::fmt;
use std::net::SocketAddrV4;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use tokio_postgres::Client as PgClient;

const CHECK_CHANS_PER_TICK: usize = 50000;

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

enum ChanOp {
    Finder(String, SystemTime),
    ConnCmd(Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>),
}

impl fmt::Debug for ChanOp {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Finder(arg0, arg1) => fmt.debug_tuple("Finder").field(arg0).field(arg1).finish(),
            Self::ConnCmd(_arg0) => fmt.debug_tuple("ConnCmd").finish(),
        }
    }
}

#[derive(Debug, Serialize)]
pub enum ChannelStateValue {
    Active(ActiveChannelState),
    ToRemove { addr: Option<SocketAddrV4> },
}

#[derive(Debug)]
pub struct ChannelState {
    value: ChannelStateValue,
    pending_op: Option<ChanOp>,
}

#[derive(Debug)]
pub enum DaemonEvent {
    TimerTick,
    ChannelAdd(Channel),
    ChannelRemove(Channel),
    SearchDone(Result<VecDeque<FindIocRes>, Error>),
}

pub struct DaemonOpts {
    backend: String,
    local_epics_hostname: String,
    array_truncate: usize,
    insert_item_queue_cap: usize,
    search_tgts: Vec<SocketAddrV4>,
    //search_excl: Vec<SocketAddrV4>,
    pgconf: Database,
    scyconf: ScyllaConfig,
}

impl DaemonOpts {
    pub fn backend(&self) -> &str {
        &self.backend
    }
}

struct OptFut<F> {
    fut: Option<F>,
}

impl<F> OptFut<F> {
    fn empty() -> Self {
        Self { fut: None }
    }

    fn new(fut: F) -> Self {
        Self { fut: Some(fut) }
    }

    fn is_enabled(&self) -> bool {
        self.fut.is_some()
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

pub async fn make_pg_client(d: &Database) -> Result<PgClient, Error> {
    let (client, pg_conn) = tokio_postgres::connect(
        &format!("postgresql://{}:{}@{}:{}/{}", d.user, d.pass, d.host, d.port, d.name),
        tokio_postgres::tls::NoTls,
    )
    .await
    .err_conv()?;
    // TODO allow clean shutdown on ctrl-c and join the pg_conn in the end:
    tokio::spawn(pg_conn);
    Ok(client)
}

pub struct Daemon {
    opts: DaemonOpts,
    channel_states: BTreeMap<Channel, ChannelState>,
    tx: Sender<DaemonEvent>,
    rx: Receiver<DaemonEvent>,
    conns: BTreeMap<SocketAddrV4, (Sender<ConnCommand>, tokio::task::JoinHandle<()>)>,
    chan_check_next: Option<Channel>,
    search_tx: Sender<String>,
    ioc_finder_jh: tokio::task::JoinHandle<()>,
    datastore: Arc<DataStore>,
    common_insert_item_queue: CommonInsertItemQueue,
}

impl Daemon {
    pub async fn new(opts: DaemonOpts) -> Result<Self, Error> {
        let pg_client = make_pg_client(&opts.pgconf).await?;
        let pg_client = Arc::new(pg_client);
        let datastore = DataStore::new(&opts.scyconf, pg_client).await?;
        let datastore = Arc::new(datastore);
        let (tx, rx) = async_channel::bounded(32);
        let tgts = opts.search_tgts.clone();
        let (search_tx, ioc_finder_jh) = Self::start_finder(tx.clone(), tgts);
        let common_insert_item_queue = CommonInsertItemQueue::new(opts.insert_item_queue_cap);
        // TODO hook up with insert worker
        tokio::spawn({
            let rx = common_insert_item_queue.receiver();
            async move {
                while let Ok(item) = rx.recv().await {
                    info!("insert queue item {item:?}");
                }
            }
        });
        let ret = Self {
            opts,
            channel_states: BTreeMap::new(),
            tx,
            rx,
            conns: BTreeMap::new(),
            chan_check_next: None,
            search_tx,
            ioc_finder_jh,
            datastore,
            common_insert_item_queue,
        };
        Ok(ret)
    }

    fn start_finder(tx: Sender<DaemonEvent>, tgts: Vec<SocketAddrV4>) -> (Sender<String>, tokio::task::JoinHandle<()>) {
        let (qtx, qrx) = async_channel::bounded(32);
        let (atx, arx) = async_channel::bounded(32);
        let ioc_finder_fut = async move {
            const FINDER_JOB_QUEUE_LEN_MAX: usize = 1;
            let mut finder = FindIocStream::new(tgts);
            let mut finder_more = true;
            let mut fut1 = OptFut::new(finder.next());
            let mut fut2 = OptFut::new(qrx.recv());
            let mut qrx_more = true;
            let mut fut_tick = Box::pin(tokio::time::sleep(Duration::from_millis(500)));
            let mut asend = OptFut::empty();
            loop {
                tokio::time::sleep(Duration::from_millis(10)).await;
                tokio::select! {
                    _ = &mut asend, if asend.is_enabled() => {
                        //info!("finder asend done");
                        asend = OptFut::empty();
                    }
                    r1 = &mut fut1, if fut1.is_enabled() => {
                        //info!("finder fut1");
                        fut1 = OptFut::empty();
                        match r1 {
                            Some(item) => {
                                asend = OptFut::new(atx.send(item));
                            }
                            None => {
                                // TODO finder has stopped, do no longer poll on it
                                warn!("Finder has stopped");
                                finder_more = false;
                            }
                        }
                        //info!("finder.job_queue_len()  {}", finder.job_queue_len());
                        if qrx_more && finder.job_queue_len() < FINDER_JOB_QUEUE_LEN_MAX {
                            fut2 = OptFut::new(qrx.recv());
                        }
                        if finder_more {
                            fut1 = OptFut::new(finder.next());
                        }
                        fut_tick = Box::pin(tokio::time::sleep(Duration::from_millis(2000)));
                    }
                    r2 = &mut fut2, if fut2.is_enabled() => {
                        //info!("finder fut2");
                        fut2 = OptFut::empty();
                        match r2 {
                            Ok(item) => {
                                //info!("Push to finder: {item:?}");
                                finder.push(item);
                            }
                            Err(e) => {
                                // TODO input is done... ignore from here on.
                                error!("Finder input channel error {e}");
                                qrx_more = false;
                            }
                        }
                        //info!("finder.job_queue_len()  {}", finder.job_queue_len());
                        if qrx_more && finder.job_queue_len() < FINDER_JOB_QUEUE_LEN_MAX {
                            fut2 = OptFut::new(qrx.recv());
                        }
                        if finder_more {
                            fut1 = OptFut::new(finder.next());
                        } else {
                            fut1 = OptFut::empty();
                        }
                        fut_tick = Box::pin(tokio::time::sleep(Duration::from_millis(2000)));
                    }
                    _ = &mut fut_tick => {
                        //info!("finder fut_tick finder.job_queue_len()  {}", finder.job_queue_len());
                        if qrx_more && finder.job_queue_len() < FINDER_JOB_QUEUE_LEN_MAX {
                            fut2 = OptFut::new(qrx.recv());
                        }
                        if finder_more {
                            fut1 = OptFut::new(finder.next());
                        } else {
                            fut1 = OptFut::empty();
                        }
                        fut_tick = Box::pin(tokio::time::sleep(Duration::from_millis(2000)));
                    }
                    else => {
                        error!("all branches are disabled");
                        break;
                    }
                };
            }
        };
        let ioc_finder_jh = taskrun::spawn(ioc_finder_fut);
        taskrun::spawn({
            async move {
                while let Ok(item) = arx.recv().await {
                    //info!("forward search result item");
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
    }

    async fn check_chans(&mut self) -> Result<(), Error> {
        let tsnow = SystemTime::now();
        let k = self.chan_check_next.take();
        info!("------------   check_chans  start at {:?}", k);
        let mut currently_search_pending = 0;
        for (_ch, st) in &self.channel_states {
            if let ChannelStateValue::Active(ActiveChannelState::SearchPending { .. }) = &st.value {
                currently_search_pending += 1;
            }
        }
        let it = if let Some(last) = k {
            self.channel_states.range_mut(last..)
        } else {
            self.channel_states.range_mut(..)
        };
        let mut count_unknown_address = 0;
        let mut count_search_pending = 0;
        let mut count_no_address = 0;
        let mut count_unassigned = 0;
        let mut count_assigned = 0;
        for (i, (ch, st)) in it.enumerate() {
            use ActiveChannelState::*;
            use ChannelStateValue::*;
            match &mut st.value {
                Active(st2) => match st2 {
                    UnknownAddress => {
                        //info!("UnknownAddress {} {:?}", i, ch);
                        count_unknown_address += 1;
                        if currently_search_pending < 10 {
                            currently_search_pending += 1;
                            if st.pending_op.is_none() {
                                st.pending_op = Some(ChanOp::Finder(ch.id().to_string(), tsnow));
                                st.value = Active(SearchPending { since: tsnow });
                            }
                        }
                    }
                    SearchPending { since } => {
                        //info!("SearchPending {} {:?}", i, ch);
                        count_search_pending += 1;
                        // TODO handle Err
                        match tsnow.duration_since(*since) {
                            Ok(dt) => {
                                if dt >= Duration::from_millis(10000) {
                                    warn!("Search timeout for {ch:?}");
                                    st.value = Active(ActiveChannelState::NoAddress);
                                    currently_search_pending -= 1;
                                }
                            }
                            Err(e) => {
                                error!("SearchPending {e}");
                            }
                        }
                    }
                    WithAddress { addr, state } => {
                        //info!("WithAddress {} {:?}", i, ch);
                        use WithAddressState::*;
                        match state {
                            Unassigned { assign_at } => {
                                count_unassigned += 1;
                                if *assign_at <= tsnow {
                                    if st.pending_op.is_none() {
                                        if !self.conns.contains_key(addr) {
                                            info!("====================   create CaConn for {ch:?}");
                                            let backend = self.opts.backend().into();
                                            let local_epics_hostname = self.opts.local_epics_hostname.clone();
                                            let array_truncate = self.opts.array_truncate;
                                            let insert_item_sender = self.common_insert_item_queue.sender().await;
                                            let mut conn = CaConn::new(
                                                backend,
                                                addr.clone(),
                                                local_epics_hostname,
                                                self.datastore.clone(),
                                                insert_item_sender,
                                                array_truncate,
                                                256,
                                            );
                                            let conn_tx = conn.conn_command_tx();
                                            let conn_fut = async move { while let Some(_item) = conn.next().await {} };
                                            let conn_jh = tokio::spawn(conn_fut);
                                            self.conns.insert(*addr, (conn_tx, conn_jh));
                                        }
                                        if let Some((tx, _)) = self.conns.get(addr) {
                                            let tx = tx.clone();
                                            let (cmd, rx) = ConnCommand::channel_add(ch.id().into());
                                            let fut = async move {
                                                tx.send(cmd).await?;
                                                let res = rx.recv().await?;
                                                info!("answer from CaConn: {res:?}");
                                                Ok(())
                                            };
                                            st.pending_op = Some(ChanOp::ConnCmd(Box::pin(fut)));
                                            let cs = ConnectionState {
                                                updated: tsnow,
                                                value: ConnectionStateValue::Unconnected,
                                            };
                                            *state = WithAddressState::Assigned(cs)
                                        } else {
                                            error!("no CaConn for {ch:?}");
                                        }
                                    }
                                }
                            }
                            Assigned(_) => {
                                // TODO check if channel is healthy and alive
                                count_assigned += 1;
                            }
                        }
                    }
                    NoAddress => {
                        // TODO try to find address again after some randomized timeout
                        //info!("NoAddress {} {:?}", i, ch);
                        count_no_address += 1;
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
        info!(
            "{:8} {:8} {:8} {:8} {:8}",
            count_unknown_address, count_search_pending, count_unassigned, count_assigned, count_no_address
        );
        for (_ch, st) in &mut self.channel_states {
            match &mut st.pending_op {
                Some(op) => match op {
                    ChanOp::Finder(s, start) => {
                        if *start + Duration::from_millis(10000) >= tsnow {
                            match self.search_tx.try_send(s.clone()) {
                                Ok(_) => {
                                    st.pending_op = None;
                                    info!("OK, sent msg to Finder");
                                }
                                Err(e) => match e {
                                    async_channel::TrySendError::Full(_) => {
                                        //warn!("Finder channel full");
                                        *start = tsnow;
                                    }
                                    async_channel::TrySendError::Closed(_) => {
                                        error!("Finder channel closed");
                                    }
                                },
                            }
                        } else {
                            st.pending_op = None;
                            warn!("ChanOp::Finder timeout");
                        }
                    }
                    ChanOp::ConnCmd(fut) => {
                        use std::task::Poll::*;
                        match futures_util::poll!(fut) {
                            Ready(res) => {
                                st.pending_op = None;
                                match res {
                                    Ok(_) => {
                                        info!("ChanOp::ConnCmd completed fine");
                                    }
                                    Err(e) => {
                                        error!("ChanOp::ConnCmd {e}");
                                    }
                                }
                            }
                            Pending => {}
                        }
                    }
                },
                None => {}
            }
        }
        Ok(())
    }

    async fn handle_timer_tick(&mut self) -> Result<(), Error> {
        self.check_chans().await?;
        Ok(())
    }

    fn handle_channel_add(&mut self, ch: Channel) -> Result<(), Error> {
        if !self.channel_states.contains_key(&ch) {
            let st = ChannelState {
                value: ChannelStateValue::Active(ActiveChannelState::UnknownAddress),
                pending_op: None,
            };
            self.channel_states.insert(ch, st);
        }
        Ok(())
    }

    fn handle_channel_remove(&mut self, ch: Channel) -> Result<(), Error> {
        if let Some(k) = self.channel_states.get_mut(&ch) {
            match &k.value {
                ChannelStateValue::Active(j) => match j {
                    ActiveChannelState::UnknownAddress => {
                        k.value = ChannelStateValue::ToRemove { addr: None };
                    }
                    ActiveChannelState::SearchPending { .. } => {
                        k.value = ChannelStateValue::ToRemove { addr: None };
                    }
                    ActiveChannelState::WithAddress { addr, .. } => {
                        k.value = ChannelStateValue::ToRemove {
                            addr: Some(addr.clone()),
                        };
                    }
                    ActiveChannelState::NoAddress => {
                        k.value = ChannelStateValue::ToRemove { addr: None };
                    }
                },
                ChannelStateValue::ToRemove { .. } => {}
            }
        }
        Ok(())
    }

    async fn handle_event(&mut self, item: DaemonEvent) -> Result<(), Error> {
        use DaemonEvent::*;
        match item {
            TimerTick => self.handle_timer_tick().await,
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
                                    if let ChannelStateValue::Active(ActiveChannelState::SearchPending { .. }) =
                                        &st.value
                                    {
                                        let stnew = ChannelStateValue::Active(ActiveChannelState::WithAddress {
                                            addr,
                                            state: WithAddressState::Unassigned {
                                                assign_at: SystemTime::now(),
                                            },
                                        });
                                        st.value = stnew;
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
                let mut ticker = tokio::time::interval(Duration::from_millis(1500));
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
                    match self.handle_event(item).await {
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
        warn!("TODO shut down IOC finder properly");
        let _ = &self.ioc_finder_jh;
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
        backend: opts.backend().into(),
        local_epics_hostname: opts.local_epics_hostname().into(),
        array_truncate: opts.array_truncate(),
        insert_item_queue_cap: opts.insert_item_queue_cap(),
        pgconf: opts.postgresql().clone(),
        scyconf: opts.scylla().clone(),
        search_tgts,
    };
    let mut daemon = Daemon::new(opts2).await?;
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
