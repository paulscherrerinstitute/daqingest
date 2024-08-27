use super::conncmd::ConnCommand;
use super::connevent::CaConnEvent;
use super::connevent::EndOfStreamReason;
use crate::ca::conn::CaConnOpts;
use crate::ca::proto::CaProto;
use async_channel::Sender;
use dbpg::seriesbychannel::ChannelInfoQuery;
use futures_util::Future;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use hashbrown::HashMap;
use log::*;
use scywr::insertqueues::InsertQueuesTx;
use stats::rand_xoshiro::Xoshiro128PlusPlus;
use stats::CaConnStats;
use stats::CaProtoStats;
use std::net::SocketAddrV4;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;
use taskrun::tokio;
use tokio::net::TcpStream;

#[derive(Debug)]
pub enum Error {}

type ConnectingFut =
    Pin<Box<dyn Future<Output = Result<Result<TcpStream, std::io::Error>, tokio::time::error::Elapsed>> + Send>>;

enum ConnectedState {
    Init(CaProto),
    Handshake(CaProto),
    PeerReady(CaProto),
}

enum CaConnState {
    Connecting(Instant, SocketAddrV4, ConnectingFut),
    Connected(CaProto),
    Shutdown(EndOfStreamReason),
    Done,
}

struct CaConn {
    opts: CaConnOpts,
    backend: String,
    state: CaConnState,
    rng: Xoshiro128PlusPlus,
}

impl CaConn {
    fn new(
        opts: CaConnOpts,
        backend: String,
        remote_addr: SocketAddrV4,
        local_epics_hostname: String,
        iqtx: InsertQueuesTx,
        channel_info_query_tx: Sender<ChannelInfoQuery>,
        stats: Arc<CaConnStats>,
        ca_proto_stats: Arc<CaProtoStats>,
    ) -> Self {
        let tsnow = Instant::now();
        let (cq_tx, cq_rx) = async_channel::bounded::<ConnCommand>(32);
        let mut rng = stats::xoshiro_from_time();
        Self {
            opts,
            backend,
            state: CaConnState::Connecting(tsnow, remote_addr, err::todoval()),
            rng,
        }
    }
}

impl Stream for CaConn {
    type Item = CaConnEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        todo!()
    }
}
