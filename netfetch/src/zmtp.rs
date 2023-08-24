pub mod dumper;
pub mod zmtpproto;

use self::zmtpproto::ZmtpFrame;
use self::zmtpproto::ZmtpMessage;
use crate::bsread::ChannelDescDecoded;
use crate::bsreadclient;
use crate::bsreadclient::BsreadClient;
use crate::zmtp::zmtpproto::SocketType;
use crate::zmtp::zmtpproto::Zmtp;
#[allow(unused)]
use bytes::BufMut;
use err::thiserror;
use err::ThisError;
use futures_util::Future;
use futures_util::FutureExt;
use futures_util::StreamExt;
use futures_util::TryFutureExt;
use log::*;
use scylla::Session as ScySession;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use taskrun::tokio;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("Msg({0})")]
    Msg(String),
    #[error("TaskJoin")]
    TaskJoin,
    #[error("BsreadClient({0})")]
    BsreadClient(#[from] bsreadclient::Error),
    #[error("IO({0})")]
    IO(#[from] io::Error),
}

#[allow(unused)]
fn test_listen() -> Result<(), Error> {
    use std::time::Duration;
    let fut = async move {
        let _ = tokio::time::timeout(Duration::from_millis(16000), futures_util::future::ready(0u32)).await;
        Ok::<_, Error>(())
    };
    taskrun::run(fut).map_err(|e| Error::Msg(e.to_string()))
}

#[allow(unused)]
fn test_service() -> Result<(), Error> {
    let fut = async move {
        let sock = tokio::net::TcpListener::bind("0.0.0.0:9999").await?;
        loop {
            info!("accepting...");
            let (conn, remote) = sock.accept().await?;
            info!("new connection from {:?}", remote);
            let mut zmtp = Zmtp::new(conn, SocketType::PUSH);
            let fut = async move {
                while let Some(item) = zmtp.next().await {
                    info!("item from {:?}  {:?}", remote, item);
                }
                Ok::<_, Error>(())
            };
            taskrun::spawn(fut);
        }
    };
    taskrun::run(fut)
}

pub async fn get_series_id(_scy: &ScySession, _chn: &ChannelDescDecoded) -> Result<u64, Error> {
    error!("TODO get_series_id");
    err::todoval()
}

#[derive(Clone)]
pub struct ZmtpClientOpts {
    pub backend: String,
    pub addr: SocketAddr,
    pub do_pulse_id: bool,
    pub rcvbuf: Option<usize>,
    pub array_truncate: Option<usize>,
    pub process_channel_count_limit: Option<usize>,
}

struct ClientRun {
    #[allow(unused)]
    client: Pin<Box<BsreadClient>>,
    fut: Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>,
}

impl ClientRun {
    fn new(client: BsreadClient) -> Self {
        let mut client = Box::pin(client);
        let client2 = unsafe { &mut *(&mut client as &mut _ as *mut _) } as &mut BsreadClient;
        let fut = client2.run().map_err(|e| e.into());
        let fut = Box::pin(fut) as _;
        Self { client, fut }
    }
}

impl Future for ClientRun {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        self.fut.poll_unpin(cx)
    }
}

#[derive(Debug)]
pub enum ZmtpEvent {
    ZmtpCommand(ZmtpFrame),
    ZmtpMessage(ZmtpMessage),
}

pub async fn zmtp_client(opts: ZmtpClientOpts) -> Result<(), Error> {
    let client = BsreadClient::new(opts.clone(), todo!(), todo!()).await?;
    let fut = {
        async move {
            let mut client = client;
            client.run().await?;
            Ok::<_, Error>(())
        }
    };
    let jh = tokio::spawn(fut);
    //let mut jhs = Vec::new();
    //jhs.push(jh);
    //futures_util::future::join_all(jhs).await;
    jh.await.map_err(|_| Error::TaskJoin)??;
    Ok(())
}
