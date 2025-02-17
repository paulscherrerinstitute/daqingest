use futures_util::FutureExt;
use std::fmt;
use std::future::Future;
use std::net::SocketAddrV4;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;
use taskrun::tokio;
use tokio::net::TcpStream;
use tokio::time::error::Elapsed;

autoerr::create_error_v1!(
    name(Error, "Connecting"),
    enum variants {
        Timeout,
        IO(#[from] std::io::Error),
    },
);

type PollType = TcpStream;

type ReturnType = Result<Result<TcpStream, std::io::Error>, Elapsed>;

type ConnectingFut = Pin<Box<dyn Future<Output = ReturnType> + Send>>;

pub struct Connecting {
    tsbeg: Instant,
    addr: SocketAddrV4,
    fut: ConnectingFut,
}

impl fmt::Debug for Connecting {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Connecting")
            .field("tsbeg", &self.tsbeg)
            .field("addr", &self.addr)
            .finish()
    }
}

impl Connecting {
    pub fn new(remote_addr: SocketAddrV4, tsnow: Instant) -> Self {
        let fut = tokio::time::timeout(Duration::from_millis(1800), tokio::net::TcpStream::connect(remote_addr));
        Self {
            tsbeg: tsnow,
            addr: remote_addr,
            fut: Box::pin(fut),
        }
    }

    pub fn addr(&self) -> SocketAddrV4 {
        self.addr
    }

    pub fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Option<PollType>, Error>> {
        use Poll::*;
        match self.fut.poll_unpin(cx) {
            Ready(x) => match x {
                Ok(Ok(x)) => Ready(Ok(Some(x))),
                Ok(Err(e)) => Ready(Err(e.into())),
                Err(_) => Ready(Err(Error::Timeout)),
            },
            Pending => Pending,
        }
    }

    pub fn poll_unpin(&mut self, cx: &mut Context) -> Poll<Result<Option<PollType>, Error>> {
        Pin::new(self).poll(cx)
    }
}
