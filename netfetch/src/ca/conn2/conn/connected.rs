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
    name(Error, "Connected"),
    enum variants {
        Timeout,
        IO(#[from] std::io::Error),
    },
);

type PollType = ();

type ReturnType = Result<Result<TcpStream, std::io::Error>, Elapsed>;

// type ConnectingFut = Pin<Box<dyn Future<Output = ReturnType> + Send>>;

pub struct Connected {
    tsbeg: Instant,
    addr: SocketAddrV4,
    tcp: TcpStream,
    // fut: ConnectingFut,
}

impl fmt::Debug for Connected {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Connected")
            .field("tsbeg", &self.tsbeg)
            .field("addr", &self.addr)
            .finish()
    }
}

impl Connected {
    pub fn new(remote_addr: SocketAddrV4, tcp: TcpStream, tsnow: Instant) -> Self {
        Self {
            tsbeg: tsnow,
            addr: remote_addr,
            tcp,
            // fut: Box::pin(fut),
        }
    }

    pub fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Option<PollType>, Error>> {
        use Poll::*;
        Pending
    }

    pub fn poll_unpin(&mut self, cx: &mut Context) -> Poll<Result<Option<PollType>, Error>> {
        Pin::new(self).poll(cx)
    }
}
