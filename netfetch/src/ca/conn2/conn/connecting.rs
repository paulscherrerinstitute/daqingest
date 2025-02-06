use std::fmt;
use std::future::Future;
use std::net::SocketAddrV4;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;
use taskrun::tokio;
use tokio::net::TcpStream;

autoerr::create_error_v1!(
    name(Error, "Connecting"),
    enum variants {
        Logic,
    },
);

type ConnectingFut =
    Pin<Box<dyn Future<Output = Result<Result<TcpStream, std::io::Error>, tokio::time::error::Elapsed>> + Send>>;

pub struct Connecting {
    tsbeg: Instant,
    addr: SocketAddrV4,
    fut: ConnectingFut,
}

impl Connecting {
    pub fn dummy_new(remote_addr: SocketAddrV4, tsnow: Instant) -> Self {
        Self {
            tsbeg: tsnow,
            addr: remote_addr,
            fut: err::todoval(),
        }
    }

    pub fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Option<()>, Error>> {
        let x = Err(Error::Logic);
        x?
    }

    pub fn poll_unpin(&mut self, cx: &mut Context) -> Poll<Result<Option<()>, Error>> {
        Pin::new(self).poll(cx)
    }
}

impl fmt::Debug for Connecting {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Connecting")
            .field("tsbeg", &self.tsbeg)
            .field("addr", &self.addr)
            .finish()
    }
}
