use futures_util::AsyncRead;
use futures_util::AsyncWrite;
use std::io;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use taskrun::tokio::io::ReadBuf;
use taskrun::tokio::net::TcpStream;

#[pin_project::pin_project]
pub struct TcpAsyncWriteRead {
    #[pin]
    tcp: TcpStream,
}

impl From<TcpStream> for TcpAsyncWriteRead {
    fn from(value: TcpStream) -> Self {
        Self { tcp: value }
    }
}

impl AsyncWrite for TcpAsyncWriteRead {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<io::Result<usize>> {
        use taskrun::tokio::io::AsyncWrite;
        let this = self.project();
        this.tcp.poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        use taskrun::tokio::io::AsyncWrite;
        let this = self.project();
        this.tcp.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        use taskrun::tokio::io::AsyncWrite;
        let this = self.project();
        this.tcp.poll_shutdown(cx)
    }
}

impl AsyncRead for TcpAsyncWriteRead {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        use taskrun::tokio::io::AsyncRead;
        use Poll::*;
        let this = self.project();
        let mut readbuf = ReadBuf::new(buf);
        match this.tcp.poll_read(cx, &mut readbuf) {
            Ready(Ok(())) => Ready(Ok(readbuf.filled().len())),
            Ready(Err(e)) => Ready(Err(e)),
            Pending => Pending,
        }
    }
}
