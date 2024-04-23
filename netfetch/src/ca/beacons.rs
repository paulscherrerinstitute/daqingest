use bytes::Buf;
use err::thiserror;
use err::ThisError;
use log::*;
use std::io::Cursor;
use std::net::Ipv4Addr;
use taskrun::tokio::net::UdpSocket;

#[derive(Debug, ThisError)]
pub enum Error {
    Io(#[from] std::io::Error),
}

pub async fn listen_beacons(mut cancel: taskrun::tokio::sync::mpsc::Receiver<u32>) -> Result<(), Error> {
    let sock = UdpSocket::bind("0.0.0.0:5065").await?;
    sock.set_broadcast(true).unwrap();
    let mut buf = Vec::new();
    buf.resize(1024 * 4, 0);
    loop {
        let bb = &mut buf;
        let (n, remote) = taskrun::tokio::select! {
            x = sock.recv_from(bb) => x,
            _ = cancel.recv() => {
                break;
            }
        }?;
        if n != 16 {
            debug!("len recv {n}");
        }
        if n >= 16 {
            let mut cur = Cursor::new(bb);
            let cmd = cur.get_u16();
            let _ = cur.get_u16();
            let ver = cur.get_u16();
            let port = cur.get_u16();
            let _seqid = cur.get_u32();
            let addr = cur.get_u32();
            let addr = Ipv4Addr::from(addr);
            if cmd == 0x0d {
                debug!("beacon  {remote}  {ver}  {addr}  {port}")
            }
        }
    }
    Ok(())
}
