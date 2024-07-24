use async_channel::Sender;
use bytes::Buf;
use dbpg::seriesbychannel::ChannelInfoQuery;
use err::thiserror;
use err::ThisError;
use log::*;
use netpod::ScalarType;
use netpod::SeriesKind;
use netpod::Shape;
use netpod::TsNano;
use scywr::iteminsertqueue::DataValue;
use scywr::iteminsertqueue::ScalarValue;
use serieswriter::writer::SeriesWriter;
use std::collections::VecDeque;
use std::io::Cursor;
use std::net::Ipv4Addr;
use std::time::SystemTime;
use taskrun::tokio::net::UdpSocket;

#[derive(Debug, ThisError)]
#[cstm(name = "NetfetchBeacons")]
pub enum Error {
    Io(#[from] std::io::Error),
    SeriesWriter(#[from] serieswriter::writer::Error),
    ChannelSend,
    ChannelRecv,
    ChannelLookup(#[from] dbpg::seriesbychannel::Error),
}

impl<T> From<async_channel::SendError<T>> for Error {
    fn from(_value: async_channel::SendError<T>) -> Self {
        Self::ChannelSend
    }
}

impl From<async_channel::RecvError> for Error {
    fn from(_value: async_channel::RecvError) -> Self {
        Self::ChannelRecv
    }
}

pub async fn listen_beacons(
    mut cancel: taskrun::tokio::sync::mpsc::Receiver<u32>,
    worker_tx: Sender<ChannelInfoQuery>,
    backend: String,
) -> Result<(), Error> {
    let stnow = SystemTime::now();
    let channel = "epics-ca-beacons".to_string();
    let scalar_type = ScalarType::U64;
    let shape = Shape::Scalar;
    let (tx, rx) = async_channel::bounded(1);
    let qu = ChannelInfoQuery {
        backend,
        channel,
        kind: SeriesKind::ChannelData,
        scalar_type,
        shape,
        tx: Box::pin(tx),
    };
    worker_tx.send(qu).await?;
    let chinfo = rx.recv().await??;
    // TODO
    // let mut writer = SeriesWriter::new(chinfo.series.to_series());
    // let mut deque = VecDeque::new();
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
            let addr_u32 = cur.get_u32();
            let addr = Ipv4Addr::from(addr_u32);
            if cmd == 0x0d {
                debug!("beacon  {remote}  {ver}  {addr}  {port}");
                let stnow = SystemTime::now();
                let x = stnow.duration_since(SystemTime::UNIX_EPOCH).unwrap();
                let ts = TsNano::from_ms(1000 * x.as_secs() + x.subsec_millis() as u64);
                let ts_local = ts;
                let blob = addr_u32 as i64;
                let val = DataValue::Scalar(ScalarValue::I64(blob));
                // writer.write(ts, ts_local, val, &mut deque)?;
            }
        }
        // if deque.len() != 0 {
        // TODO deliver to insert queue
        // deque.clear();
        // }
    }
    Ok(())
}
