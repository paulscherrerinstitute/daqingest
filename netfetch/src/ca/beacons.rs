use async_channel::Sender;
use bytes::Buf;
use dbpg::seriesbychannel::ChannelInfoQuery;
use err::thiserror;
use err::ThisError;
use log::*;
use netpod::ScalarType;
use netpod::Shape;
use netpod::TsNano;
use scywr::insertqueues::InsertDeques;
use scywr::iteminsertqueue::DataValue;
use scywr::iteminsertqueue::ScalarValue;
use serieswriter::writer::SeriesWriter;
use std::io::Cursor;
use std::net::Ipv4Addr;
use std::time::SystemTime;
use taskrun::tokio::net::UdpSocket;

#[derive(Debug, ThisError)]
pub enum Error {
    Io(#[from] std::io::Error),
    SeriesWriter(#[from] serieswriter::writer::Error),
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
    let mut writer = SeriesWriter::establish(worker_tx, backend, channel, scalar_type, shape, stnow).await?;
    let sock = UdpSocket::bind("0.0.0.0:5065").await?;
    sock.set_broadcast(true).unwrap();
    let mut buf = Vec::new();
    buf.resize(1024 * 4, 0);
    let mut iqdqs = InsertDeques::new();
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
                writer.write(ts, ts_local, val, &mut iqdqs)?;
            }
        }
        if iqdqs.len() != 0 {
            // TODO deliver to insert queue
            iqdqs.clear();
        }
    }
    Ok(())
}
