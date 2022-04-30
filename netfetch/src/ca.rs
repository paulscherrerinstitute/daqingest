pub mod conn;
pub mod proto;

use crate::ca::conn::FindIoc;

use self::conn::CaConn;
use err::Error;
use futures_util::StreamExt;
use log::*;
use tokio::net::TcpStream;

pub struct CaConnectOpts {
    pub source: String,
    pub channels: Vec<String>,
}

pub async fn ca_connect_3(opts: CaConnectOpts) -> Result<(), Error> {
    debug!("ca_connect_3");
    let addr = FindIoc::new(opts.channels[0].clone()).await?;
    info!("Found IOC address: {addr:?}");
    let tcp = TcpStream::connect(&opts.source).await?;
    let mut conn = CaConn::new(tcp);
    for c in opts.channels {
        conn.channel_add(c);
    }
    while let Some(item) = conn.next().await {
        match item {
            Ok(k) => {
                trace!("CaConn gives item: {k:?}");
            }
            Err(e) => {
                error!("CaConn gives error: {e:?}");
                break;
            }
        }
    }
    Ok(())
}
