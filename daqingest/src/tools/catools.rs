use crate::opts::CaFind;
use err::thiserror;
use err::ThisError;
use futures_util::StreamExt;
use stats::IocFinderStats;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, ThisError)]
#[cstm(name = "CaTools")]
pub enum Error {}

pub async fn find(cmd: CaFind, broadcast: String) -> Result<(), Error> {
    let brd = broadcast.split(",");
    let (channels_input_tx, channels_input_rx) = async_channel::bounded(10);
    let tgts = brd.map(|x| x.parse().unwrap()).collect();
    let blacklist = Vec::new();
    let batch_run_max = Duration::from_millis(1200);
    let in_flight_max = 1;
    let batch_size = 1;
    let stats = Arc::new(IocFinderStats::new());
    channels_input_tx.send(cmd.channel).await.unwrap();
    let mut stream = netfetch::ca::findioc::FindIocStream::new(
        channels_input_rx,
        tgts,
        blacklist,
        batch_run_max,
        in_flight_max,
        batch_size,
        stats,
    );
    while let Some(e) = stream.next().await {
        eprintln!("{e:?}");
    }
    eprintln!("done");
    Ok(())
}
