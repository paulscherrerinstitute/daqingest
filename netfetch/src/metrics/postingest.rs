use async_channel::Receiver;
use async_channel::Sender;
use dbpg::seriesbychannel::ChannelInfoQuery;
use err::thiserror;
use err::ThisError;
use netpod::ScalarType;
use netpod::Shape;
use netpod::TsNano;
use scywr::iteminsertqueue::DataValue;
use scywr::iteminsertqueue::QueryItem;
use scywr::iteminsertqueue::ScalarValue;
use serieswriter::writer::SeriesWriter;
use std::collections::VecDeque;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;

#[derive(Debug, ThisError)]
pub enum Error {
    Msg,
    SeriesWriter(#[from] serieswriter::writer::Error),
    SendError,
}

impl From<async_channel::SendError<QueryItem>> for Error {
    fn from(value: async_channel::SendError<QueryItem>) -> Self {
        Error::SendError
    }
}

#[derive(Debug)]
pub struct EventValueItem {
    ts: TsNano,
    channel: String,
    val: DataValue,
}

async fn process_api_query_items(
    backend: String,
    item_rx: Receiver<EventValueItem>,
    info_worker_tx: Sender<ChannelInfoQuery>,
    iiq_tx: Sender<QueryItem>,
) -> Result<(), Error> {
    let mut item_qu = VecDeque::new();
    let mut sw_tick_last = Instant::now();

    #[allow(irrefutable_let_patterns)]
    while let item = taskrun::tokio::time::timeout(Duration::from_millis(500), item_rx.recv()).await {
        let tsnow = Instant::now();
        if tsnow.saturating_duration_since(sw_tick_last) >= Duration::from_millis(5000) {
            sw_tick_last = tsnow;
            tick_writers(&mut item_qu)?;
        }
        let item = match item {
            Ok(Ok(item)) => item,
            Ok(Err(_)) => break,
            Err(_) => {
                continue;
            }
        };
        let scalar_type = item.val.scalar_type();
        let shape = item.val.shape();

        // TODO cache the SeriesWriter.
        // Evict only from cache if older than some threshold.
        // If full, then reject the insert.
        let stnow = SystemTime::now();
        let mut sw = SeriesWriter::establish(
            info_worker_tx.clone(),
            backend.clone(),
            item.channel,
            scalar_type,
            shape,
            stnow,
        )
        .await?;

        let sw = &mut sw;
        sw.write(item.ts, item.ts, item.val, &mut item_qu)?;

        for e in item_qu.drain(..).into_iter() {
            iiq_tx.send(e).await?;
        }
    }
    // let scalar_type = ScalarType::F32;
    // let shape = Shape::Scalar;

    // TODO SeriesWriter need to get ticked.

    Ok(())
}

fn tick_writers(iiq: &mut VecDeque<QueryItem>) -> Result<(), Error> {
    let sw: &mut SeriesWriter = err::todoval();
    sw.tick(iiq)?;
    Ok(())
}
