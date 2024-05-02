use async_channel::Receiver;
use async_channel::Sender;
use dbpg::seriesbychannel::ChannelInfoQuery;
use err::thiserror;
use err::ThisError;
use mrucache::mucache::MuCache;
use netpod::ScalarType;
use netpod::Shape;
use netpod::TsNano;
use scywr::insertqueues::InsertDeques;
use scywr::insertqueues::InsertQueuesTx;
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

impl From<async_channel::SendError<VecDeque<QueryItem>>> for Error {
    fn from(value: async_channel::SendError<VecDeque<QueryItem>>) -> Self {
        Error::SendError
    }
}

#[derive(Debug)]
pub struct EventValueItem {
    ts: TsNano,
    channel: String,
    val: DataValue,
}

struct SeriesWriterIngredients {
    writer: SeriesWriter,
}

pub async fn process_api_query_items(
    backend: String,
    item_rx: Receiver<EventValueItem>,
    info_worker_tx: Sender<ChannelInfoQuery>,
    mut iqtx: InsertQueuesTx,
) -> Result<(), Error> {
    // TODO so far arbitrary upper limit on the number of ad-hoc channels:
    let mut mucache: MuCache<String, SeriesWriter> = MuCache::new(2000);
    let mut iqdqs = InsertDeques::new();
    let mut sw_tick_last = Instant::now();

    #[allow(irrefutable_let_patterns)]
    while let item = taskrun::tokio::time::timeout(Duration::from_millis(500), item_rx.recv()).await {
        let tsnow = Instant::now();
        if tsnow.saturating_duration_since(sw_tick_last) >= Duration::from_millis(5000) {
            sw_tick_last = tsnow;
            tick_writers(mucache.all_ref_mut(), &mut iqdqs)?;
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
        sw.write(item.ts, item.ts, item.val, &mut iqdqs)?;
        iqtx.send_all(&mut iqdqs).await.map_err(|_| Error::SendError)?;
    }
    finish_writers(mucache.all_ref_mut(), &mut iqdqs)?;
    Ok(())
}

fn tick_writers(sws: Vec<&mut SeriesWriter>, iqdqs: &mut InsertDeques) -> Result<(), Error> {
    for sw in sws {
        sw.tick(iqdqs)?;
    }
    Ok(())
}

fn finish_writers(sws: Vec<&mut SeriesWriter>, iqdqs: &mut InsertDeques) -> Result<(), Error> {
    for sw in sws {
        sw.tick(iqdqs)?;
    }
    Ok(())
}
