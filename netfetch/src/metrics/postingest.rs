use async_channel::Receiver;
use async_channel::Sender;
use dbpg::seriesbychannel::ChannelInfoQuery;
use err::thiserror;
use err::ThisError;
use mrucache::mucache::MuCache;
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
    iiq_tx: Sender<VecDeque<QueryItem>>,
) -> Result<(), Error> {
    // TODO so far arbitrary upper limit on the number of ad-hoc channels:
    let mut mucache: MuCache<String, SeriesWriter> = MuCache::new(2000);
    let mut item_qu = VecDeque::new();
    let mut sw_tick_last = Instant::now();

    #[allow(irrefutable_let_patterns)]
    while let item = taskrun::tokio::time::timeout(Duration::from_millis(500), item_rx.recv()).await {
        let tsnow = Instant::now();
        if tsnow.saturating_duration_since(sw_tick_last) >= Duration::from_millis(5000) {
            sw_tick_last = tsnow;
            tick_writers(mucache.all_ref_mut(), &mut item_qu)?;
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
        let item = core::mem::replace(&mut item_qu, VecDeque::new());
        iiq_tx.send(item).await?;
    }
    finish_writers(mucache.all_ref_mut(), &mut item_qu)?;
    Ok(())
}

fn tick_writers(sws: Vec<&mut SeriesWriter>, iiq: &mut VecDeque<QueryItem>) -> Result<(), Error> {
    for sw in sws {
        sw.tick(iiq)?;
    }
    Ok(())
}

fn finish_writers(sws: Vec<&mut SeriesWriter>, iiq: &mut VecDeque<QueryItem>) -> Result<(), Error> {
    for sw in sws {
        sw.tick(iiq)?;
    }
    Ok(())
}
