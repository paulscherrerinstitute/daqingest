use async_channel::Sender;
use dbpg::seriesbychannel::ChannelInfoQuery;
use err::thiserror;
use err::ThisError;
use log::*;
use netpod::ScalarType;
use netpod::Shape;
use series::series::CHANNEL_STATUS_DUMMY_SCALAR_TYPE;
use series::ChannelStatusSeriesId;
use series::SeriesId;

#[derive(Debug, ThisError)]
pub enum Error {
    DbPgSid(#[from] dbpg::seriesid::Error),
    ChannelSendError,
    ChannelRecvError,
    SeriesLookupError,
}

impl<T> From<async_channel::SendError<T>> for Error {
    fn from(value: async_channel::SendError<T>) -> Self {
        Error::ChannelSendError
    }
}
impl From<async_channel::RecvError> for Error {
    fn from(value: async_channel::RecvError) -> Self {
        Error::ChannelRecvError
    }
}

pub struct SeriesWriter {
    cssid: ChannelStatusSeriesId,
    sid: SeriesId,
}

impl SeriesWriter {
    // TODO this requires a database
    pub async fn establish(
        worker_tx: Sender<Vec<ChannelInfoQuery>>,
        backend: String,
        channel: String,
        scalar_type: ScalarType,
        shape: Shape,
    ) -> Result<Self, Error> {
        let (tx, rx) = async_channel::bounded(1);
        let item = ChannelInfoQuery {
            backend: backend.clone(),
            channel: channel.clone(),
            scalar_type: CHANNEL_STATUS_DUMMY_SCALAR_TYPE,
            shape_dims: shape.to_scylla_vec(),
            tx: Box::pin(tx),
        };
        worker_tx.send(vec![item]).await?;
        let res = rx.recv().await?.map_err(|_| Error::SeriesLookupError)?;
        let cssid = ChannelStatusSeriesId::new(res.series.into_inner().id());
        let (tx, rx) = async_channel::bounded(1);
        let item = ChannelInfoQuery {
            backend,
            channel,
            scalar_type: scalar_type.to_scylla_i32(),
            shape_dims: shape.to_scylla_vec(),
            tx: Box::pin(tx),
        };
        worker_tx.send(vec![item]).await?;
        let res = rx.recv().await?.map_err(|_| Error::SeriesLookupError)?;
        let sid = res.series.into_inner();
        let res = Self { cssid, sid };
        Ok(res)
    }
}
