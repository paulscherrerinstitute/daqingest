use crate::timebin::ConnTimeBin;
use async_channel::Sender;
use dbpg::seriesbychannel::ChannelInfoQuery;
use err::thiserror;
use err::ThisError;
use netpod::timeunits::HOUR;
use netpod::timeunits::SEC;
use netpod::ScalarType;
use netpod::SeriesKind;
use netpod::Shape;
use netpod::TsNano;
use scywr::insertqueues::InsertDeques;
use scywr::iteminsertqueue::DataValue;
use scywr::iteminsertqueue::InsertItem;
use scywr::iteminsertqueue::QueryItem;
use series::ChannelStatusSeriesId;
use series::SeriesId;
use std::collections::VecDeque;
use std::time::SystemTime;

#[derive(Debug, ThisError)]
pub enum Error {
    DbPgSid(#[from] dbpg::seriesid::Error),
    ChannelSendError,
    ChannelRecvError,
    SeriesLookupError,
    Db(#[from] dbpg::err::Error),
    DbSchema(#[from] dbpg::schema::Error),
    Scy(#[from] scywr::session::Error),
    ScySchema(#[from] scywr::schema::Error),
    Series(#[from] dbpg::seriesbychannel::Error),
    Timebin(#[from] crate::timebin::Error),
}

impl<T> From<async_channel::SendError<T>> for Error {
    fn from(_value: async_channel::SendError<T>) -> Self {
        Error::ChannelSendError
    }
}

impl From<async_channel::RecvError> for Error {
    fn from(_value: async_channel::RecvError) -> Self {
        Error::ChannelRecvError
    }
}

#[derive(Debug)]
pub struct SeriesWriter {
    cssid: ChannelStatusSeriesId,
    sid: SeriesId,
    scalar_type: ScalarType,
    shape: Shape,
    ts_msp_last: Option<TsNano>,
    inserted_in_current_msp: u32,
    bytes_in_current_msp: u32,
    msp_max_entries: u32,
    msp_max_bytes: u32,
    // TODO this should be in an Option:
    ts_msp_grid_last: u32,
    binner: Option<ConnTimeBin>,
}

impl SeriesWriter {
    pub async fn establish(
        worker_tx: Sender<ChannelInfoQuery>,
        backend: String,
        channel: String,
        scalar_type: ScalarType,
        shape: Shape,
        stnow: SystemTime,
    ) -> Result<Self, Error> {
        let (tx, rx) = async_channel::bounded(1);
        let item = ChannelInfoQuery {
            backend: backend.clone(),
            channel: channel.clone(),
            kind: SeriesKind::ChannelStatus,
            scalar_type: ScalarType::ChannelStatus,
            shape: Shape::Scalar,
            tx: Box::pin(tx),
        };
        worker_tx.send(item).await?;
        let res = rx.recv().await?.map_err(|_| Error::SeriesLookupError)?;
        let cssid = ChannelStatusSeriesId::new(res.series.to_series().id());
        Self::establish_with_cssid(worker_tx, cssid, backend, channel, scalar_type, shape, stnow).await
    }

    pub async fn establish_with_cssid(
        channel_info_tx: Sender<ChannelInfoQuery>,
        cssid: ChannelStatusSeriesId,
        backend: String,
        channel: String,
        scalar_type: ScalarType,
        shape: Shape,
        stnow: SystemTime,
    ) -> Result<Self, Error> {
        let (tx, rx) = async_channel::bounded(1);
        let item = ChannelInfoQuery {
            backend,
            channel,
            kind: SeriesKind::ChannelData,
            scalar_type: scalar_type.clone(),
            shape: shape.clone(),
            tx: Box::pin(tx),
        };
        channel_info_tx.send(item).await?;
        let res = rx.recv().await?.map_err(|_| Error::SeriesLookupError)?;
        let sid = res.series.to_series();
        Self::establish_with_cssid_sid(cssid, sid, scalar_type, shape, stnow).await
    }

    pub async fn establish_with_cssid_sid(
        cssid: ChannelStatusSeriesId,
        sid: SeriesId,
        scalar_type: ScalarType,
        shape: Shape,
        stnow: SystemTime,
    ) -> Result<Self, Error> {
        let mut binner = ConnTimeBin::empty(sid.clone(), TsNano::from_ns(SEC * 10));
        binner.setup_for(&scalar_type, &shape, stnow)?;
        let _ = binner;
        let binner = None;
        let res = Self {
            cssid,
            sid,
            scalar_type,
            shape,
            ts_msp_last: None,
            inserted_in_current_msp: 0,
            bytes_in_current_msp: 0,
            msp_max_entries: 64000,
            msp_max_bytes: 1024 * 1024 * 20,
            ts_msp_grid_last: 0,
            binner,
        };
        Ok(res)
    }

    pub fn sid(&self) -> SeriesId {
        self.sid.clone()
    }

    pub fn scalar_type(&self) -> &ScalarType {
        &self.scalar_type
    }

    pub fn shape(&self) -> &Shape {
        &self.shape
    }

    pub fn write(
        &mut self,
        ts_ioc: TsNano,
        ts_local: TsNano,
        val: DataValue,
        deque: &mut VecDeque<QueryItem>,
    ) -> Result<(), Error> {
        let ts_main = ts_local;

        // TODO compute the binned data here as well and flush completed bins if needed.
        if let Some(binner) = self.binner.as_mut() {
            binner.push(ts_main.clone(), &val)?;
        }

        // TODO decide on better msp/lsp: random offset!
        // As long as one writer is active, the msp is arbitrary.

        // Maximum resolution of the ts msp:
        let msp_res_max = SEC * 2;

        let (ts_msp, ts_msp_changed) = match self.ts_msp_last {
            Some(ts_msp_last) => {
                if self.inserted_in_current_msp >= self.msp_max_entries
                    || self.bytes_in_current_msp >= self.msp_max_bytes
                    || ts_msp_last.add_ns(HOUR) <= ts_main
                {
                    let ts_msp = ts_main.div(msp_res_max).mul(msp_res_max);
                    if ts_msp == ts_msp_last {
                        (ts_msp, false)
                    } else {
                        self.ts_msp_last = Some(ts_msp);
                        self.inserted_in_current_msp = 1;
                        self.bytes_in_current_msp = val.byte_size();
                        (ts_msp, true)
                    }
                } else {
                    self.inserted_in_current_msp += 1;
                    self.bytes_in_current_msp += val.byte_size();
                    (ts_msp_last, false)
                }
            }
            None => {
                let ts_msp = ts_main.div(msp_res_max).mul(msp_res_max);
                self.ts_msp_last = Some(ts_msp);
                self.inserted_in_current_msp = 1;
                self.bytes_in_current_msp = val.byte_size();
                (ts_msp, true)
            }
        };
        let ts_lsp = ts_main.delta(ts_msp);
        let item = InsertItem {
            series: self.sid.clone(),
            ts_msp: ts_msp.to_ts_ms(),
            ts_lsp,
            ts_net: ts_local.to_ts_ms(),
            ts_alt_1: ts_ioc,
            msp_bump: ts_msp_changed,
            pulse: 0,
            scalar_type: self.scalar_type.clone(),
            shape: self.shape.clone(),
            val,
        };
        // TODO decide on the path in the new deques struct
        deque.push_back(QueryItem::Insert(item));
        Ok(())
    }

    pub fn tick(&mut self, deque: &mut VecDeque<QueryItem>) -> Result<(), Error> {
        if let Some(binner) = self.binner.as_mut() {
            // TODO
            //binner.tick(deque)?;
        }
        Ok(())
    }
}
