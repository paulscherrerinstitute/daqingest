use async_channel::Sender;
use dbpg::seriesbychannel::ChannelInfoQuery;
use err::thiserror;
use err::ThisError;
use log::*;
use netpod::timeunits::HOUR;
use netpod::timeunits::SEC;
use netpod::ScalarType;
use netpod::SeriesKind;
use netpod::Shape;
use netpod::TsMs;
use netpod::TsNano;
use scywr::insertqueues::InsertDeques;
use scywr::iteminsertqueue::DataValue;
use scywr::iteminsertqueue::InsertItem;
use scywr::iteminsertqueue::QueryItem;
use series::ChannelStatusSeriesId;
use series::SeriesId;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::time::Instant;
use std::time::SystemTime;

pub trait EmittableType: Clone {
    fn ts(&self) -> TsNano;
    fn has_change(&self, k: &Self) -> bool;
    fn byte_size(&self) -> u32;
    fn into_data_value(self) -> DataValue;
}

#[derive(Debug, ThisError)]
#[cstm(name = "SerieswriterWriter")]
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
pub struct SeriesWriter<ET> {
    sid: SeriesId,
    ts_msp_last: Option<TsNano>,
    inserted_in_current_msp: u32,
    bytes_in_current_msp: u32,
    msp_max_entries: u32,
    msp_max_bytes: u32,
    // TODO this should be in an Option:
    ts_msp_grid_last: u32,
    _t1: PhantomData<ET>,
}

impl<ET> SeriesWriter<ET>
where
    ET: EmittableType,
{
    pub fn new(sid: SeriesId) -> Result<Self, Error> {
        let res = Self {
            sid,
            ts_msp_last: None,
            inserted_in_current_msp: 0,
            bytes_in_current_msp: 0,
            msp_max_entries: 64000,
            msp_max_bytes: 1024 * 1024 * 20,
            ts_msp_grid_last: 0,
            _t1: PhantomData,
        };
        Ok(res)
    }

    pub fn sid(&self) -> SeriesId {
        self.sid.clone()
    }

    pub fn write(&mut self, item: ET, ts_net: Instant, deque: &mut VecDeque<QueryItem>) -> Result<(), Error> {
        let ts_main = item.ts();

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
                        self.bytes_in_current_msp = item.byte_size();
                        (ts_msp, true)
                    }
                } else {
                    self.inserted_in_current_msp += 1;
                    self.bytes_in_current_msp += item.byte_size();
                    (ts_msp_last, false)
                }
            }
            None => {
                let ts_msp = ts_main.div(msp_res_max).mul(msp_res_max);
                self.ts_msp_last = Some(ts_msp);
                self.inserted_in_current_msp = 1;
                self.bytes_in_current_msp = item.byte_size();
                (ts_msp, true)
            }
        };
        let ts_lsp = ts_main.delta(ts_msp);
        let item = InsertItem {
            series: self.sid.clone(),
            ts_msp: ts_msp.to_ts_ms(),
            ts_lsp,
            ts_net,
            ts_alt_1: ts_main,
            msp_bump: ts_msp_changed,
            val: item.into_data_value(),
        };
        // TODO decide on the path in the new deques struct
        trace!("emit value for ts {:?}", ts_main);
        deque.push_back(QueryItem::Insert(item));
        Ok(())
    }

    pub fn tick(&mut self, deque: &mut VecDeque<QueryItem>) -> Result<(), Error> {
        Ok(())
    }
}
