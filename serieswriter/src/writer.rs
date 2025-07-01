use log;
use netpod::TsNano;
use scywr::iteminsertqueue::QueryItem;
use serde::Serialize;
use series::SeriesId;
use std::collections::VecDeque;
use std::fmt;
use std::marker::PhantomData;
use std::time::Instant;

use netpod::ByteSize;
use scywr::iteminsertqueue::MspItem;
pub use smallvec::SmallVec;

macro_rules! trace_emit { ($det:expr, $($arg:tt)*) => ( if $det { log::trace!($($arg)*); } ) }

autoerr::create_error_v1!(
    name(Error, "SerieswriterWriter"),
    enum variants {
        ChannelSendError,
        ChannelRecvError,
        SeriesLookupError,
        Db(#[from] dbpg::err::Error),
        DbSchema(#[from] dbpg::schema::Error),
        Scy(#[from] scywr::session::Error),
        ScySchema(#[from] scywr::schema::Error),
        Series(#[from] dbpg::seriesbychannel::Error),
    },
);

#[derive(Debug)]
pub struct EmitRes {
    pub data_item: scywr::iteminsertqueue::DataValue,
    pub bytes: ByteSize,
}

pub trait EmittableType: fmt::Debug + Clone {
    type State: fmt::Debug + Serialize;
    fn ts(&self) -> TsNano;
    fn has_change(&self, k: &Self) -> bool;
    fn byte_size(&self) -> u32;
    fn into_query_item(self, ts_net: Instant, tsev: TsNano, state: &mut <Self as EmittableType>::State) -> EmitRes;
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
pub struct WriteRes {
    pub bytes: u32,
}

#[derive(Debug, Serialize)]
pub struct SeriesWriter<ET> {
    series: SeriesId,
    msp_split: crate::msptool::MspSplit,
    do_trace_detail: bool,
    _t1: PhantomData<ET>,
}

impl<ET> SeriesWriter<ET>
where
    ET: EmittableType,
{
    pub fn new(series: SeriesId) -> Result<Self, Error> {
        let res = Self {
            series,
            msp_split: crate::msptool::MspSplit::new(1024 * 64, 1024 * 1024 * 10),
            do_trace_detail: series::dbg::dbg_series(series),
            _t1: PhantomData,
        };
        Ok(res)
    }

    pub fn sid(&self) -> SeriesId {
        self.series.clone()
    }

    pub fn write(
        &mut self,
        item: ET,
        state: &mut <ET as EmittableType>::State,
        ts_net: Instant,
        tsev: TsNano,
        deque: &mut VecDeque<QueryItem>,
    ) -> Result<WriteRes, Error> {
        let det = self.do_trace_detail;
        // let ts_main = item.ts();
        let res = item.into_query_item(ts_net, tsev, state);
        trace_emit!(det, "emit value for ts {tsev}");
        // TODO adapt, taken from trait impl
        let (ts_msp, ts_lsp, ts_msp_chg) = self.msp_split.split(tsev, res.bytes.bytes());
        if ts_msp_chg {
            deque.push_back(QueryItem::Msp(MspItem::new(
                self.series.clone(),
                ts_msp.to_ts_ms(),
                ts_net,
            )));
        }
        let item = scywr::iteminsertqueue::InsertItem {
            series: self.series.clone(),
            ts_msp: ts_msp.to_ts_ms(),
            ts_lsp,
            ts_net,
            val: res.data_item,
        };
        deque.push_back(QueryItem::Insert(item));
        let res = WriteRes {
            bytes: res.bytes.bytes(),
        };
        Ok(res)
    }

    pub fn tick(&mut self, _deque: &mut VecDeque<QueryItem>) -> Result<(), Error> {
        Ok(())
    }

    pub fn on_close(&mut self, _deque: &mut VecDeque<QueryItem>) -> Result<(), Error> {
        Ok(())
    }
}
