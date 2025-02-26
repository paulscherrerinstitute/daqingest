use core::fmt;
use log::*;
use netpod::TsNano;
use scywr::iteminsertqueue::QueryItem;
use series::SeriesId;
pub use smallvec::SmallVec;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::time::Instant;

macro_rules! trace_emit { ($det:expr, $($arg:tt)*) => ( if $det { trace!($($arg)*); } ) }

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
    pub items: SmallVec<[QueryItem; 4]>,
    pub bytes: u32,
    pub status: u8,
}

pub trait EmittableType: fmt::Debug + Clone {
    type State;
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
    pub status: u8,
}

#[derive(Debug)]
pub struct SeriesWriter<ET> {
    series: SeriesId,
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
            do_trace_detail: netpod::TRACE_SERIES_ID.contains(&series.id()),
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
        let ts_main = item.ts();
        let res = item.into_query_item(ts_net, tsev, state);
        trace_emit!(det, "emit value for ts {:?}  items len {}", ts_main, res.items.len());
        for item in res.items {
            deque.push_back(item);
        }
        let res = WriteRes {
            bytes: res.bytes,
            status: res.status,
        };
        Ok(res)
    }

    pub fn tick(&mut self, _deque: &mut VecDeque<QueryItem>) -> Result<(), Error> {
        Ok(())
    }
}
