use err::thiserror;
use err::ThisError;
use log::*;
use netpod::TsNano;
use scywr::iteminsertqueue::QueryItem;
use series::SeriesId;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::time::Instant;

use core::fmt;
pub use smallvec::SmallVec;

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
    fn into_query_item(self, ts_net: Instant, state: &mut <Self as EmittableType>::State) -> EmitRes;
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
pub struct WriteRes {
    pub bytes: u32,
    pub status: u8,
}

#[derive(Debug)]
pub struct SeriesWriter<ET> {
    sid: SeriesId,
    _t1: PhantomData<ET>,
}

impl<ET> SeriesWriter<ET>
where
    ET: EmittableType,
{
    pub fn new(sid: SeriesId) -> Result<Self, Error> {
        let res = Self { sid, _t1: PhantomData };
        Ok(res)
    }

    pub fn sid(&self) -> SeriesId {
        self.sid.clone()
    }

    pub fn write(
        &mut self,
        item: ET,
        state: &mut <ET as EmittableType>::State,
        ts_net: Instant,
        deque: &mut VecDeque<QueryItem>,
    ) -> Result<WriteRes, Error> {
        let ts_main = item.ts();
        let res = item.into_query_item(ts_net, state);
        trace!("emit value for ts {:?}  items len {}", ts_main, res.items.len());
        for item in res.items {
            deque.push_back(item);
        }
        let res = WriteRes {
            bytes: res.bytes,
            status: res.status,
        };
        Ok(res)
    }

    pub fn tick(&mut self, deque: &mut VecDeque<QueryItem>) -> Result<(), Error> {
        Ok(())
    }
}
