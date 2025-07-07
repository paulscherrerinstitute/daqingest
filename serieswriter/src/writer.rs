use log;
use netpod::TsNano;
use scywr::iteminsertqueue::QueryItem;
use serde::Serialize;
use series::SeriesId;
use std::collections::VecDeque;
use std::fmt;
use std::marker::PhantomData;
use std::time::Instant;

use crate::msptool::MspSplit;
use netpod::ByteSize;
use netpod::ttl::RetentionTime;
use scywr::iteminsertqueue::MspItem;
pub use smallvec::SmallVec;
use std::time::Duration;

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
    pub msp_rewrite: u8,
}

#[derive(Debug)]
pub struct HousekeepingRes {
    pub ts_msp_reput: u8,
}

#[derive(Debug)]
pub struct OnCloseRes {
    pub ts_msp_reput: u8,
}

#[derive(Debug, Serialize)]
pub struct SeriesWriter<ET, SPL> {
    series: SeriesId,
    msp_split: SPL,
    evts_on_msp_write: TsNano,
    evts_latest: TsNano,
    do_trace_detail: bool,
    #[serde(skip)]
    ts_check_ts_msp_reput_last: Instant,
    #[serde(skip)]
    ts_ts_msp_put_last: Option<Instant>,
    _t1: PhantomData<ET>,
}

impl<ET, SPL> SeriesWriter<ET, SPL>
where
    ET: EmittableType,
    SPL: MspSplit,
{
    pub fn new(series: SeriesId, spl: SPL) -> Result<Self, Error> {
        let res = Self {
            series,
            msp_split: spl,
            evts_on_msp_write: TsNano::from_ns(0),
            evts_latest: TsNano::from_ns(0),
            do_trace_detail: series::dbg::dbg_series(series),
            ts_check_ts_msp_reput_last: Instant::now(),
            ts_ts_msp_put_last: None,
            _t1: PhantomData,
        };
        Ok(res)
    }

    pub fn sid(&self) -> SeriesId {
        self.series.clone()
    }

    pub fn rt(&self) -> RetentionTime {
        self.msp_split.rt()
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
        let res = item.into_query_item(ts_net, tsev, state);
        trace_emit!(det, "emit value for ts {tsev}");
        let mut msp_rewrite = 0;
        let (ts_msp, ts_lsp, ts_msp_chg, ts_msp_retired) = self.msp_split.split(tsev, res.bytes.bytes());
        if let Some(msp) = ts_msp_retired {
            let item = MspItem::new(self.series.clone(), msp.to_ts_ms(), ts_net);
            deque.push_back(QueryItem::Msp(item));
            msp_rewrite += 1;
        }
        if ts_msp_chg {
            let item = MspItem::new(self.series.clone(), ts_msp.to_ts_ms(), ts_net);
            deque.push_back(QueryItem::Msp(item));
            self.evts_on_msp_write = tsev;
            self.ts_ts_msp_put_last = Some(Instant::now());
        }
        let item = scywr::iteminsertqueue::InsertItem {
            series: self.series.clone(),
            ts_msp: ts_msp.to_ts_ms(),
            ts_lsp,
            ts_net,
            val: res.data_item,
        };
        deque.push_back(QueryItem::Insert(item));
        self.evts_latest = tsev;
        let res = WriteRes {
            bytes: res.bytes.bytes(),
            msp_rewrite,
        };
        Ok(res)
    }

    pub fn tick(&mut self, _deque: &mut VecDeque<QueryItem>) -> Result<(), Error> {
        Ok(())
    }

    pub fn on_close(&mut self, deque: &mut VecDeque<QueryItem>) -> Result<OnCloseRes, Error> {
        if let Some(msp) = self.msp_split.ts_msp_current() {
            let item = MspItem::new(self.series.clone(), msp.to_ts_ms(), Instant::now());
            deque.push_back(QueryItem::Msp(item));
        }
        let ret = OnCloseRes { ts_msp_reput: 1 };
        Ok(ret)
    }

    pub fn housekeeping(&mut self, deque: &mut VecDeque<QueryItem>) -> Result<HousekeepingRes, Error> {
        let tsnow = Instant::now();
        let mut ts_msp_reput = 0;
        if self.ts_check_ts_msp_reput_last + Duration::from_millis(1000 * 60) <= tsnow {
            self.ts_check_ts_msp_reput_last = tsnow;
            if let Some(ts_ts_msp_put_last) = self.ts_ts_msp_put_last {
                if ts_ts_msp_put_last + Duration::from_millis(1000 * 60 * 60) <= tsnow {
                    if self.evts_latest != self.evts_on_msp_write {
                        if let Some(msp) = self.msp_split.ts_msp_current() {
                            self.ts_ts_msp_put_last = Some(tsnow);
                            self.evts_on_msp_write = self.evts_latest;
                            let item = MspItem::new(self.series.clone(), msp.to_ts_ms(), tsnow);
                            deque.push_back(QueryItem::Msp(item));
                            ts_msp_reput += 1;
                        } else {
                            // TODO return this, it should not happen
                        }
                    } else {
                    }
                } else {
                }
            } else {
            }
        } else {
        }
        let ret = HousekeepingRes { ts_msp_reput };
        Ok(ret)
    }
}
