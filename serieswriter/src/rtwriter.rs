use crate::ratelimitwriter::RateLimitWriter;
use crate::writer::EmittableType;
use async_channel::Sender;
use dbpg::seriesbychannel::ChannelInfoQuery;
use err::thiserror;
use err::ThisError;
use netpod::log::*;
use netpod::ScalarType;
use netpod::SeriesKind;
use netpod::Shape;
use netpod::TsNano;
use scywr::insertqueues::InsertDeques;
use scywr::iteminsertqueue::DataValue;
use scywr::iteminsertqueue::QueryItem;
use series::SeriesId;
use std::collections::VecDeque;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;

#[allow(unused)]
macro_rules! trace_ {
    ($($arg:tt)*) => {
        if false {
            trace!($($arg)*);
        }
    };
}

#[derive(Debug, ThisError)]
#[cstm(name = "SerieswriterRtwriter")]
pub enum Error {
    SeriesLookupError,
    SeriesWriter(#[from] crate::writer::Error),
    RateLimitWriter(#[from] crate::ratelimitwriter::Error),
}

#[derive(Debug, Clone)]
pub struct MinQuiets {
    pub st: Duration,
    pub mt: Duration,
    pub lt: Duration,
}

#[derive(Debug)]
struct State<ET> {
    writer: RateLimitWriter<ET>,
}

#[derive(Debug)]
pub struct RtWriter<ET> {
    series: SeriesId,
    scalar_type: ScalarType,
    shape: Shape,
    state_st: State<ET>,
    state_mt: State<ET>,
    state_lt: State<ET>,
    min_quiets: MinQuiets,
}

impl<ET> RtWriter<ET>
where
    ET: EmittableType,
{
    pub fn new(
        series: SeriesId,
        scalar_type: ScalarType,
        shape: Shape,
        min_quiets: MinQuiets,
        stnow: SystemTime,
    ) -> Result<Self, Error> {
        let state_st = {
            // let writer = SeriesWriter::establish_with_sid(sid, stnow)?;
            let writer = RateLimitWriter::new(series, min_quiets.st, "st".into())?;
            State { writer }
        };
        let state_mt = {
            let writer = RateLimitWriter::new(series, min_quiets.mt, "mt".into())?;
            State { writer }
        };
        let state_lt = {
            let writer = RateLimitWriter::new(series, min_quiets.lt, "lt".into())?;
            State { writer }
        };
        let ret = Self {
            series,
            scalar_type,
            shape,
            state_st,
            state_mt,
            state_lt,
            min_quiets,
        };
        Ok(ret)
    }

    pub fn series(&self) -> SeriesId {
        self.series.clone()
    }

    pub fn scalar_type(&self) -> ScalarType {
        self.scalar_type.clone()
    }

    pub fn shape(&self) -> Shape {
        self.shape.clone()
    }

    pub fn min_quiets(&self) -> MinQuiets {
        self.min_quiets.clone()
    }

    pub fn write(
        &mut self,
        item: ET,
        ts_net: Instant,
        iqdqs: &mut InsertDeques,
    ) -> Result<((bool, bool, bool),), Error> {
        trace!("write  {:?}", item.ts());
        // TODO
        // Optimize for the common case that we only write into one of the stores.
        // Make the decision first, based on ref, then clone only as required.
        let (did_write_st,) = Self::write_inner(&mut self.state_st, item.clone(), ts_net, &mut iqdqs.st_rf3_rx)?;
        let (did_write_mt,) = Self::write_inner(&mut self.state_mt, item.clone(), ts_net, &mut iqdqs.mt_rf3_rx)?;
        let (did_write_lt,) = Self::write_inner(&mut self.state_lt, item, ts_net, &mut iqdqs.lt_rf3_rx)?;
        Ok(((did_write_st, did_write_mt, did_write_lt),))
    }

    fn write_inner(
        state: &mut State<ET>,
        item: ET,
        ts_net: Instant,
        deque: &mut VecDeque<QueryItem>,
    ) -> Result<(bool,), Error> {
        Ok(state.writer.write(item, ts_net, deque)?)
    }

    pub fn tick(&mut self, iqdqs: &mut InsertDeques) -> Result<(), Error> {
        self.state_st.writer.tick(&mut iqdqs.st_rf3_rx)?;
        self.state_mt.writer.tick(&mut iqdqs.mt_rf3_rx)?;
        self.state_lt.writer.tick(&mut iqdqs.lt_rf3_rx)?;
        Ok(())
    }
}

#[derive(Debug)]
struct LastIns {
    ts_local: TsNano,
    ts_ioc: TsNano,
    val: DataValue,
}
