use crate::ratelimitwriter::RateLimitWriter;
use crate::writer::EmittableType;
use err::thiserror;
use err::ThisError;
use netpod::log::*;
use netpod::ScalarType;
use netpod::Shape;
use scywr::insertqueues::InsertDeques;
use scywr::iteminsertqueue::QueryItem;
use series::SeriesId;
use std::collections::VecDeque;
use std::time::Duration;
use std::time::Instant;

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
struct State<ET>
where
    ET: EmittableType,
{
    writer: RateLimitWriter<ET>,
}

#[derive(Debug)]
pub struct WriteRes {
    pub st: WriteRtRes,
    pub mt: WriteRtRes,
    pub lt: WriteRtRes,
}

impl WriteRes {
    pub fn nstatus(&self) -> u8 {
        self.st.status + self.mt.status + self.lt.status
    }
}

#[derive(Debug)]
pub struct WriteRtRes {
    pub accept: bool,
    pub bytes: u32,
    pub status: u8,
}

#[derive(Debug)]
pub struct RtWriter<ET>
where
    ET: EmittableType,
{
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
        emit_state_new: &dyn Fn() -> <ET as EmittableType>::State,
    ) -> Result<Self, Error> {
        let state_st = {
            // let writer = SeriesWriter::establish_with_sid(sid, stnow)?;
            let writer = RateLimitWriter::new(series, min_quiets.st, emit_state_new(), "st".into())?;
            State { writer }
        };
        let state_mt = {
            let writer = RateLimitWriter::new(series, min_quiets.mt, emit_state_new(), "mt".into())?;
            State { writer }
        };
        let state_lt = {
            let writer = RateLimitWriter::new(series, min_quiets.lt, emit_state_new(), "lt".into())?;
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

    pub fn write(&mut self, item: ET, ts_net: Instant, iqdqs: &mut InsertDeques) -> Result<WriteRes, Error> {
        trace!("write  {:?}", item.ts());
        // TODO
        // Optimize for the common case that we only write into one of the stores.
        // Make the decision first, based on ref, then clone only as required.
        let res_st = Self::write_inner(&mut self.state_st, item.clone(), ts_net, &mut iqdqs.st_rf3_qu)?;
        let res_mt = Self::write_inner(&mut self.state_mt, item.clone(), ts_net, &mut iqdqs.mt_rf3_qu)?;
        let res_lt = Self::write_inner(&mut self.state_lt, item, ts_net, &mut iqdqs.lt_rf3_qu)?;
        let ret = WriteRes {
            st: WriteRtRes {
                accept: res_st.accept,
                bytes: res_st.bytes,
                status: res_st.status,
            },
            mt: WriteRtRes {
                accept: res_mt.accept,
                bytes: res_mt.bytes,
                status: res_mt.status,
            },
            lt: WriteRtRes {
                accept: res_lt.accept,
                bytes: res_lt.bytes,
                status: res_lt.status,
            },
        };
        Ok(ret)
    }

    fn write_inner(
        state: &mut State<ET>,
        item: ET,
        ts_net: Instant,
        deque: &mut VecDeque<QueryItem>,
    ) -> Result<crate::ratelimitwriter::WriteRes, Error> {
        Ok(state.writer.write(item, ts_net, deque)?)
    }

    pub fn tick(&mut self, iqdqs: &mut InsertDeques) -> Result<(), Error> {
        self.state_st.writer.tick(&mut iqdqs.st_rf3_qu)?;
        self.state_mt.writer.tick(&mut iqdqs.mt_rf3_qu)?;
        self.state_lt.writer.tick(&mut iqdqs.lt_rf3_qu)?;
        Ok(())
    }
}
