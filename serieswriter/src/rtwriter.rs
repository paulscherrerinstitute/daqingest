use crate::ratelimitwriter::RateLimitWriter;
use crate::writer::EmittableType;
use err::thiserror;
use err::ThisError;
use netpod::log::*;
use netpod::ScalarType;
use netpod::Shape;
use netpod::TsNano;
use scywr::insertqueues::InsertDeques;
use scywr::iteminsertqueue::QueryItem;
use series::SeriesId;
use std::collections::VecDeque;
use std::time::Duration;
use std::time::Instant;

#[allow(unused)]
macro_rules! trace_emit {
    ($det:expr, $($arg:tt)*) => {
        if $det {
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

impl Default for WriteRtRes {
    fn default() -> Self {
        Self {
            accept: false,
            bytes: 0,
            status: 0,
        }
    }
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
    do_trace_detail: bool,
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
        is_polled: bool,
        emit_state_new: &dyn Fn() -> <ET as EmittableType>::State,
    ) -> Result<Self, Error> {
        let state_st = {
            // let writer = SeriesWriter::establish_with_sid(sid, stnow)?;
            let writer = RateLimitWriter::new(series, min_quiets.st, is_polled, emit_state_new(), "st".into())?;
            State { writer }
        };
        let state_mt = {
            let writer = RateLimitWriter::new(series, min_quiets.mt, is_polled, emit_state_new(), "mt".into())?;
            State { writer }
        };
        let state_lt = {
            let writer = RateLimitWriter::new(series, min_quiets.lt, is_polled, emit_state_new(), "lt".into())?;
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
            do_trace_detail: netpod::TRACE_SERIES_ID.contains(&series.id()),
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
        tsev: TsNano,
        iqdqs: &mut InsertDeques,
    ) -> Result<WriteRes, Error> {
        let det = self.do_trace_detail;
        trace_emit!(det, "write  {:?}", item.ts());
        // TODO
        // Optimize for the common case that we only write into one of the stores.
        // Make the decision first, based on ref, then clone only as required.
        let res_lt;
        let mut res_mt = WriteRtRes::default();
        let mut res_st = WriteRtRes::default();
        {
            res_lt = Self::write_inner(&mut self.state_lt, item.clone(), ts_net, tsev, &mut iqdqs.lt_rf3_qu)?;
            if !res_lt.accept {
                res_mt = Self::write_inner(&mut self.state_mt, item.clone(), ts_net, tsev, &mut iqdqs.mt_rf3_qu)?;
                if !res_mt.accept {
                    res_st = Self::write_inner(&mut self.state_st, item.clone(), ts_net, tsev, &mut iqdqs.st_rf3_qu)?;
                }
            }
        }
        let ret = WriteRes {
            st: res_st,
            mt: res_mt,
            lt: res_lt,
        };
        Ok(ret)
    }

    fn write_inner(
        state: &mut State<ET>,
        item: ET,
        ts_net: Instant,
        tsev: TsNano,
        deque: &mut VecDeque<QueryItem>,
    ) -> Result<WriteRtRes, Error> {
        let x = state.writer.write(item, ts_net, tsev, deque)?;
        let ret = WriteRtRes {
            accept: x.accept,
            bytes: x.bytes,
            status: x.status,
        };
        Ok(ret)
    }

    pub fn tick(&mut self, iqdqs: &mut InsertDeques) -> Result<(), Error> {
        self.state_st.writer.tick(&mut iqdqs.st_rf3_qu)?;
        self.state_mt.writer.tick(&mut iqdqs.mt_rf3_qu)?;
        self.state_lt.writer.tick(&mut iqdqs.lt_rf3_qu)?;
        Ok(())
    }
}
