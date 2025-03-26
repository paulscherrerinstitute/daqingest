use crate::log;
use crate::ratelimitwriter::RateLimitWriter;
use crate::writer::EmittableType;
use netpod::ScalarType;
use netpod::Shape;
use netpod::TsNano;
use scywr::insertqueues::InsertDeques;
use scywr::iteminsertqueue::QueryItem;
use serde::Serialize;
use series::SeriesId;
use std::collections::VecDeque;
use std::time::Duration;
use std::time::Instant;

macro_rules! debug_init { ($det:expr, $($arg:expr),*) => ( if $det { log::info!($($arg),*); } ); }
macro_rules! trace_emit { ($det:expr, $($arg:expr),*) => ( if $det { log::trace!($($arg),*); } ); }
macro_rules! trace_rt_decision { ($det:expr, $($arg:expr),*) => ( if $det { log::trace!($($arg),*); } ); }

autoerr::create_error_v1!(
    name(Error, "SerieswriterRtwriter"),
    enum variants {
        SeriesLookupError,
        SeriesWriter(#[from] crate::writer::Error),
        RateLimitWriter(#[from] crate::ratelimitwriter::Error),
    },
);

#[derive(Debug, Clone, Serialize)]
pub struct MinQuiets {
    pub st: Duration,
    pub mt: Duration,
    pub lt: Duration,
}

#[derive(Debug, Serialize)]
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

    pub fn accept_any(&self) -> bool {
        self.lt.accept || self.mt.accept || self.st.accept
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

#[derive(Debug, Serialize)]
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
    do_st_rf1: bool,
    last_insert_ts: TsNano,
    last_insert_val: Option<ET>,
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
        do_st_rf1: bool,
        emit_state_new: &dyn Fn() -> <ET as EmittableType>::State,
    ) -> Result<Self, Error> {
        let dtd = series::dbg::dbg_series(series);
        debug_init!(dtd, "new  {:?}  is_polled {}", min_quiets, is_polled);
        let state_st = {
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
            do_trace_detail: dtd,
            do_st_rf1,
            last_insert_ts: TsNano::from_ns(0),
            last_insert_val: None,
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
        let res_mt;
        let res_st;
        let tsl = self.last_insert_ts.clone();
        if tsev < tsl {
            trace_rt_decision!(
                det,
                "{}  ignore, because rewind time  {:?}  {:?}",
                self.series,
                tsev,
                tsl
            );
            res_lt = WriteRtRes::default();
            res_mt = WriteRtRes::default();
            res_st = WriteRtRes::default();
        } else if tsev == tsl {
            trace_rt_decision!(det, "{}  ignore, because same time  {:?}  {:?}", self.series, tsev, tsl);
            res_lt = WriteRtRes::default();
            res_mt = WriteRtRes::default();
            res_st = WriteRtRes::default();
        } else if self
            .last_insert_val
            .as_ref()
            .map(|k| item.has_change(k))
            .unwrap_or(true)
            == false
        {
            trace_rt_decision!(det, "{}  ignore, because value did not change", self.series);
            res_lt = WriteRtRes::default();
            res_mt = WriteRtRes::default();
            res_st = WriteRtRes::default();
        } else {
            res_lt = Self::write_inner(&mut self.state_lt, item.clone(), ts_net, tsev, &mut iqdqs.lt_rf3_qu)?;
            res_mt = Self::write_inner(&mut self.state_mt, item.clone(), ts_net, tsev, &mut iqdqs.mt_rf3_qu)?;
            res_st = if self.do_st_rf1 {
                // Self::write_inner(&mut self.state_st, item.clone(), ts_net, tsev, &mut iqdqs.st_rf1_qu)?
                Self::write_inner(&mut self.state_st, item.clone(), ts_net, tsev, &mut iqdqs.st_rf3_qu)?
            } else {
                Self::write_inner(&mut self.state_st, item.clone(), ts_net, tsev, &mut iqdqs.st_rf3_qu)?
            };
        }
        let ret = WriteRes {
            st: res_st,
            mt: res_mt,
            lt: res_lt,
        };
        if ret.accept_any() {
            self.last_insert_ts = tsev.clone();
            self.last_insert_val = Some(item.clone());
        }
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
        if self.do_st_rf1 {
            self.state_st.writer.tick(&mut iqdqs.st_rf1_qu)?;
        } else {
            self.state_st.writer.tick(&mut iqdqs.st_rf3_qu)?;
        }
        self.state_mt.writer.tick(&mut iqdqs.mt_rf3_qu)?;
        self.state_lt.writer.tick(&mut iqdqs.lt_rf3_qu)?;
        Ok(())
    }
}
