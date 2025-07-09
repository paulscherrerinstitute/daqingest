use crate::msptool::MspSplit;
use crate::writer::EmittableType;
use crate::writer::SeriesWriter;
use core::fmt;
use netpod::DtNano;
use netpod::TsNano;
use netpod::log;
use scywr::iteminsertqueue::QueryItem;
use serde::Serialize;
use series::SeriesId;
use std::collections::VecDeque;
use std::time::Duration;
use std::time::Instant;

macro_rules! debug { ($($arg:tt)*) => ( if true { log::debug!($($arg)*); } ); }
macro_rules! trace { ($($arg:tt)*) => ( if true { log::trace!($($arg)*); } ); }
macro_rules! trace_rt_decision { ($dtd:expr, $($arg:tt)*) => ( if $dtd { log::trace!($($arg)*); } ); }

autoerr::create_error_v1!(
    name(Error, "RateLimitWriter"),
    enum variants {
        SeriesWriter(#[from] crate::writer::Error),
    },
);

#[derive(Debug)]
pub struct WriteRes {
    pub accept: bool,
    pub bytes: u32,
    pub msp_rewrite: u8,
    pub ignore_monitor_not_min_quiet: u8,
    pub ignore_poll_not_min_quiet: u8,
    pub ignore_rate_cap: u8,
}

#[derive(Debug)]
pub struct HousekeepingRes {
    pub ts_msp_reput: u8,
}

#[derive(Serialize)]
pub struct RateLimitWriter<ET, SPL>
where
    ET: EmittableType,
{
    series: SeriesId,
    min_quiet: Duration,
    is_polled: bool,
    emit_state: <ET as EmittableType>::State,
    last_insert_ts: TsNano,
    last_insert_val: Option<ET>,
    dbgname: String,
    writer: SeriesWriter<ET, SPL>,
    do_trace_detail: bool,
}

impl<ET, SPL> RateLimitWriter<ET, SPL>
where
    ET: EmittableType,
    SPL: MspSplit,
{
    pub fn new(
        series: SeriesId,
        min_quiet: Duration,
        is_polled: bool,
        emit_state: <ET as EmittableType>::State,
        dbgname: String,
        spl: SPL,
    ) -> Result<Self, Error> {
        let writer = SeriesWriter::new(series, spl)?;
        let ret = Self {
            series,
            min_quiet,
            is_polled,
            emit_state,
            last_insert_ts: TsNano::from_ns(0),
            last_insert_val: None,
            dbgname,
            writer,
            do_trace_detail: series::dbg::dbg_series(series),
        };
        if ret.do_trace_detail {
            debug!("debug test for detail series");
            trace!("trace test for detail series");
        }
        Ok(ret)
    }

    pub fn write(
        &mut self,
        item: ET,
        ts_net: Instant,
        tsev: TsNano,
        deque: &mut VecDeque<QueryItem>,
    ) -> Result<WriteRes, Error> {
        let dtd = self.do_trace_detail;
        let dbgname = &self.dbgname;
        let sid = &self.series;
        let min_quiet = 1000 * self.min_quiet.as_secs() + self.min_quiet.subsec_millis() as u64;
        let tsl = self.last_insert_ts.clone();
        let ts = tsev;
        let mut ignore_monitor_not_min_quiet: u8 = 0;
        let mut ignore_poll_not_min_quiet: u8 = 0;
        let mut ignore_rate_cap: u8 = 0;
        let do_write = {
            if !self.is_polled && ts.ms() < tsl.ms() + min_quiet {
                trace_rt_decision!(
                    dtd,
                    "{}  {}  ignore, because monitor not min quiet  {}  {}",
                    dbgname,
                    sid,
                    ts,
                    tsl
                );
                ignore_monitor_not_min_quiet += 1;
                false
            } else if self.is_polled && ts.ms() + 800 < tsl.ms() + min_quiet {
                trace_rt_decision!(
                    dtd,
                    "{}  {}  ignore, because poll not min quiet  {}  {}",
                    dbgname,
                    sid,
                    ts,
                    tsl
                );
                ignore_poll_not_min_quiet += 1;
                false
            } else if ts < tsl.add_dt_nano(DtNano::from_ms(1)) {
                trace_rt_decision!(
                    dtd,
                    "{}  {}  ignore, because store rate cap  {}  {}",
                    dbgname,
                    sid,
                    ts,
                    tsl
                );
                ignore_rate_cap += 1;
                false
            } else {
                trace_rt_decision!(dtd, "{}  {}  accept  {}  {}", dbgname, sid, ts, tsl);
                true
            }
        };
        if do_write {
            self.last_insert_ts = ts;
            let res = self.writer.write(item, &mut self.emit_state, ts_net, ts, deque)?;
            let ret = WriteRes {
                accept: true,
                bytes: res.bytes,
                msp_rewrite: res.msp_rewrite,
                ignore_monitor_not_min_quiet,
                ignore_poll_not_min_quiet,
                ignore_rate_cap,
            };
            Ok(ret)
        } else {
            let ret = WriteRes {
                accept: false,
                bytes: 0,
                msp_rewrite: 0,
                ignore_monitor_not_min_quiet,
                ignore_poll_not_min_quiet,
                ignore_rate_cap,
            };
            Ok(ret)
        }
    }

    pub fn tick(&mut self, iqdqs: &mut VecDeque<QueryItem>) -> Result<(), Error> {
        let ret = self.writer.tick(iqdqs)?;
        Ok(ret)
    }

    pub fn on_close(&mut self, iqdqs: &mut VecDeque<QueryItem>) -> Result<(), Error> {
        self.writer.on_close(iqdqs)?;
        Ok(())
    }

    pub fn housekeeping(&mut self, deque: &mut VecDeque<QueryItem>) -> Result<HousekeepingRes, Error> {
        let res = self.writer.housekeeping(deque)?;
        let ret = HousekeepingRes {
            ts_msp_reput: res.ts_msp_reput,
        };
        Ok(ret)
    }
}

impl<ET, SPL> fmt::Debug for RateLimitWriter<ET, SPL>
where
    ET: EmittableType,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("RateLimitWriter")
            .field("min_quiet", &self.min_quiet)
            .field("last_insert_ts", &self.last_insert_ts)
            .field("last_insert_val", &self.last_insert_val)
            .finish()
    }
}
