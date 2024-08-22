use crate::writer::EmittableType;
use crate::writer::SeriesWriter;
use core::fmt;
use err::thiserror;
use err::ThisError;
use netpod::log::*;
use netpod::DtNano;
use netpod::TsNano;
use scywr::iteminsertqueue::QueryItem;
use series::SeriesId;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::time::Duration;
use std::time::Instant;

#[allow(unused)]
macro_rules! trace_rt_decision {
    ($det:expr, $($arg:tt)*) => {
        if $det {
            trace!($($arg)*);
        }
    };
}

#[derive(Debug, ThisError)]
#[cstm(name = "RateLimitWriter")]
pub enum Error {
    SeriesWriter(#[from] crate::writer::Error),
}

#[derive(Debug)]
pub struct WriteRes {
    pub accept: bool,
    pub bytes: u32,
    pub status: u8,
}

pub struct RateLimitWriter<ET>
where
    ET: EmittableType,
{
    series: SeriesId,
    min_quiet: Duration,
    emit_state: <ET as EmittableType>::State,
    last_insert_ts: TsNano,
    last_insert_val: Option<ET>,
    dbgname: String,
    writer: SeriesWriter<ET>,
    do_trace_detail: bool,
    _t1: PhantomData<ET>,
}

impl<ET> RateLimitWriter<ET>
where
    ET: EmittableType,
{
    pub fn new(
        series: SeriesId,
        min_quiet: Duration,
        emit_state: <ET as EmittableType>::State,
        dbgname: String,
    ) -> Result<Self, Error> {
        let writer = SeriesWriter::new(series)?;
        let ret = Self {
            series,
            min_quiet,
            emit_state,
            last_insert_ts: TsNano::from_ns(0),
            last_insert_val: None,
            dbgname,
            writer,
            do_trace_detail: netpod::TRACE_SERIES_ID.contains(&series.id()),
            _t1: PhantomData,
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
        // Decide whether we want to write.
        // TODO catch already in CaConn the cases when the IOC-timestamp did not change.
        let det = self.do_trace_detail;
        let tsl = self.last_insert_ts.clone();
        let dbgname = &self.dbgname;
        let sid = &self.series;
        let min_quiet = 1000 * self.min_quiet.as_secs() + self.min_quiet.subsec_millis() as u64;
        let ts = tsev;
        if false {
            trace_rt_decision!(
                det,
                "{dbgname}  {sid}  min_quiet {min_quiet:?}  ts1 {ts1:?}  ts2 {ts2:?}  item {item:?}",
                ts1 = ts.ms(),
                ts2 = tsl.ms(),
                item = item,
            );
        }
        let do_write = {
            if ts == tsl {
                trace_rt_decision!(det, "{dbgname}  {sid}  ignore, because same time  {ts:?}  {tsl:?}");
                false
            } else if ts < tsl {
                trace_rt_decision!(
                    det,
                    "{dbgname}  {sid}  ignore, because ts_local  rewind  {ts:?}  {tsl:?}",
                );
                false
            } else if ts.ms() < tsl.ms() + min_quiet {
                trace_rt_decision!(det, "{dbgname}  {sid}  ignore, because not min quiet  {ts:?}  {tsl:?}");
                false
            } else if ts < tsl.add_dt_nano(DtNano::from_ms(5)) {
                trace_rt_decision!(det, "{dbgname}  {sid}  ignore, because store rate cap");
                false
            } else if self
                .last_insert_val
                .as_ref()
                .map(|k| !item.has_change(k))
                .unwrap_or(false)
            {
                trace_rt_decision!(det, "{dbgname}  {sid}  ignore, because value did not change");
                false
            } else {
                trace_rt_decision!(det, "{dbgname}  {sid}  accept");
                if true {
                    self.last_insert_val = Some(item.clone());
                }
                self.last_insert_ts = ts.clone();
                true
            }
        };
        if do_write {
            let res = self.writer.write(item, &mut self.emit_state, ts_net, ts, deque)?;
            let ret = WriteRes {
                accept: true,
                bytes: res.bytes,
                status: res.status,
            };
            Ok(ret)
        } else {
            let ret = WriteRes {
                accept: false,
                bytes: 0,
                status: 0,
            };
            Ok(ret)
        }
    }

    pub fn tick(&mut self, iqdqs: &mut VecDeque<QueryItem>) -> Result<(), Error> {
        let ret = self.writer.tick(iqdqs)?;
        Ok(ret)
    }
}

impl<ET> fmt::Debug for RateLimitWriter<ET>
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
