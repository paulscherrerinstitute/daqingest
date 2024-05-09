use crate::patchcollect::PatchCollect;
use core::fmt;
use err::thiserror;
use err::ThisError;
use items_0::scalar_ops::ScalarOps;
use items_0::timebin::TimeBinner;
use items_0::Appendable;
use items_0::Empty;
use items_0::Events;
use items_0::Resettable;
use items_0::WithLen;
use items_2::binsdim0::BinsDim0;
use items_2::eventsdim0::EventsDim0;
use items_2::eventsdim0::EventsDim0TimeBinner;
use netpod::log::*;
use netpod::timeunits::MS;
use netpod::timeunits::SEC;
use netpod::BinnedRange;
use netpod::BinnedRangeEnum;
use netpod::ScalarType;
use netpod::Shape;
use netpod::TsMs;
use netpod::TsNano;
use scywr::insertqueues::InsertDeques;
use scywr::iteminsertqueue::DataValue;
use scywr::iteminsertqueue::GetValHelp;
use scywr::iteminsertqueue::QueryItem;
use scywr::iteminsertqueue::TimeBinSimpleF32;
use series::SeriesId;
use std::any;
use std::any::Any;
use std::collections::VecDeque;
use std::time::SystemTime;

#[allow(unused)]
macro_rules! trace2 {
    ($($arg:tt)*) => {
        if false {
            trace!($($arg)*);
        }
    };
}

#[derive(Debug, ThisError)]
pub enum Error {
    PatchWithoutBins,
    PatchUnexpectedContainer,
    GetValHelpMismatch,
    HaveBinsButNoneReturned,
    ErrError(#[from] err::Error),
}

struct TickParams<'a> {
    series: SeriesId,
    acc: &'a mut Box<dyn Any + Send>,
    tb: &'a mut Box<dyn TimeBinner>,
    pc: &'a mut PatchCollect,
    iqdqs: &'a mut InsertDeques,
    next_coarse: Option<&'a mut EventsDim0TimeBinner<f32>>,
}

pub struct PushFnParams<'a> {
    sid: SeriesId,
    acc: &'a mut Box<dyn Any + Send>,
    ts: TsNano,
    val: &'a DataValue,
}

pub struct ConnTimeBin {
    did_setup: bool,
    series: SeriesId,
    bin_len: TsNano,
    next_coarse: Option<Box<EventsDim0TimeBinner<f32>>>,
    patch_collect: PatchCollect,
    events_binner: Option<Box<dyn TimeBinner>>,
    acc: Box<dyn Any + Send>,
    push_fn: Box<dyn Fn(PushFnParams) -> Result<(), Error> + Send>,
    tick_fn: Box<dyn Fn(TickParams) -> Result<(), Error> + Send>,
}

impl fmt::Debug for ConnTimeBin {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("ConnTimeBin")
            .field("did_setup", &self.did_setup)
            .field("series", &self.series)
            .field("acc", &self.acc)
            // .field("push_fn", &self.push_fn)
            // .field("tick_fn", &self.tick_fn)
            .field("events_binner", &self.events_binner)
            .field("patch_collect", &self.patch_collect)
            .finish()
    }
}

impl ConnTimeBin {
    pub fn empty(series: SeriesId, bin_len: TsNano) -> Self {
        let do_time_weight = true;
        #[cfg(DISABLED)]
        let next_coarse = if bin_len.ns() < SEC * 60 {
            type ST = f32;
            let brange = BinnedRange {
                bin_len: TsNano::from_ns(SEC * 60),
                bin_off: todo!(),
                bin_cnt: todo!(),
            };
            let binned_range = BinnedRangeEnum::Time(brange);
            let tb = EventsDim0TimeBinner::<ST>::new(binned_range, do_time_weight).unwrap();
            Some(tb)
        } else if bin_len.ns() < SEC * 60 * 2 {
            todo!()
        } else if bin_len.ns() < SEC * 60 * 10 {
            todo!()
        } else {
            None
        }
        .map(Box::new);
        Self {
            patch_collect: PatchCollect::new(bin_len.clone(), 1),
            did_setup: false,
            series,
            bin_len,
            next_coarse: None,
            events_binner: None,
            acc: Box::new(()),
            push_fn: Box::new(push::<i32>),
            tick_fn: Box::new(tick::<i32>),
        }
    }

    pub fn setup_for(&mut self, scalar_type: &ScalarType, shape: &Shape, tsnow: SystemTime) -> Result<(), Error> {
        use ScalarType::*;
        // TODO should not take a system time here:
        let bin_len = &self.bin_len;
        let ts0 = SEC * tsnow.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
        let range1 = BinnedRange {
            bin_off: ts0 / bin_len.ns(),
            bin_cnt: u64::MAX / bin_len.ns() - 10,
            bin_len: bin_len.clone(),
        };
        let binrange = BinnedRangeEnum::Time(range1);
        //info!("binrange {binrange:?}");
        let do_time_weight = true;
        match shape {
            Shape::Scalar => {
                type Cont<T> = EventsDim0<T>;
                match scalar_type {
                    I8 => {
                        type ST = i8;
                        trace2!("SCALAR {}", any::type_name::<ST>());
                        let cont = Cont::<ST>::empty();
                        self.events_binner =
                            Some(cont.as_time_binnable_ref().time_binner_new(binrange, do_time_weight));
                        self.acc = Box::new(cont);
                        self.push_fn = Box::new(push::<ST>);
                        self.tick_fn = Box::new(tick::<ST>);
                        self.did_setup = true;
                    }
                    I16 => {
                        type ST = i16;
                        trace2!("SCALAR {}", std::any::type_name::<ST>());
                        let cont = Cont::<ST>::empty();
                        self.events_binner =
                            Some(cont.as_time_binnable_ref().time_binner_new(binrange, do_time_weight));
                        self.acc = Box::new(cont);
                        self.push_fn = Box::new(push::<ST>);
                        self.tick_fn = Box::new(tick::<ST>);
                        self.did_setup = true;
                    }
                    I32 => {
                        type ST = i32;
                        trace2!("SCALAR {}", std::any::type_name::<ST>());
                        let cont = Cont::<ST>::empty();
                        self.events_binner =
                            Some(cont.as_time_binnable_ref().time_binner_new(binrange, do_time_weight));
                        self.acc = Box::new(cont);
                        self.push_fn = Box::new(push::<ST>);
                        self.tick_fn = Box::new(tick::<ST>);
                        self.did_setup = true;
                    }
                    F32 => {
                        type ST = f32;
                        trace2!("SCALAR {}", std::any::type_name::<ST>());
                        let cont = Cont::<ST>::empty();
                        self.events_binner =
                            Some(cont.as_time_binnable_ref().time_binner_new(binrange, do_time_weight));
                        self.acc = Box::new(cont);
                        self.push_fn = Box::new(push::<ST>);
                        self.tick_fn = Box::new(tick::<ST>);
                        self.did_setup = true;
                    }
                    F64 => {
                        type ST = f64;
                        trace2!("SCALAR {}", std::any::type_name::<ST>());
                        let cont = Cont::<ST>::empty();
                        self.events_binner =
                            Some(cont.as_time_binnable_ref().time_binner_new(binrange, do_time_weight));
                        self.acc = Box::new(cont);
                        self.push_fn = Box::new(push::<ST>);
                        self.tick_fn = Box::new(tick::<ST>);
                        self.did_setup = true;
                    }
                    STRING => {
                        trace2!("TODO  setup_event_acc  {:?}  {:?}", scalar_type, shape);
                    }
                    _ => {
                        trace2!("TODO  setup_event_acc  {:?}  {:?}", scalar_type, shape);
                    }
                }
            }
            Shape::Wave(..) => {
                //type Cont<T> = EventsDim1<T>;
                match scalar_type {
                    _ => {
                        trace2!("TODO  setup_event_acc  {:?}  {:?}", scalar_type, shape);
                    }
                }
            }
            _ => {
                trace2!("TODO  setup_event_acc  {:?}  {:?}", scalar_type, shape);
            }
        }
        Ok(())
    }

    pub fn push(&mut self, ts: TsNano, val: &DataValue) -> Result<(), Error> {
        if !self.did_setup {
            // TODO record as logic error
            return Ok(());
        }
        let (f, acc) = (&self.push_fn, &mut self.acc);
        let params = PushFnParams {
            sid: self.series.clone(),
            acc,
            ts,
            val,
        };
        f(params)
    }

    pub fn tick(&mut self, iqdqs: &mut InsertDeques) -> Result<(), Error> {
        if !self.did_setup {
            return Ok(());
        }
        let (f,) = (&self.tick_fn,);
        let params = TickParams {
            series: self.series.clone(),
            acc: &mut self.acc,
            tb: self.events_binner.as_mut().unwrap(),
            pc: &mut self.patch_collect,
            iqdqs,
            next_coarse: self.next_coarse.as_mut().map(|x| x.as_mut()),
        };
        f(params)
    }
}

fn push<STY>(params: PushFnParams) -> Result<(), Error>
where
    STY: ScalarOps,
    DataValue: GetValHelp<STY, ScalTy = STY>,
{
    let sid = &params.sid;
    let ts = params.ts;
    let v = match GetValHelp::<STY>::get(params.val) {
        Ok(x) => x,
        Err(e) => {
            // TODO throttle the error
            let msg = format!(
                "GetValHelp mismatch:  series {:?}  STY {}  data {:?}  {e}",
                sid,
                any::type_name::<STY>(),
                params.val
            );
            error!("{msg}");
            return Err(Error::GetValHelpMismatch);
        }
    };
    if let Some(c) = params.acc.downcast_mut::<EventsDim0<STY>>() {
        c.push(ts.ns(), 0, v.clone());
        Ok(())
    } else {
        // TODO report once and error out
        error!("unexpected container");
        //Err(Error::with_msg_no_trace("unexpected container"))
        Ok(())
    }
}

fn tick<STY>(params: TickParams) -> Result<(), Error>
where
    STY: ScalarOps,
{
    let acc = params.acc;
    let tb = params.tb;
    // let pc = params.pc;
    let iqdqs = params.iqdqs;
    let next = params.next_coarse;
    if let Some(c) = acc.downcast_mut::<EventsDim0<STY>>() {
        if c.len() >= 1 {
            tb.ingest(c);
            c.reset();
            let nbins = tb.bins_ready_count();
            if nbins >= 1 {
                trace!("store bins len {}  {:?}", nbins, params.series);
                store_bins(params.series.clone(), tb, iqdqs, next)?;
                // if let Some(mut bins) = tb.bins_ready() {
                //     //info!("store bins  {bins:?}");
                //     let mut bins = bins.to_simple_bins_f32();

                //     TODO;

                //     pc.ingest(bins.as_mut())?;
                //     let noutq = pc.outq_len();
                //     info!("noutq  {noutq}");
                //     if noutq != 0 {
                //         store_patch(params.series.clone(), pc, iiq)?;
                //         Ok(())
                //     } else {
                //         warn!("pc outq len zero");
                //         Ok(())
                //     }
                // } else {
                //     error!("have bins but none returned");
                //     Err(Error::HaveBinsButNoneReturned)
                // }

                Ok(())
            } else {
                Ok(())
            }
        } else {
            Ok(())
        }
    } else {
        error!("unexpected container");
        //Err(Error::with_msg_no_trace("unexpected container"))
        Ok(())
    }
}

fn store_bins(
    series: SeriesId,
    tb: &mut Box<dyn TimeBinner>,
    iqdqs: &mut InsertDeques,
    next: Option<&mut EventsDim0TimeBinner<f32>>,
) -> Result<(), Error> {
    if let Some(mut bins) = tb.bins_ready() {
        let bins = bins.to_simple_bins_f32();
        if let Some(k) = bins.as_any_ref().downcast_ref::<BinsDim0<f32>>() {
            if k.len() == 0 {
                return Err(Error::PatchWithoutBins);
            } else {
                for (((((&ts1, &ts2), &count), &min), &max), &avg) in k
                    .ts1s
                    .iter()
                    .zip(k.ts2s.iter())
                    .zip(k.counts.iter())
                    .zip(k.mins.iter())
                    .zip(k.maxs.iter())
                    .zip(k.avgs.iter())
                {
                    // TODO the inner must be of BinsDim0<f32> type so we feed also count, min, max, etc.
                    if let Some(next) = &next {
                        // next.ingest();
                    }

                    // TODO this must depend on the data type: waveforms need smaller batches
                    let bins_per_msp = 82000;

                    let ts1ms = ts1 / MS;
                    let ts2ms = ts2 / MS;
                    let bin_len_ms = ts2ms - ts1ms;
                    let h = bins_per_msp * bin_len_ms;
                    let ts_msp = ts1ms / h * h;
                    let off = (ts1ms - ts_msp) / bin_len_ms;
                    let item = TimeBinSimpleF32 {
                        series: series.clone(),
                        bin_len_ms: bin_len_ms as i32,
                        ts_msp: TsMs::from_ms_u64(ts_msp),
                        off: off as i32,
                        count: count as i64,
                        min,
                        max,
                        avg,
                    };
                    let item = QueryItem::TimeBinSimpleF32(item);
                    trace!("push item B  ts1ms {ts1ms}  bin_len_ms {bin_len_ms}  ts_msp {ts_msp}  off {off}");

                    // TODO check which RT we want to push into
                    iqdqs.st_rf3_rx.push_back(item.clone());
                    // iqdqs.mt_rf3_rx.push_back(item.clone());
                    // iqdqs.lt_rf3_rx.push_back(item);
                }
            }
            Ok(())
        } else {
            error!("unexpected container!");
            Err(Error::PatchUnexpectedContainer)
        }
        // TODO feed also the next patch collector for the next coarse resolution.
        // pc.ingest(bins.as_mut())?;
        // let noutq = pc.outq_len();
        // info!("noutq  {noutq}");
        // if noutq != 0 {
        //     store_patch(params.series.clone(), pc, iiq)?;
        //     Ok(())
        // } else {
        //     warn!("pc outq len zero");
        //     Ok(())
        // }
    } else {
        error!("have bins but none returned");
        Err(Error::HaveBinsButNoneReturned)
    }
}

fn store_patch(series: SeriesId, pc: &mut PatchCollect, iiq: &mut VecDeque<QueryItem>) -> Result<(), Error> {
    // TODO
    // I probably still want to keep the "patchcollect" because I want to store also the next
    // resolutions.
    // But I need to emit each bin as they come.

    for item in pc.take_outq() {
        if let Some(k) = item.as_any_ref().downcast_ref::<BinsDim0<f32>>() {
            let ts0 = if let Some(x) = k.ts1s.front() {
                *x
            } else {
                return Err(Error::PatchWithoutBins);
            };

            // TODO insert each bin individually

            let bin_len_sec = (pc.bin_len().ns() / MS);
            let bin_count = pc.bin_count();
            let off = ts0 / pc.patch_len().0;
            let off_msp = off / 1000;
            let off_lsp = off % 1000;
            // let item = TimeBinSimpleF32 {
            // };
            // let item = QueryItem::TimeBinSimpleF32(item);
            // warn!(
            //     "push item B  bin_len_sec {bin_len_sec}  bin_count {bin_count}  off_msp {off_msp}  off_lsp {off_lsp}"
            // );
            // iiq.push_back(item);
        } else {
            error!("unexpected container!");
            return Err(Error::PatchUnexpectedContainer);
        }
    }
    Ok(())
}
