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
use items_2::binsdim0::BinsDim0;
use items_2::eventsdim0::EventsDim0;
use netpod::log::*;
use netpod::timeunits::SEC;
use netpod::BinnedRange;
use netpod::BinnedRangeEnum;
use netpod::ScalarType;
use netpod::Shape;
use netpod::TsNano;
use scywr::iteminsertqueue::DataValue;
use scywr::iteminsertqueue::GetValHelp;
use scywr::iteminsertqueue::QueryItem;
use scywr::iteminsertqueue::TimeBinPatchSimpleF32;
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
    iiq: &'a mut VecDeque<QueryItem>,
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
    acc: Box<dyn Any + Send>,
    push_fn: Box<dyn Fn(PushFnParams) -> Result<(), Error> + Send>,
    tick_fn: Box<dyn Fn(TickParams) -> Result<(), Error> + Send>,
    events_binner: Option<Box<dyn TimeBinner>>,
    patch_collect: PatchCollect,
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
    pub fn empty() -> Self {
        Self {
            did_setup: false,
            series: SeriesId::new(0),
            acc: Box::new(()),
            push_fn: Box::new(push::<i32>),
            tick_fn: Box::new(tick::<i32>),
            events_binner: None,
            patch_collect: PatchCollect::new(TsNano(SEC * 60), 1),
        }
    }

    pub fn setup_for(&mut self, series: SeriesId, scalar_type: &ScalarType, shape: &Shape) -> Result<(), Error> {
        use ScalarType::*;
        self.series = series;
        let tsnow = SystemTime::now();
        let ts0 = SEC * tsnow.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
        let bin_len = self.patch_collect.bin_len();
        let range1 = BinnedRange {
            bin_off: ts0 / bin_len.ns(),
            bin_cnt: u64::MAX / bin_len.ns() - 10,
            bin_len,
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
            //return Err(Error::with_msg_no_trace("ConnTimeBin not yet set up"));
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

    pub fn tick(&mut self, insert_item_queue: &mut VecDeque<QueryItem>) -> Result<(), Error> {
        if !self.did_setup {
            return Ok(());
        }
        let (f,) = (&self.tick_fn,);
        let params = TickParams {
            series: self.series.clone(),
            acc: &mut self.acc,
            tb: self.events_binner.as_mut().unwrap(),
            pc: &mut self.patch_collect,
            iiq: insert_item_queue,
        };
        f(params)
    }
}

fn store_patch(series: SeriesId, pc: &mut PatchCollect, iiq: &mut VecDeque<QueryItem>) -> Result<(), Error> {
    for item in pc.take_outq() {
        if let Some(k) = item.as_any_ref().downcast_ref::<BinsDim0<f32>>() {
            let ts0 = if let Some(x) = k.ts1s.front() {
                *x
            } else {
                return Err(Error::PatchWithoutBins);
            };
            let off = ts0 / pc.patch_len().0;
            let off_msp = off / 1000;
            let off_lsp = off % 1000;
            let item = TimeBinPatchSimpleF32 {
                series: series.clone(),
                bin_len_sec: (pc.bin_len().ns() / SEC) as u32,
                bin_count: pc.bin_count() as u32,
                off_msp: off_msp as u32,
                off_lsp: off_lsp as u32,
                counts: k.counts.iter().map(|x| *x as i64).collect(),
                mins: k.mins.iter().map(|x| *x).collect(),
                maxs: k.maxs.iter().map(|x| *x).collect(),
                avgs: k.avgs.iter().map(|x| *x).collect(),
            };
            let item = QueryItem::TimeBinPatchSimpleF32(item);
            iiq.push_back(item);
        } else {
            error!("unexpected container!");
            return Err(Error::PatchUnexpectedContainer);
        }
    }
    Ok(())
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
    use items_0::WithLen;
    let acc = params.acc;
    let tb = params.tb;
    let pc = params.pc;
    let iiq = params.iiq;
    if let Some(c) = acc.downcast_mut::<EventsDim0<STY>>() {
        if c.len() >= 1 {
            //info!("push events  len {}", c.len());
            tb.ingest(c);
            c.reset();
            if tb.bins_ready_count() >= 1 {
                info!("store bins len {}", tb.bins_ready_count());
                if let Some(mut bins) = tb.bins_ready() {
                    //info!("store bins  {bins:?}");
                    let mut bins = bins.to_simple_bins_f32();
                    pc.ingest(bins.as_mut())?;
                    if pc.outq_len() != 0 {
                        store_patch(params.series.clone(), pc, iiq)?;
                        for item in pc.take_outq() {
                            if let Some(k) = item.as_any_ref().downcast_ref::<BinsDim0<f32>>() {
                                // TODO
                                //let off_msp =
                                let item = TimeBinPatchSimpleF32 {
                                    series: params.series.clone(),
                                    bin_len_sec: (pc.bin_len().ns() / SEC) as u32,
                                    bin_count: pc.bin_count() as u32,
                                    off_msp: 0,
                                    off_lsp: 0,
                                    counts: k.counts.iter().map(|x| *x as i64).collect(),
                                    mins: k.mins.iter().map(|x| *x).collect(),
                                    maxs: k.maxs.iter().map(|x| *x).collect(),
                                    avgs: k.avgs.iter().map(|x| *x).collect(),
                                };
                                let item = QueryItem::TimeBinPatchSimpleF32(item);
                                iiq.push_back(item);
                            } else {
                                error!("unexpected container!");
                            }
                        }
                    }
                    Ok(())
                } else {
                    error!("have bins but none returned");
                    Err(Error::HaveBinsButNoneReturned)
                }
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
