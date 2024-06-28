use any::Any;
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
use netpod::f32_close;
use netpod::log::*;
use netpod::timeunits::MS;
use netpod::ttl::RetentionTime;
use netpod::BinnedRange;
use netpod::BinnedRangeEnum;
use netpod::DtMs;
use netpod::DtNano;
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

#[allow(unused)]
macro_rules! todo_setup {
    ($($arg:tt)*) => {
        if true {
            debug!($($arg)*);
        }
    };
}

#[allow(unused)]
macro_rules! trace_store_bin {
    ($($arg:tt)*) => {
        if true {
            trace!($($arg)*);
        }
    };
}

#[allow(unused)]
macro_rules! trace_setup {
    ($($arg:tt)*) => {
        if true {
            trace!($($arg)*);
        }
    };
}

#[allow(unused)]
macro_rules! trace_tick {
    ($($arg:tt)*) => {
        if true {
            trace!($($arg)*);
        }
    };
}

#[allow(unused)]
macro_rules! trace_push {
    ($($arg:tt)*) => {
        if true {
            trace!($($arg)*);
        }
    };
}

#[derive(Debug, ThisError)]
pub enum Error {
    UnexpectedContainer,
    PatchWithoutBins,
    PatchUnexpectedContainer,
    GetValHelpMismatch,
    HaveBinsButNoneReturned,
    UnsupportedType,
    Unsupported,
}

struct TickParams<'a> {
    rt: RetentionTime,
    series: SeriesId,
    acc: &'a mut (dyn Any + Send),
    tb: &'a mut dyn TimeBinner,
    iqdqs: &'a mut InsertDeques,
    next_coarse: Option<&'a mut EventsDim0TimeBinner<f32>>,
}

pub struct PushFnParams<'a> {
    series: SeriesId,
    acc: &'a mut (dyn Any + Send),
    ts: TsNano,
    val: &'a DataValue,
}

struct Internal {
    push_fn: Box<dyn Fn(PushFnParams) -> Result<(), Error> + Send>,
    tick_fn: Box<dyn Fn(TickParams) -> Result<(), Error> + Send>,
}

impl fmt::Debug for Internal {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Internal")
            .field("push_fn", &())
            .field("tick_fn", &())
            .finish()
    }
}

struct SetupResult {
    events_binner: Box<dyn TimeBinner>,
    acc: Box<dyn Any + Send>,
    push_fn: Box<dyn Fn(PushFnParams) -> Result<(), Error> + Send>,
    tick_fn: Box<dyn Fn(TickParams) -> Result<(), Error> + Send>,
}

#[derive(Debug)]
pub struct ConnTimeBin {
    rt: RetentionTime,
    series: SeriesId,
    #[allow(unused)]
    bin_len: DtNano,
    next_coarse: Option<Box<EventsDim0TimeBinner<f32>>>,
    events_binner: Box<dyn TimeBinner>,
    acc: Box<dyn Any + Send>,
    internal: Internal,
    unsup: bool,
}

impl ConnTimeBin {
    pub fn new(
        rt: RetentionTime,
        series: SeriesId,
        beg: TsNano,
        bin_len: DtNano,
        scalar_type: ScalarType,
        shape: Shape,
    ) -> Result<Self, Error> {
        let do_time_weight = true;
        #[cfg(target_abi = "12")]
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
        let mut unsup = false;
        let k = Self::setup_for(beg, bin_len, &scalar_type, &shape, do_time_weight);
        let k = if k.is_ok() {
            k
        } else {
            unsup = true;
            Self::setup_for(beg, bin_len, &ScalarType::F32, &Shape::Scalar, do_time_weight)
        };
        let k = k?;
        let ret = Self {
            rt,
            series,
            bin_len,
            next_coarse: None,
            events_binner: k.events_binner,
            acc: k.acc,
            internal: Internal {
                push_fn: k.push_fn,
                tick_fn: k.tick_fn,
            },
            unsup,
        };
        Ok(ret)
    }

    fn setup_for(
        beg: TsNano,
        bin_len: DtNano,
        scalar_type: &ScalarType,
        shape: &Shape,
        do_time_weight: bool,
    ) -> Result<SetupResult, Error> {
        // TODO should not take a system time here:
        let range1 = BinnedRange {
            bin_off: beg.ns() / bin_len.ns(),
            bin_cnt: u64::MAX / bin_len.ns() - 10,
            // TODO fix trait requirements
            bin_len: TsNano::from_ns(bin_len.ns()),
        };
        let binrange = BinnedRangeEnum::Time(range1);
        match shape {
            Shape::Scalar => {
                use ScalarType::*;
                match scalar_type {
                    U8 => Self::setup_scalar::<u8>(binrange, do_time_weight),
                    U16 => Self::setup_scalar::<u16>(binrange, do_time_weight),
                    U32 => Self::setup_scalar::<u32>(binrange, do_time_weight),
                    U64 => Self::setup_scalar::<u64>(binrange, do_time_weight),
                    I8 => Self::setup_scalar::<i8>(binrange, do_time_weight),
                    I16 => Self::setup_scalar::<i16>(binrange, do_time_weight),
                    I32 => Self::setup_scalar::<i32>(binrange, do_time_weight),
                    I64 => Self::setup_scalar::<i64>(binrange, do_time_weight),
                    F32 => Self::setup_scalar::<f32>(binrange, do_time_weight),
                    F64 => Self::setup_scalar::<f64>(binrange, do_time_weight),
                    STRING => {
                        todo_setup!("TODO  setup_event_acc  {:?}  {:?}", scalar_type, shape);
                        Err(Error::UnsupportedType)
                    }
                    _ => {
                        todo_setup!("TODO  setup_event_acc  {:?}  {:?}", scalar_type, shape);
                        Err(Error::UnsupportedType)
                    }
                }
            }
            Shape::Wave(..) => match scalar_type {
                _ => {
                    todo_setup!("TODO  setup_event_acc  {:?}  {:?}", scalar_type, shape);
                    Err(Error::UnsupportedType)
                }
            },
            _ => {
                todo_setup!("TODO  setup_event_acc  {:?}  {:?}", scalar_type, shape);
                Err(Error::UnsupportedType)
            }
        }
    }

    fn setup_scalar<ST>(binrange: BinnedRangeEnum, do_time_weight: bool) -> Result<SetupResult, Error>
    where
        ST: ScalarOps,
        DataValue: GetValHelp<ST, ScalTy = ST>,
    {
        trace_setup!("SCALAR {}", any::type_name::<ST>());
        type Cont<T> = EventsDim0<T>;
        let cont = Cont::<ST>::empty();
        let emit_empty_bins = false;
        let ret = SetupResult {
            events_binner: cont
                .as_time_binnable_ref()
                .time_binner_new(binrange, do_time_weight, emit_empty_bins),
            acc: Box::new(cont),
            push_fn: Box::new(push::<ST>),
            tick_fn: Box::new(tick::<ST>),
        };
        Ok(ret)
    }

    pub fn push(&mut self, ts: TsNano, val: &DataValue) -> Result<(), Error> {
        if self.unsup {
            return Ok(());
        }
        let (f, acc) = (&self.internal.push_fn, self.acc.as_mut());
        let params = PushFnParams {
            series: self.series.clone(),
            acc,
            ts,
            val,
        };
        f(params)
    }

    pub fn tick(&mut self, iqdqs: &mut InsertDeques) -> Result<(), Error> {
        if self.unsup {
            return Ok(());
        }
        let (f,) = (&self.internal.tick_fn,);
        let params = TickParams {
            rt: self.rt.clone(),
            series: self.series.clone(),
            acc: self.acc.as_mut(),
            tb: self.events_binner.as_mut(),
            // pc: &mut self.patch_collect,
            iqdqs,
            next_coarse: self.next_coarse.as_mut().map(|x| x.as_mut()),
        };
        f(params)
    }

    pub fn finish(&mut self, iqdqs: &mut InsertDeques) -> Result<(), Error> {
        if self.unsup {
            return Ok(());
        }
        let series = self.series.clone();
        let tb = self.events_binner.as_mut();
        tb.push_in_progress(false);
        let nbins = tb.bins_ready_count();
        if nbins >= 1 {
            trace_store_bin!("finish  nbins {}  {:?}", nbins, series);
            let rt = self.rt.clone();
            let next_coarse = self.next_coarse.as_mut().map(|x| x.as_mut());
            store_bins(rt, series, tb, iqdqs, next_coarse)?;
        }
        Ok(())
    }
}

fn push<STY>(params: PushFnParams) -> Result<(), Error>
where
    STY: ScalarOps,
    DataValue: GetValHelp<STY, ScalTy = STY>,
{
    let series = &params.series;
    let ts = params.ts;
    let v = match GetValHelp::<STY>::get(params.val) {
        Ok(x) => x,
        Err(e) => {
            // TODO throttle the error
            let msg = format!(
                "GetValHelp mismatch:  series {:?}  STY {}  data {:?}  {e}",
                series,
                any::type_name::<STY>(),
                params.val
            );
            error!("{msg}");
            return Err(Error::GetValHelpMismatch);
        }
    };
    if let Some(c) = params.acc.downcast_mut::<EventsDim0<STY>>() {
        trace_push!("PUSHED");
        c.push(ts.ns(), 0, v.clone());
        Ok(())
    } else {
        Err(Error::UnexpectedContainer)
    }
}

fn tick<STY>(params: TickParams) -> Result<(), Error>
where
    STY: ScalarOps,
{
    let rt = params.rt;
    let acc = params.acc;
    let tb = params.tb;
    let iqdqs = params.iqdqs;
    let next = params.next_coarse;
    if let Some(c) = acc.downcast_mut::<EventsDim0<STY>>() {
        trace_tick!("TICK CONV");
        if c.len() >= 1 {
            trace_tick!("TICK EVENTS");
            tb.ingest(c);
            c.reset();
            trace_tick!("TICK INGESTED");
            let nbins = tb.bins_ready_count();
            if nbins >= 1 {
                trace_tick!("TICK READY {nbins}");
                trace_store_bin!("store bins len {}  {:?}", nbins, params.series);
                store_bins(rt, params.series.clone(), tb, iqdqs, next)?;
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
                trace_tick!("TICK NOT READY");
                Ok(())
            }
        } else {
            trace_tick!("TICK NO EVENTS TO PROCESS");
            Ok(())
        }
    } else {
        trace_tick!("TICK UNEXPECTED CONTAINER");
        Err(Error::UnexpectedContainer)
    }
}

fn store_bins(
    rt: RetentionTime,
    series: SeriesId,
    tb: &mut dyn TimeBinner,
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
                    if let Some(_next) = &next {
                        // next.ingest();
                    }

                    let ts1 = TsMs::from_ms_u64(ts1 / MS);
                    let ts2 = TsMs::from_ms_u64(ts2 / MS);
                    let bin_len = ts2 - ts1;
                    let h = if bin_len == DtMs::from_ms_u64(1000 * 10) {
                        DtMs::from_ms_u64(1000 * 60 * 60 * 2)
                    } else {
                        // TODO
                        return Err(Error::Unsupported);
                    };
                    let ts_msp = TsMs::from_ms_u64(ts1.ms() / h.ms() * h.ms());
                    let off = (ts1.ms() - ts_msp.ms()) / bin_len.ms();
                    let item = TimeBinSimpleF32 {
                        series: series.clone(),
                        bin_len_ms: bin_len.ms() as i32,
                        ts_msp,
                        off: off as i32,
                        count: count as i64,
                        min,
                        max,
                        avg,
                    };
                    let item = QueryItem::TimeBinSimpleF32(item);
                    trace_store_bin!("push item B  ts1ms {ts1:?}  bin_len_ms {bin_len:?}  ts_msp {ts_msp}  off {off}");
                    iqdqs.deque(rt.clone()).push_back(item.clone());
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

#[test]
fn test_00() {
    let mut ctb = init_scalar_f32_conn_time_bin().unwrap();
    let mut iqdqs = InsertDeques::new();
    ctb.tick(&mut iqdqs).unwrap();
    assert_eq!(iqdqs.len(), 0);
}

#[test]
fn test_01() {
    use scywr::iteminsertqueue::ScalarValue;
    let mut ctb = init_scalar_f32_conn_time_bin().unwrap();
    ctb.push(TsNano::from_ms(1000), &DataValue::Scalar(ScalarValue::I32(10)))
        .unwrap();
    let mut iqdqs = InsertDeques::new();
    ctb.tick(&mut iqdqs).unwrap();
    assert_eq!(iqdqs.len(), 0);
}

#[test]
fn test_02() {
    use scywr::iteminsertqueue::ScalarValue;
    let mut ctb = init_scalar_f32_conn_time_bin().unwrap();
    ctb.push(TsNano::from_ms(1000 * 10), &DataValue::Scalar(ScalarValue::I32(10)))
        .unwrap();
    ctb.push(TsNano::from_ms(1000 * 11), &DataValue::Scalar(ScalarValue::I32(12)))
        .unwrap();
    ctb.push(TsNano::from_ms(1000 * 12), &DataValue::Scalar(ScalarValue::I32(10)))
        .unwrap();
    let mut iqdqs = InsertDeques::new();
    ctb.tick(&mut iqdqs).unwrap();
    ctb.finish(&mut iqdqs).unwrap();
    assert_eq!(iqdqs.len(), 1);
    for e in iqdqs.st_rf3_rx {
        eprintln!("{e:?}");
        if let QueryItem::TimeBinSimpleF32(x) = e {
            assert!(f32_close(x.avg, 10.2));
        } else {
            panic!();
        }
    }
}

#[cfg(test)]
fn init_scalar_f32_conn_time_bin() -> Result<ConnTimeBin, Error> {
    let rt = RetentionTime::Short;
    let series = SeriesId::new(1);
    let beg = TsNano::from_ms(1000 * 10);
    let bin_len = DtNano::from_ms(1000 * 10);
    let scalar_type = ScalarType::I32;
    let shape = Shape::Scalar;
    let ctb = ConnTimeBin::new(rt, series, beg, bin_len, scalar_type, shape).unwrap();
    Ok(ctb)
}
