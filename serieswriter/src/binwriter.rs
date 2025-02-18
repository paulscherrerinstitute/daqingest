use crate::log::*;
use crate::rtwriter::MinQuiets;
use items_0::timebin::BinnedBinsTimeweightTrait;
use items_0::timebin::BinnedEventsTimeweightTrait;
use items_0::timebin::BinsBoxed;
use items_2::binning::container_bins::ContainerBins;
use items_2::binning::container_events::ContainerEvents;
use items_2::binning::timeweight::timeweight_bins::BinnedBinsTimeweight;
use items_2::binning::timeweight::timeweight_bins_lazy::BinnedBinsTimeweightLazy;
use items_2::binning::timeweight::timeweight_events::BinnedEventsTimeweight;
use items_2::binning::timeweight::timeweight_events_dyn::BinnedEventsTimeweightLazy;
use netpod::ttl::RetentionTime;
use netpod::BinnedRange;
use netpod::DtMs;
use netpod::ScalarType;
use netpod::Shape;
use netpod::TsNano;
use scywr::insertqueues::InsertDeques;
use scywr::iteminsertqueue::QueryItem;
use scywr::iteminsertqueue::TimeBinSimpleF32V02;
use series::ChannelStatusSeriesId;
use series::SeriesId;
use std::time::Duration;

macro_rules! trace_ingest { ($($arg:expr),*) => ( if false { trace!($($arg),*); } ) }
macro_rules! trace_tick { ($($arg:expr),*) => ( if false { trace!($($arg),*); } ) }
macro_rules! trace_tick_verbose { ($($arg:expr),*) => ( if false { trace!($($arg),*); } ) }

macro_rules! debug_bin2 { ($t:expr, $($arg:expr),*) => ( if true { if $t { debug!($($arg),*); } } ) }
macro_rules! trace_bin2 { ($t:expr, $($arg:expr),*) => ( if false { if $t { trace!($($arg),*); } } ) }

autoerr::create_error_v1!(
    name(Error, "SerieswriterBinwriter"),
    enum variants {
        SeriesLookupError,
        SeriesWriter(#[from] crate::writer::Error),
        Binning(#[from] items_2::binning::timeweight::timeweight_events::Error),
        UnsupportedBinGrid(DtMs),
        BinBinning(#[from] items_0::timebin::BinningggError),
        UnexpectedContainerType,
        PartitionMsp(#[from] series::msp::Error),
        UnsupportedGridDiv(DtMs, DtMs),
    },
);

fn bin_len_clamp(dur: Duration) -> DtMs {
    let dur = DtMs::from_ms_u64(1000 * dur.as_secs());
    if dur < DtMs::from_ms_u64(1000 * 2) {
        DtMs::from_ms_u64(1000 * 1)
    } else if dur <= DtMs::from_ms_u64(1000 * 20) {
        DtMs::from_ms_u64(1000 * 10)
    } else if dur <= DtMs::from_ms_u64(1000 * 60 * 2) {
        DtMs::from_ms_u64(1000 * 60 * 1)
    } else if dur <= DtMs::from_ms_u64(1000 * 60 * 20) {
        DtMs::from_ms_u64(1000 * 60 * 10)
    } else if dur <= DtMs::from_ms_u64(1000 * 60 * 60 * 2) {
        DtMs::from_ms_u64(1000 * 60 * 60 * 1)
    } else {
        DtMs::from_ms_u64(1000 * 60 * 60 * 24)
    }
}

fn get_div(bin_len: DtMs) -> Result<DtMs, Error> {
    let pbp = series::msp::PrebinnedPartitioning::try_from(bin_len)?;
    let ret = pbp.msp_div();
    Ok(ret)
}

#[derive(Debug, Clone)]
enum WriteCntZero {
    Enable,
    Disable,
}

impl WriteCntZero {
    fn enabled(&self) -> bool {
        match self {
            WriteCntZero::Enable => true,
            WriteCntZero::Disable => false,
        }
    }
}

#[derive(Debug)]
pub struct BinWriter {
    chname: String,
    cssid: ChannelStatusSeriesId,
    sid: SeriesId,
    scalar_type: ScalarType,
    shape: Shape,
    evbuf: ContainerEvents<f32>,
    binner_1st: Option<(RetentionTime, BinnedEventsTimeweight<f32>, WriteCntZero)>,
    binner_others: Vec<(RetentionTime, BinnedBinsTimeweight<f32, f32>, WriteCntZero)>,
    trd: bool,
}

impl BinWriter {
    pub fn new(
        beg: TsNano,
        min_quiets: MinQuiets,
        cssid: ChannelStatusSeriesId,
        sid: SeriesId,
        scalar_type: ScalarType,
        shape: Shape,
        chname: String,
    ) -> Result<Self, Error> {
        let trd = series::dbg::dbg_chn(&chname);
        if trd {
            debug_bin2!(trd, "enabled debug for {}", chname);
        }
        const DUR_ZERO: DtMs = DtMs::from_ms_u64(0);
        const DUR_MAX: DtMs = DtMs::from_ms_u64(1000 * 60 * 60 * 24 * 40);
        let rts = [RetentionTime::Short, RetentionTime::Medium, RetentionTime::Long];
        let quiets = [min_quiets.st.clone(), min_quiets.mt.clone(), min_quiets.lt.clone()];
        let mut binner_1st = None;
        let mut binner_others = Vec::new();
        let mut combs: Vec<_> = rts
            .into_iter()
            .zip(quiets.into_iter().map(bin_len_clamp))
            .map(|x| (x.0, x.1, WriteCntZero::Disable))
            .collect();
        if let Some(last) = combs.last_mut() {
            if last.1 >= DtMs::from_ms_u64(1000 * 60 * 60 * 24) {
                last.0 = RetentionTime::Long;
                last.1 = DtMs::from_ms_u64(1000 * 60 * 60 * 24);
                last.2 = WriteCntZero::Enable;
            } else if last.1 >= DtMs::from_ms_u64(1000 * 60 * 60 * 1) {
                last.0 = RetentionTime::Long;
                last.1 = DtMs::from_ms_u64(1000 * 60 * 60 * 1);
                combs.push((
                    RetentionTime::Long,
                    DtMs::from_ms_u64(1000 * 60 * 60 * 24),
                    WriteCntZero::Enable,
                ));
            } else {
                combs.push((
                    RetentionTime::Long,
                    DtMs::from_ms_u64(1000 * 60 * 60 * 1),
                    WriteCntZero::Disable,
                ));
                combs.push((
                    RetentionTime::Long,
                    DtMs::from_ms_u64(1000 * 60 * 60 * 24),
                    WriteCntZero::Enable,
                ));
            }
        }
        // check
        for e in combs.iter() {
            if get_div(e.1).is_err() {
                info!("unsupported bin length  {:?}  {:?}  {:?}", e.0, e.1, chname);
                combs.clear();
                break;
            }
        }
        let combs = combs;
        debug_bin2!(trd, "{:?} binning combs {:?}", chname, combs);
        for (rt, bin_len, write_zero) in combs {
            if bin_len > DUR_ZERO && bin_len <= DUR_MAX {
                if binner_1st.is_none() {
                    let range = BinnedRange::from_beg_to_inf(beg, bin_len);
                    let mut binner = BinnedEventsTimeweight::new(range);
                    if let WriteCntZero::Enable = write_zero {
                        binner.cnt_zero_enable();
                    }
                    binner_1st = Some((rt, binner, write_zero));
                } else {
                    let range = BinnedRange::from_beg_to_inf(beg, bin_len);
                    let binner = BinnedBinsTimeweight::new(range);
                    if let WriteCntZero::Enable = write_zero {
                        // TODO
                        // binner.cnt_zero_enable();
                    }
                    binner_others.push((rt, binner, write_zero));
                }
            }
        }
        let ret = Self {
            chname,
            cssid,
            sid,
            scalar_type,
            shape,
            evbuf: ContainerEvents::new(),
            binner_1st,
            binner_others,
            trd,
        };
        let _ = ret.cssid;
        Ok(ret)
    }

    pub fn sid(&self) -> SeriesId {
        self.sid.clone()
    }

    pub fn scalar_type(&self) -> ScalarType {
        self.scalar_type.clone()
    }

    pub fn shape(&self) -> Shape {
        self.shape.clone()
    }

    pub fn ingest(&mut self, ts_local: TsNano, val: f32, iqdqs: &mut InsertDeques) -> Result<(), Error> {
        let _ = iqdqs;
        trace_ingest!("ingest  {ts_local}  {val}");
        self.evbuf.push_back(ts_local, val);
        Ok(())
    }

    pub fn tick(&mut self, iqdqs: &mut InsertDeques) -> Result<(), Error> {
        if self.evbuf.len() != 0 {
            trace_tick!("tick  evbuf len {}", self.evbuf.len());
            let buf = &self.evbuf;
            if true {
                if let Some(ee) = self.binner_1st.as_mut() {
                    let rt = ee.0.clone();
                    let write_zero = ee.2.clone();
                    let binner = &mut ee.1;
                    // TODO avoid boxing
                    binner.ingest(&Box::new(buf))?;
                    let bins = binner.output();
                    if bins.len() > 0 {
                        trace_bin2!(self.trd, "binner_1st  out len {}", bins.len());
                        Self::handle_output_ready(self.trd, self.sid, rt, &bins, write_zero, iqdqs)?;
                        // TODO avoid boxing
                        let mut bins2: BinsBoxed = Box::new(bins);
                        for i in 0..self.binner_others.len() {
                            let (rt, binner, write_zero) = &mut self.binner_others[i];
                            let write_zero = write_zero.clone();
                            binner.ingest(&bins2)?;
                            let bb: Option<BinsBoxed> = binner.output()?;
                            match bb {
                                Some(bb) => {
                                    if bb.len() > 0 {
                                        trace_bin2!(self.trd, "binner_others {}  out len {}", i, bb.len());
                                        if let Some(bb2) = bb.as_any_ref().downcast_ref::<ContainerBins<f32, f32>>() {
                                            Self::handle_output_ready(
                                                self.trd,
                                                self.sid,
                                                rt.clone(),
                                                &bb2,
                                                write_zero,
                                                iqdqs,
                                            )?;
                                        } else {
                                            return Err(Error::UnexpectedContainerType);
                                        }
                                        bins2 = bb;
                                    } else {
                                        break;
                                    }
                                }
                                None => {
                                    break;
                                }
                            }
                        }
                    } else {
                        // nothing to do
                    }
                } else {
                    // nothing to do
                }
            }
            self.evbuf.clear();
        } else {
            trace_tick_verbose!("tick  nothing to ingest");
        }
        Ok(())
    }

    fn handle_output_ready(
        trd: bool,
        series: SeriesId,
        rt: RetentionTime,
        bins: &ContainerBins<f32, f32>,
        write_zero: WriteCntZero,
        iqdqs: &mut InsertDeques,
    ) -> Result<(), Error> {
        let selfname = "handle_output_ready";
        trace_tick!("{selfname}  bins ready len {}", bins.len());
        for e in bins.iter_debug() {
            trace_tick_verbose!("{e:?}");
        }
        let bins_len = bins.len();
        for (ts1, ts2, cnt, min, max, avg, lst, fnl) in bins.zip_iter_2() {
            let bin_len = DtMs::from_ms_u64(ts2.delta(ts1).ms_u64());
            if fnl == false {
                info!("non final bin  {:?}", series);
            } else if cnt == 0 && !write_zero.enabled() {
                info!("zero count bin  {:?}", series);
            } else {
                let div = get_div(bin_len)?;
                if div.ns() % bin_len.ns() != 0 {
                    let e = Error::UnsupportedGridDiv(bin_len, div);
                    return Err(e);
                }
                let msp = ts1.ms() / div.ms();
                let off = (ts1.ms() - div.ms() * msp) / bin_len.ms();
                let item = QueryItem::TimeBinSimpleF32V02(TimeBinSimpleF32V02 {
                    series,
                    binlen: bin_len.ms() as i32,
                    msp: msp as i64,
                    off: off as i32,
                    cnt: cnt as i64,
                    min,
                    max,
                    avg,
                    dev: f32::NAN,
                    lst,
                });
                if bin_len >= DtMs::from_ms_u64(1000 * 60 * 60) {
                    debug_bin2!(trd, "handle_output_ready  emit  {:?}  len {}  {:?}", rt, bins_len, item);
                }
                match rt {
                    RetentionTime::Short => {
                        iqdqs.st_rf3_qu.push_back(item);
                    }
                    RetentionTime::Medium => {
                        iqdqs.mt_rf3_qu.push_back(item);
                    }
                    RetentionTime::Long => {
                        iqdqs.lt_rf3_qu.push_back(item);
                    }
                }
            }
        }
        Ok(())
    }
}
