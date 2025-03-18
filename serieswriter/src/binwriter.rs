use crate::log;
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
use netpod::BinnedRange;
use netpod::DtMs;
use netpod::ScalarType;
use netpod::Shape;
use netpod::TsNano;
use netpod::ttl::RetentionTime;
use scywr::insertqueues::InsertDeques;
use scywr::iteminsertqueue::BinWriteIndexV03;
use scywr::iteminsertqueue::QueryItem;
use scywr::iteminsertqueue::TimeBinSimpleF32V02;
use series::ChannelStatusSeriesId;
use series::SeriesId;
use series::msp::PrebinnedPartitioning;
use std::time::Duration;

macro_rules! info { ($($arg:expr),*) => ( if true { log::info!($($arg),*); } ) }

macro_rules! debug_init { ($t:expr, $($arg:expr),*) => ( if true { if $t { log::debug!($($arg),*); } } ) }
macro_rules! debug_bin { ($t:expr, $($arg:expr),*) => ( if true { if $t { log::debug!($($arg),*); } } ) }

macro_rules! trace_ingest { ($($arg:expr),*) => ( if false { log::trace!($($arg),*); } ) }
macro_rules! trace_tick { ($($arg:expr),*) => ( if true { log::trace!($($arg),*); } ) }
macro_rules! trace_tick_verbose { ($($arg:expr),*) => ( if true { log::trace!($($arg),*); } ) }

macro_rules! trace_bin { ($t:expr, $($arg:expr),*) => ( if true { if $t { log::trace!($($arg),*); } } ) }

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
        UnexpectedBinLen(DtMs, PrebinnedPartitioning),
        BinnerNoProgress,
        IngestLoopLimit,
    },
);

fn bin_len_clamp(dur: DtMs) -> PrebinnedPartitioning {
    if dur < DtMs::from_ms_u64(1000 * 2) {
        PrebinnedPartitioning::Sec1
    } else if dur <= DtMs::from_ms_u64(1000 * 20) {
        PrebinnedPartitioning::Sec10
    } else if dur <= DtMs::from_ms_u64(1000 * 60 * 2) {
        PrebinnedPartitioning::Min1
    } else if dur <= DtMs::from_ms_u64(1000 * 60 * 20) {
        PrebinnedPartitioning::Min10
    } else if dur <= DtMs::from_ms_u64(1000 * 60 * 60 * 2) {
        PrebinnedPartitioning::Hour1
    } else {
        PrebinnedPartitioning::Day1
    }
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
enum IndexWritten {
    None,
    Last(u32, u32),
}

impl IndexWritten {
    fn new() -> Self {
        IndexWritten::None
    }

    fn should_write(&self, msp: u32, lsp: u32) -> bool {
        if let IndexWritten::Last(lmsp, llsp) = self {
            *lmsp != msp || *llsp != lsp
        } else {
            true
        }
    }

    fn mark_written(&mut self, msp: u32, lsp: u32) {
        *self = IndexWritten::Last(msp, lsp);
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
    binner_1st: Option<(
        RetentionTime,
        BinnedEventsTimeweight<f32>,
        WriteCntZero,
        PrebinnedPartitioning,
        IndexWritten,
        Option<IndexWritten>,
    )>,
    binner_others: Vec<(
        RetentionTime,
        BinnedBinsTimeweight<f32, f32>,
        WriteCntZero,
        PrebinnedPartitioning,
        IndexWritten,
        Option<IndexWritten>,
    )>,
    trd: bool,
}

impl BinWriter {
    pub fn new(
        beg: TsNano,
        min_quiets: MinQuiets,
        is_polled: bool,
        cssid: ChannelStatusSeriesId,
        sid: SeriesId,
        scalar_type: ScalarType,
        shape: Shape,
        chname: String,
    ) -> Result<Self, Error> {
        let trd = true || series::dbg::dbg_chn(&chname);
        if trd {
            debug_bin!(trd, "enabled debug for {}", chname);
        }
        const DUR_ZERO: DtMs = DtMs::from_ms_u64(0);
        const DUR_MAX: DtMs = DtMs::from_ms_u64(1000 * 60 * 60 * 24 * 123);
        let rts = [RetentionTime::Short, RetentionTime::Medium, RetentionTime::Long];
        let quiets = [min_quiets.st.clone(), min_quiets.mt.clone(), min_quiets.lt.clone()];
        let mut binner_1st = None;
        let mut binner_others = Vec::new();
        let mut has_monitor = None;
        let mut combs: Vec<_> = rts
            .into_iter()
            .zip(quiets.into_iter().map(|x| DtMs::from_ms_u64(x.as_millis() as u64)))
            .inspect(|x| {
                if x.1 <= DUR_ZERO {
                    has_monitor = Some(x.0.clone());
                }
            })
            .filter(|x| x.1 > DUR_ZERO && x.1 < DUR_MAX)
            .map(|x| (x.0, bin_len_clamp(x.1), WriteCntZero::Disable))
            .collect();
        let has_monitor = has_monitor;
        debug_init!(trd, "has_monitor {:?}  is_polled {:?}", has_monitor, is_polled);
        debug_init!(trd, "combs A  {:?}", combs);
        if let Some(last) = combs.last_mut() {
            match &last.1 {
                PrebinnedPartitioning::Day1 => {
                    last.0 = RetentionTime::Long;
                    last.2 = WriteCntZero::Enable;
                }
                PrebinnedPartitioning::Hour1 => {
                    last.0 = RetentionTime::Long;
                    combs.push((RetentionTime::Long, PrebinnedPartitioning::Day1, WriteCntZero::Enable));
                }
                _ => {
                    combs.push((RetentionTime::Long, PrebinnedPartitioning::Hour1, WriteCntZero::Disable));
                    combs.push((RetentionTime::Long, PrebinnedPartitioning::Day1, WriteCntZero::Enable));
                }
            }
        } else {
            match &has_monitor {
                Some(RetentionTime::Short) => {
                    combs.push((RetentionTime::Short, PrebinnedPartitioning::Min1, WriteCntZero::Disable));
                    combs.push((
                        RetentionTime::Medium,
                        PrebinnedPartitioning::Hour1,
                        WriteCntZero::Disable,
                    ));
                    combs.push((RetentionTime::Long, PrebinnedPartitioning::Day1, WriteCntZero::Enable));
                }
                Some(RetentionTime::Medium) => {
                    combs.push((RetentionTime::Short, PrebinnedPartitioning::Min1, WriteCntZero::Disable));
                    combs.push((
                        RetentionTime::Medium,
                        PrebinnedPartitioning::Hour1,
                        WriteCntZero::Disable,
                    ));
                    combs.push((RetentionTime::Long, PrebinnedPartitioning::Day1, WriteCntZero::Enable));
                }
                Some(RetentionTime::Long) => {
                    combs.push((
                        RetentionTime::Medium,
                        PrebinnedPartitioning::Min1,
                        WriteCntZero::Disable,
                    ));
                    combs.push((RetentionTime::Long, PrebinnedPartitioning::Hour1, WriteCntZero::Disable));
                    combs.push((RetentionTime::Long, PrebinnedPartitioning::Day1, WriteCntZero::Enable));
                }
                None => {
                    combs.push((RetentionTime::Long, PrebinnedPartitioning::Day1, WriteCntZero::Enable));
                }
            }
        }
        if chname.contains("TEST:MIN10:") {
            for c in &mut combs {
                if let PrebinnedPartitioning::Min1 = c.1 {
                    c.2 = WriteCntZero::Enable;
                }
            }
        }
        debug_init!(trd, "combs B  {:?}", combs);
        if combs.len() > 1 && has_monitor.is_none() && is_polled {
            combs.remove(0);
        }
        let combs = combs;
        debug_init!(trd, "combs C  {:?}", combs);
        debug_bin!(trd, "{:?} binning combs {:?}", chname, combs);
        for (rt, pbp, write_zero) in combs {
            if binner_1st.is_none() {
                let range = BinnedRange::from_beg_to_inf(beg, pbp.bin_len());
                let mut binner = BinnedEventsTimeweight::new(range);
                if let WriteCntZero::Enable = write_zero {
                    binner.cnt_zero_enable();
                }
                let iw2 = if pbp.uses_index_min10() {
                    Some(IndexWritten::new())
                } else {
                    None
                };
                binner_1st = Some((rt, binner, write_zero, pbp, IndexWritten::new(), iw2));
            } else {
                let range = BinnedRange::from_beg_to_inf(beg, pbp.bin_len());
                let mut binner = BinnedBinsTimeweight::new(range);
                if let WriteCntZero::Enable = write_zero {
                    binner.cnt_zero_enable();
                }
                let iw2 = if pbp.uses_index_min10() {
                    Some(IndexWritten::new())
                } else {
                    None
                };
                binner_others.push((rt, binner, write_zero, pbp, IndexWritten::new(), iw2));
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
            if true {
                self.tick_ingest_loop(iqdqs)?;
            } else {
                self.evbuf.clear();
            }
        } else {
            trace_tick_verbose!("tick  nothing to ingest");
        }
        Ok(())
    }

    fn tick_ingest_loop(&mut self, iqdqs: &mut InsertDeques) -> Result<(), Error> {
        // loop until all events are ingested.
        // remove zero bins for small bin lengths.
        let mut i = 0;
        while self.evbuf.len() != 0 {
            self.tick_ingest_and_handle(iqdqs)?;
            i += 1;
            if i > 20000 {
                let e = Error::IngestLoopLimit;
                return Err(e);
            }
        }
        Ok(())
    }

    fn tick_ingest_and_handle(&mut self, iqdqs: &mut InsertDeques) -> Result<(), Error> {
        let buf = &self.evbuf;
        if let Some(ee) = self.binner_1st.as_mut() {
            let rt = ee.0.clone();
            let write_zero = ee.2.clone();
            let binner = &mut ee.1;
            let pbp = ee.3.clone();
            let index_written = &mut ee.4;
            let iw2 = &mut ee.5;
            // TODO avoid boxing
            let bufbox = Box::new(buf);
            use items_0::timebin::IngestReport;
            let consumed_evs = match binner.ingest(&bufbox)? {
                IngestReport::ConsumedAll => {
                    let n = bufbox.len();
                    self.evbuf.clear();
                    n
                }
                items_0::timebin::IngestReport::ConsumedPart(n) => {
                    self.evbuf.truncate_front(self.evbuf.len() - n);
                    n
                }
            };
            let bins = binner.output();
            if bins.len() > 0 {
                trace_bin!(self.trd, "binner_1st  out len {}", bins.len());
                Self::handle_output_ready(
                    self.trd,
                    true,
                    self.sid,
                    rt,
                    &bins,
                    write_zero,
                    index_written,
                    iw2,
                    pbp,
                    iqdqs,
                )?;
                // TODO avoid boxing
                let mut bins2: BinsBoxed = Box::new(bins);
                for i in 0..self.binner_others.len() {
                    let (rt, binner, write_zero, pbp, index_written, iw2) = &mut self.binner_others[i];
                    let write_zero = write_zero.clone();
                    binner.ingest(&bins2)?;
                    let bb: Option<BinsBoxed> = binner.output()?;
                    match bb {
                        Some(bb) => {
                            if bb.len() > 0 {
                                trace_bin!(self.trd, "binner_others {}  out len {}", i, bb.len());
                                if let Some(bb2) = bb.as_any_ref().downcast_ref::<ContainerBins<f32, f32>>() {
                                    Self::handle_output_ready(
                                        self.trd,
                                        false,
                                        self.sid,
                                        rt.clone(),
                                        &bb2,
                                        write_zero,
                                        index_written,
                                        iw2,
                                        pbp.clone(),
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
                Ok(())
            } else if consumed_evs == 0 {
                let e = Error::BinnerNoProgress;
                return Err(e);
            } else {
                Ok(())
            }
        } else {
            // TODO should rather make the top-level binner non-optional
            self.evbuf.clear();
            Ok(())
        }
    }

    fn handle_output_ready(
        trd: bool,
        is_from_events: bool,
        series: SeriesId,
        rt: RetentionTime,
        bins: &ContainerBins<f32, f32>,
        write_zero: WriteCntZero,
        iw1: &mut IndexWritten,
        iw2: &mut Option<IndexWritten>,
        pbp: PrebinnedPartitioning,
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
                if is_from_events {
                    info!("zero count bin  from events  {:?}", series);
                } else {
                    info!("zero count bin  from bins  {:?}", series);
                }
            } else {
                if bin_len != pbp.bin_len() {
                    let e = Error::UnexpectedBinLen(bin_len, pbp);
                    return Err(e);
                }
                {
                    let (msp, lsp) = pbp.msp_lsp(ts1.to_ts_ms());
                    let item = QueryItem::TimeBinSimpleF32V02(TimeBinSimpleF32V02 {
                        series,
                        binlen: bin_len.ms() as i32,
                        msp: msp as i64,
                        off: lsp as i32,
                        cnt: cnt as i64,
                        min,
                        max,
                        avg,
                        dev: f32::NAN,
                        lst,
                    });
                    if true || bin_len >= DtMs::from_ms_u64(1000 * 60 * 60) {
                        debug_bin!(trd, "handle_output_ready  emit  {:?}  len {}  {:?}", rt, bins_len, item);
                    }
                    let qu = iqdqs.deque(rt.clone());
                    qu.push_back(item);
                }
                if pbp.uses_index_min10() {
                    let pbp_ix = PrebinnedPartitioning::Min10;
                    let (msp, lsp) = pbp_ix.msp_lsp(ts1.to_ts_ms());
                    debug_bin!(
                        trd,
                        "handle_output_ready  index  {:?}  {:?}  {:?}  {:?}  {:?}  {:?}",
                        series,
                        pbp_ix,
                        pbp,
                        rt,
                        msp,
                        lsp
                    );
                    let iw = iw2.as_mut().unwrap();
                    if iw.should_write(msp, lsp) {
                        iw.mark_written(msp, lsp);
                        let item = BinWriteIndexV03 {
                            series: series.id() as i64,
                            pbp: pbp_ix.db_ix() as i16,
                            msp: msp as i32,
                            rt: rt.to_index_db_i32() as i16,
                            lsp: lsp as i32,
                            binlen: pbp.bin_len().ms() as i32,
                        };
                        let item = QueryItem::BinWriteIndexV03(item);
                        iqdqs.deque(rt.clone()).push_back(item);
                    }
                }
                if true {
                    let pbp_ix = PrebinnedPartitioning::Day1;
                    let (msp, lsp) = pbp_ix.msp_lsp(ts1.to_ts_ms());
                    debug_bin!(
                        trd,
                        "handle_output_ready  index  {:?}  {:?}  {:?}  {:?}  {:?}  {:?}",
                        series,
                        pbp_ix,
                        pbp,
                        rt,
                        msp,
                        lsp
                    );
                    // let iw = iw1;
                    if iw1.should_write(msp, lsp) {
                        iw1.mark_written(msp, lsp);
                        let item = BinWriteIndexV03 {
                            series: series.id() as i64,
                            pbp: pbp_ix.db_ix() as i16,
                            msp: msp as i32,
                            rt: rt.to_index_db_i32() as i16,
                            lsp: lsp as i32,
                            binlen: pbp.bin_len().ms() as i32,
                        };
                        let item = QueryItem::BinWriteIndexV03(item);
                        iqdqs.deque(rt.clone()).push_back(item);
                    }
                }
            }
        }
        Ok(())
    }
}
