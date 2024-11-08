use err::thiserror;
use err::ThisError;
use items_2::binning::container_bins::ContainerBins;
use items_2::binning::container_events::ContainerEvents;
use items_2::binning::timeweight::timeweight_events::BinnedEventsTimeweight;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::ttl::RetentionTime;
use netpod::BinnedRange;
use netpod::DtMs;
use netpod::ScalarType;
use netpod::Shape;
use netpod::TsMs;
use netpod::TsNano;
use scywr::insertqueues::InsertDeques;
use scywr::iteminsertqueue::QueryItem;
use scywr::iteminsertqueue::TimeBinSimpleF32V01;
use series::ChannelStatusSeriesId;
use series::SeriesId;

macro_rules! trace_ingest { ($($arg:tt)*) => ( if false { trace!($($arg)*); } ) }
macro_rules! trace_tick { ($($arg:tt)*) => ( if false { trace!($($arg)*); } ) }
macro_rules! trace_tick_verbose { ($($arg:tt)*) => ( if false { trace!($($arg)*); } ) }

#[derive(Debug, ThisError)]
#[cstm(name = "SerieswriterBinwriterGrid")]
pub enum Error {
    SeriesLookupError,
    SeriesWriter(#[from] crate::writer::Error),
    Binning(#[from] items_2::binning::timeweight::timeweight_events::Error),
    UnsupportedBinGrid(DtMs),
}

#[derive(Debug)]
pub struct BinWriterGrid {
    rt: RetentionTime,
    cssid: ChannelStatusSeriesId,
    sid: SeriesId,
    scalar_type: ScalarType,
    shape: Shape,
    binner: BinnedEventsTimeweight<f32>,
}

impl BinWriterGrid {
    pub fn new(
        beg: TsNano,
        rt: RetentionTime,
        bin_len: DtMs,
        cssid: ChannelStatusSeriesId,
        sid: SeriesId,
        scalar_type: ScalarType,
        shape: Shape,
    ) -> Result<Self, Error> {
        let margin = 1000 * 1000 * 1000 * 60 * 60 * 24 * 40;
        let end = (u64::MAX - margin) / bin_len.ns() * bin_len.ns();
        let range = BinnedRange::from_nano_range(NanoRange::from_ns_u64(beg.ns(), end), bin_len);
        let binner = BinnedEventsTimeweight::new(range).disable_cnt_zero();
        let ret = Self {
            rt,
            cssid,
            sid,
            scalar_type,
            shape,
            binner,
        };
        let _ = &ret.rt;
        let _ = &ret.cssid;
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

    pub fn ingest(&mut self, evs: ContainerEvents<f32>, iqdqs: &mut InsertDeques) -> Result<(), Error> {
        let _ = iqdqs;
        trace_ingest!("{:?}  {:?}", self, evs);
        self.binner.ingest(evs)?;
        Ok(())
    }

    fn handle_output_ready(&mut self, out: ContainerBins<f32>, iqdqs: &mut InsertDeques) -> Result<(), Error> {
        let selfname = "handle_output_ready";
        trace_tick!("{selfname}  bins ready len {}", out.len());
        for e in out.iter_debug() {
            trace_tick_verbose!("{e:?}");
        }
        for ((((((&ts1, &ts2), &cnt), &min), &max), &avg), &fnl) in out.zip_iter() {
            if fnl == false {
                info!("non final bin");
            } else if cnt == 0 {
                info!("zero count bin");
            } else {
                let bin_len = DtMs::from_ms_u64(ts2.delta(ts1).ms_u64());
                let div = if bin_len == DtMs::from_ms_u64(1000 * 1) {
                    DtMs::from_ms_u64(1000 * 60 * 10)
                } else if bin_len == DtMs::from_ms_u64(1000 * 10) {
                    DtMs::from_ms_u64(1000 * 60 * 60 * 2)
                } else if bin_len == DtMs::from_ms_u64(1000 * 60 * 1) {
                    DtMs::from_ms_u64(1000 * 60 * 60 * 8)
                } else if bin_len == DtMs::from_ms_u64(1000 * 60 * 10) {
                    DtMs::from_ms_u64(1000 * 60 * 60 * 24 * 4)
                } else if bin_len == DtMs::from_ms_u64(1000 * 60 * 60 * 1) {
                    DtMs::from_ms_u64(1000 * 60 * 60 * 24 * 28)
                } else {
                    // TODO
                    return Err(Error::UnsupportedBinGrid(bin_len));
                };
                let ts_msp = TsMs::from_ms_u64(ts1.ms() / div.ms() * div.ms());
                let off = (ts1.ms() - ts_msp.ms()) / bin_len.ms();
                let item = QueryItem::TimeBinSimpleF32V01(TimeBinSimpleF32V01 {
                    series: self.sid.clone(),
                    bin_len_ms: bin_len.ms() as i32,
                    ts_msp,
                    off: off as i32,
                    count: cnt as i64,
                    min,
                    max,
                    avg,
                });
                match &self.rt {
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

    pub fn tick(&mut self, iqdqs: &mut InsertDeques) -> Result<(), Error> {
        let out = self.binner.output();
        if out.len() != 0 {
            self.handle_output_ready(out, iqdqs)?;
        } else {
            trace_tick_verbose!("tick  NO BINS YET");
        }
        Ok(())
    }
}
