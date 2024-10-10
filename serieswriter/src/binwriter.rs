use err::thiserror;
use err::ThisError;
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
use std::mem;

#[allow(unused)]
macro_rules! trace_ingest { ($($arg:tt)*) => ( if false { trace!($($arg)*); } ) }

#[allow(unused)]
macro_rules! trace_tick { ($($arg:tt)*) => ( if false { trace!($($arg)*); } ) }

#[allow(unused)]
macro_rules! trace_tick_verbose { ($($arg:tt)*) => ( if false { trace!($($arg)*); } ) }

#[derive(Debug, ThisError)]
#[cstm(name = "SerieswriterBinwriter")]
pub enum Error {
    SeriesLookupError,
    SeriesWriter(#[from] crate::writer::Error),
    Timebin(#[from] crate::timebin::Error),
    Binning(#[from] items_2::binning::timeweight::timeweight_events::Error),
    UnsupportedBinGrid(DtMs),
}

#[derive(Debug)]
pub struct BinWriter {
    rt: RetentionTime,
    cssid: ChannelStatusSeriesId,
    sid: SeriesId,
    scalar_type: ScalarType,
    shape: Shape,
    evbuf: ContainerEvents<f32>,
    binner: BinnedEventsTimeweight<f32>,
}

impl BinWriter {
    pub fn new(
        beg: TsNano,
        rt: RetentionTime,
        // channel_info_tx: Sender<ChannelInfoQuery>,
        cssid: ChannelStatusSeriesId,
        sid: SeriesId,
        scalar_type: ScalarType,
        shape: Shape,
    ) -> Result<Self, Error> {
        // TODO select the desired bin width based on channel configuration:
        // that's user knowledge, it really depends on what users want.
        // For the moment, assume a fixed value.
        let margin = 1000 * 1000 * 1000 * 60 * 60 * 24 * 40;
        let end = u64::MAX - margin;
        let range = BinnedRange::from_nano_range(NanoRange::from_ns_u64(beg.ns(), end), DtMs::from_ms_u64(1000 * 10));
        let binner = BinnedEventsTimeweight::new(range).disable_cnt_zero();
        let ret = Self {
            rt,
            cssid,
            sid,
            scalar_type,
            shape,
            evbuf: ContainerEvents::new(),
            binner,
        };
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
            let buf = mem::replace(&mut self.evbuf, ContainerEvents::new());
            self.binner.ingest(buf)?;
        } else {
            trace_tick_verbose!("tick  NOTHING TO INGEST");
        }
        let out = self.binner.output();
        if out.len() != 0 {
            trace_tick!("bins ready len {}", out.len());
            for e in out.iter_debug() {
                trace_tick_verbose!("{e:?}");
            }
            for ((((((&ts1, &ts2), &cnt), &min), &max), &avg), &fnl) in out.zip_iter() {
                if fnl == false {
                    debug!("non final bin");
                } else if cnt == 0 {
                } else {
                    let bin_len = DtMs::from_ms_u64(ts2.delta(ts1).ms_u64());
                    let div = if bin_len == DtMs::from_ms_u64(1000 * 10) {
                        DtMs::from_ms_u64(1000 * 60 * 60 * 2)
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
                    iqdqs.lt_rf3_qu.push_back(item);
                }
            }
        } else {
            trace_tick_verbose!("tick  NO BINS YET");
        }
        Ok(())
    }
}
