use crate::binwritergrid::BinWriterGrid;
use crate::log::*;
use crate::rtwriter::MinQuiets;
use items_2::binning::container_events::ContainerEvents;
use netpod::ttl::RetentionTime;
use netpod::DtMs;
use netpod::ScalarType;
use netpod::Shape;
use netpod::TsNano;
use scywr::insertqueues::InsertDeques;
use series::ChannelStatusSeriesId;
use series::SeriesId;
use std::mem;
use std::time::Duration;

macro_rules! trace_ingest { ($($arg:tt)*) => ( if false { trace!($($arg)*); } ) }
macro_rules! trace_tick { ($($arg:tt)*) => ( if false { trace!($($arg)*); } ) }
macro_rules! trace_tick_verbose { ($($arg:tt)*) => ( if false { trace!($($arg)*); } ) }

autoerr::create_error_v1!(
    name(Error, "SerieswriterBinwriter"),
    enum variants {
        SeriesLookupError,
        SeriesWriter(#[from] crate::writer::Error),
        Binning(#[from] items_2::binning::timeweight::timeweight_events::Error),
        UnsupportedBinGrid(DtMs),
        BinWriterGrid(#[from] crate::binwritergrid::Error),
    },
);

#[derive(Debug)]
pub struct BinWriter {
    cssid: ChannelStatusSeriesId,
    sid: SeriesId,
    scalar_type: ScalarType,
    shape: Shape,
    evbuf: ContainerEvents<f32>,
    writers: Vec<BinWriterGrid>,
}

impl BinWriter {
    pub fn new(
        beg: TsNano,
        min_quiets: MinQuiets,
        cssid: ChannelStatusSeriesId,
        sid: SeriesId,
        scalar_type: ScalarType,
        shape: Shape,
    ) -> Result<Self, Error> {
        let mut writers = Vec::new();
        for (rt, dur) in [RetentionTime::Short, RetentionTime::Medium, RetentionTime::Long]
            .into_iter()
            .zip([min_quiets.st.clone(), min_quiets.mt.clone(), min_quiets.lt.clone()].into_iter())
        {
            if dur > Duration::ZERO && dur < Duration::from_millis(1000 * 60 * 60 * 24) {
                let bin_len = if dur < Duration::from_millis(1000 * 2) {
                    DtMs::from_ms_u64(1000 * 1)
                } else if dur < Duration::from_millis(1000 * 20) {
                    DtMs::from_ms_u64(1000 * 10)
                } else if dur < Duration::from_millis(1000 * 60 * 2) {
                    DtMs::from_ms_u64(1000 * 60 * 1)
                } else if dur < Duration::from_millis(1000 * 60 * 20) {
                    DtMs::from_ms_u64(1000 * 60 * 10)
                } else {
                    DtMs::from_ms_u64(1000 * 60 * 60 * 1)
                };
                let writer = BinWriterGrid::new(beg, rt, bin_len, cssid, sid, scalar_type.clone(), shape.clone())?;
                writers.push(writer);
            }
        }
        let ret = Self {
            cssid,
            sid,
            scalar_type,
            shape,
            evbuf: ContainerEvents::new(),
            writers,
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
            let buf = mem::replace(&mut self.evbuf, ContainerEvents::new());
            // TODO bin the more fine grid from the coarse grid, do not clone events
            for writer in self.writers.iter_mut() {
                writer.ingest(&buf, iqdqs)?;
            }
        } else {
            trace_tick_verbose!("tick  NOTHING TO INGEST");
        }
        for writer in self.writers.iter_mut() {
            writer.tick(iqdqs)?;
        }
        Ok(())
    }
}
