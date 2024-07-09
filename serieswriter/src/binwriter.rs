use crate::timebin::ConnTimeBin;
use crate::writer::SeriesWriter;
use async_channel::Sender;
use err::thiserror;
use err::ThisError;
use netpod::log::*;
use netpod::ttl::RetentionTime;
use netpod::DtNano;
use netpod::ScalarType;
use netpod::Shape;
use netpod::TsNano;
use scywr::insertqueues::InsertDeques;
use scywr::iteminsertqueue::DataValue;
use scywr::iteminsertqueue::QueryItem;
use series::ChannelStatusSeriesId;
use series::SeriesId;
use std::collections::VecDeque;
use std::time::Duration;
use std::time::SystemTime;

#[allow(unused)]
macro_rules! trace_binning {
    ($($arg:tt)*) => {
        if false {
            trace!($($arg)*);
        }
    };
}

#[derive(Debug, ThisError)]
#[cstm(name = "SerieswriterBinwriter")]
pub enum Error {
    SeriesLookupError,
    SeriesWriter(#[from] crate::writer::Error),
    Timebin(#[from] crate::timebin::Error),
}

#[derive(Debug)]
pub struct BinWriter {
    rt: RetentionTime,
    sid: SeriesId,
    scalar_type: ScalarType,
    shape: Shape,
    binner: ConnTimeBin,
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
        let bin_len = DtNano::from_ms(1000 * 10);
        let binner = ConnTimeBin::new(
            rt.clone(),
            sid.clone(),
            beg,
            bin_len,
            scalar_type.clone(),
            shape.clone(),
        )?;
        let ret = Self {
            rt,
            sid,
            scalar_type,
            shape,
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

    pub fn ingest(
        &mut self,
        ts_ioc: TsNano,
        ts_local: TsNano,
        val: &DataValue,
        iqdqs: &mut InsertDeques,
    ) -> Result<(), Error> {
        let ts_main = ts_local;
        self.binner.push(ts_main.clone(), val)?;
        Ok(())
    }

    pub fn tick(&mut self, iqdqs: &mut InsertDeques) -> Result<(), Error> {
        self.binner.tick(iqdqs)?;
        Ok(())
    }
}
