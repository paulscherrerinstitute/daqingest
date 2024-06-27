use crate::timebin::ConnTimeBin;
use crate::writer::SeriesWriter;
use async_channel::Sender;
use err::thiserror;
use err::ThisError;
use netpod::log::*;
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
pub enum Error {
    SeriesLookupError,
    SeriesWriter(#[from] crate::writer::Error),
    Timebin(#[from] crate::timebin::Error),
}

#[derive(Debug)]
pub struct BinWriter {
    sid: SeriesId,
    scalar_type: ScalarType,
    shape: Shape,
    binner: ConnTimeBin,
}

impl BinWriter {
    pub fn new(
        // channel_info_tx: Sender<ChannelInfoQuery>,
        cssid: ChannelStatusSeriesId,
        sid: SeriesId,
        scalar_type: ScalarType,
        shape: Shape,
        stnow: SystemTime,
    ) -> Result<Self, Error> {
        type A = SeriesWriter;
        let mut binner = ConnTimeBin::empty(sid.clone(), TsNano::from_ms(1000 * 2));
        binner.setup_for(&scalar_type, &shape, stnow)?;
        let ret = Self {
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
