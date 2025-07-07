use crate as serieswriter;
use crate::msptool::fixgrid::MspSplitFixGrid;
use crate::writer::EmittableType;
use crate::writer::SeriesWriter;
use netpod::ByteSize;
use netpod::DtMs;
use netpod::TsNano;
use scywr::iteminsertqueue::DataValue;
use scywr::iteminsertqueue::ScalarValue;
use std::time::Instant;

pub const CHANNEL_STATUS_GRID: DtMs = DtMs::from_ms_u64(1000 * 60 * 60);

pub type ChannelStatusSeriesWriter = SeriesWriter<ChannelStatusWriteValue, MspSplitFixGrid>;

#[derive(Debug, Clone)]
pub struct ChannelStatusWriteValue(TsNano, u64);

impl ChannelStatusWriteValue {
    pub fn new(ts: TsNano, val: u64) -> Self {
        Self(ts, val)
    }
}

impl EmittableType for ChannelStatusWriteValue {
    type State = ();

    fn ts(&self) -> TsNano {
        self.0
    }

    fn has_change(&self, _k: &Self) -> bool {
        // for channel status the actual event is information, e.g. periodic ca echo.
        true
    }

    fn byte_size(&self) -> u32 {
        8
    }

    fn into_query_item(
        self,
        ts_net: Instant,
        tsev: TsNano,
        state: &mut <Self as EmittableType>::State,
    ) -> serieswriter::writer::EmitRes {
        let byte_size = self.byte_size();
        let data_item = DataValue::Scalar(ScalarValue::U64(self.1));
        let ret = serieswriter::writer::EmitRes {
            data_item,
            bytes: ByteSize(byte_size),
        };
        ret
    }
}
