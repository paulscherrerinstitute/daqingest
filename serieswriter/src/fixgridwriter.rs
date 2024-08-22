use crate as serieswriter;
use crate::msptool::fixgrid::MspSplitFixGrid;
use crate::writer::EmittableType;
use crate::writer::SeriesWriter;
use netpod::DtMs;
use netpod::TsNano;
use scywr::iteminsertqueue::DataValue;
use scywr::iteminsertqueue::MspItem;
use scywr::iteminsertqueue::QueryItem;
use scywr::iteminsertqueue::ScalarValue;
use series::SeriesId;
use std::time::Instant;

pub const CHANNEL_STATUS_GRID: DtMs = DtMs::from_ms_u64(1000 * 60 * 60);

pub type ChannelStatusSeriesWriter = SeriesWriter<ChannelStatusWriteValue>;

#[derive(Debug, Clone)]
pub struct ChannelStatusWriteValue(TsNano, u64);

impl ChannelStatusWriteValue {
    pub fn new(ts: TsNano, val: u64) -> Self {
        Self(ts, val)
    }
}

impl EmittableType for ChannelStatusWriteValue {
    type State = ChannelStatusWriteState;

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
        let mut items = serieswriter::writer::SmallVec::new();
        let ts = tsev;
        state.last_accepted_ts = ts;
        state.last_accepted_val = Some(self.1);
        let byte_size = self.byte_size();
        {
            let (ts_msp, ts_lsp, ts_msp_chg) = state.msp_split.split(ts, self.byte_size());
            if ts_msp_chg {
                items.push(QueryItem::Msp(MspItem::new(
                    state.series.clone(),
                    ts_msp.to_ts_ms(),
                    ts_net,
                )));
            }
            let item = scywr::iteminsertqueue::InsertItem {
                series: state.series.clone(),
                ts_msp: ts_msp.to_ts_ms(),
                ts_lsp,
                ts_net,
                val: DataValue::Scalar(ScalarValue::U64(self.1)),
            };
            items.push(QueryItem::Insert(item));
        }
        let ret = serieswriter::writer::EmitRes {
            items,
            bytes: byte_size,
            status: 0,
        };
        ret
    }
}

#[derive(Debug)]
pub struct ChannelStatusWriteState {
    series: SeriesId,
    msp_split: MspSplitFixGrid,
    last_accepted_ts: TsNano,
    last_accepted_val: Option<u64>,
}

impl ChannelStatusWriteState {
    pub fn new(series: SeriesId, grid_dt: DtMs) -> Self {
        Self {
            series,
            msp_split: MspSplitFixGrid::new(grid_dt),
            last_accepted_ts: TsNano::from_ns(0),
            last_accepted_val: None,
        }
    }
}
