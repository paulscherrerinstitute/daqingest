use crate::fixgridwriter::CHANNEL_STATUS_GRID;
use crate::msptool::MspSplit;
use netpod::DtMs;
use netpod::DtNano;
use netpod::TsNano;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct MspSplitFixGrid {
    grid_dt: DtMs,
    last: Option<TsNano>,
}

impl MspSplitFixGrid {
    pub fn new(grid_dt: DtMs) -> Self {
        Self { grid_dt, last: None }
    }

    pub fn for_channel_status() -> Self {
        Self::new(CHANNEL_STATUS_GRID)
    }
}

impl MspSplit for MspSplitFixGrid {
    fn split(&mut self, ts: TsNano, _item_bytes: u32) -> (TsNano, DtNano, bool, Option<TsNano>) {
        let (msp, _) = ts.to_ts_ms().to_grid_02(self.grid_dt);
        let (changed, ts_msp_retired) = if let Some(ts_msp_last) = self.last {
            if msp.ns() != ts_msp_last {
                self.last = Some(msp.ns());
                (true, Some(ts_msp_last))
            } else {
                (false, None)
            }
        } else {
            (true, None)
        };
        let msp = msp.ns();
        let lsp = ts.delta(msp);
        (msp, lsp, changed, ts_msp_retired)
    }

    fn ts_msp_current(&self) -> Option<TsNano> {
        todo!()
    }

    fn rt(&self) -> netpod::ttl::RetentionTime {
        todo!()
    }
}
