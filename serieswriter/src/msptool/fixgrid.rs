use netpod::DtMs;
use netpod::DtNano;
use netpod::TsMs;
use netpod::TsNano;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct MspSplitFixGrid {
    grid_dt: DtMs,
    last: Option<TsMs>,
}

impl MspSplitFixGrid {
    pub fn new(grid_dt: DtMs) -> Self {
        Self { grid_dt, last: None }
    }

    pub fn split(&mut self, ts: TsNano, _item_bytes: u32) -> (TsNano, DtNano, bool) {
        let (msp, _) = ts.to_ts_ms().to_grid_02(self.grid_dt);
        let changed = if self.last != Some(msp) {
            self.last = Some(msp);
            true
        } else {
            false
        };
        let msp = msp.ns();
        let lsp = ts.delta(msp);
        (msp, lsp, changed)
    }
}
