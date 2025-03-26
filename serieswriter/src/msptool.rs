pub mod fixgrid;

use netpod::DtNano;
use netpod::TsNano;
use serde::Serialize;

const SEC: u64 = 1000_000_000;
const HOUR: u64 = SEC * 60 * 60 * 24;

#[derive(Debug, Serialize)]
pub struct MspSplit {
    last: Option<TsNano>,
    count: u32,
    bytes: u32,
    count_max: u32,
    bytes_max: u32,
}

impl MspSplit {
    pub fn new(count_max: u32, bytes_max: u32) -> Self {
        Self {
            last: None,
            count: 0,
            bytes: 0,
            count_max,
            bytes_max,
        }
    }

    pub fn split(&mut self, ts: TsNano, item_bytes: u32) -> (TsNano, DtNano, bool) {
        // Maximum resolution of the ts msp:
        let msp_res_max = SEC * 2;
        let ts_main = ts;
        let (ts_msp, changed) = match self.last {
            Some(ts_msp_last) => {
                if self.count >= self.count_max || self.bytes >= self.bytes_max || ts_msp_last.add_ns(HOUR) <= ts_main {
                    let ts_msp = ts_main.div(msp_res_max).mul(msp_res_max);
                    if ts_msp == ts_msp_last {
                        (ts_msp, false)
                    } else {
                        self.last = Some(ts_msp);
                        self.count = 1;
                        self.bytes = item_bytes;
                        (ts_msp, true)
                    }
                } else {
                    self.count += 1;
                    self.bytes += item_bytes;
                    (ts_msp_last, false)
                }
            }
            None => {
                let ts_msp = ts_main.div(msp_res_max).mul(msp_res_max);
                self.last = Some(ts_msp);
                self.count = 1;
                self.bytes = item_bytes;
                (ts_msp, true)
            }
        };
        let ts_lsp = ts_main.delta(ts_msp);
        (ts_msp, ts_lsp, changed)
    }
}
