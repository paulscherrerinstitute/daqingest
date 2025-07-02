pub mod fixgrid;

use netpod::DtNano;
use netpod::TsNano;
use netpod::timeunits::DAY;
use netpod::timeunits::SEC;
use serde::Serialize;

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

    pub fn split(&mut self, ts: TsNano, item_bytes: u32) -> (TsNano, DtNano, bool, Option<TsNano>) {
        // Maximum resolution of the ts msp:
        let msp_res_max = SEC * 2;
        let ts_inp = ts;
        let (ts_msp, changed, ts_msp_retired) = match self.last {
            Some(ts_msp_last) => {
                if self.count >= self.count_max || self.bytes >= self.bytes_max || ts_msp_last.add_ns(DAY) <= ts_inp {
                    let ts_msp = ts_inp.div(msp_res_max).mul(msp_res_max);
                    if ts_msp == ts_msp_last {
                        // TODO should count these for metrics
                        (ts_msp, false, None)
                    } else {
                        self.last = Some(ts_msp);
                        self.count = 1;
                        self.bytes = item_bytes;
                        (ts_msp, true, Some(ts_msp_last))
                    }
                } else {
                    self.count += 1;
                    self.bytes += item_bytes;
                    (ts_msp_last, false, None)
                }
            }
            None => {
                let ts_msp = ts_inp.div(msp_res_max).mul(msp_res_max);
                self.last = Some(ts_msp);
                self.count = 1;
                self.bytes = item_bytes;
                (ts_msp, true, None)
            }
        };
        let ts_lsp = ts_inp.delta(ts_msp);
        (ts_msp, ts_lsp, changed, ts_msp_retired)
    }

    pub fn ts_msp_current(&self) -> Option<TsNano> {
        self.last
    }
}
