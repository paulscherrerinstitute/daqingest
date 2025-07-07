use crate::msptool::MspSplit;
use netpod::DtNano;
use netpod::TsNano;
use netpod::timeunits::SEC;
use netpod::ttl::RetentionTime;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct MspSplitDyn {
    last: Option<TsNano>,
    count: u32,
    bytes: u32,
    count_max: u32,
    bytes_max: u32,
    rt: RetentionTime,
    rollover_at: TsNano,
}

impl MspSplitDyn {
    pub fn new(count_max: u32, bytes_max: u32, rt: RetentionTime) -> Self {
        Self {
            last: None,
            count: 0,
            bytes: 0,
            count_max,
            bytes_max,
            rt,
            rollover_at: TsNano::from_ns(0),
        }
    }
}

impl MspSplit for MspSplitDyn {
    fn split(&mut self, ts: TsNano, item_bytes: u32) -> (TsNano, DtNano, bool, Option<TsNano>) {
        // Maximum resolution of the ts msp:
        let msp_res_max = SEC * 2;
        let ts_inp = ts;
        let (ts_msp, changed, ts_msp_retired) = match self.last {
            Some(ts_msp_last) => {
                if self.count >= self.count_max || self.bytes >= self.bytes_max || self.rollover_at <= ts_inp {
                    let ts_msp = ts_inp.div(msp_res_max).mul(msp_res_max);
                    if ts_msp == ts_msp_last {
                        // TODO should count these for metrics
                        (ts_msp, false, None)
                    } else {
                        self.last = Some(ts_msp);
                        self.count = 1;
                        self.bytes = item_bytes;
                        self.rollover_at = TsNano::from_ns(ts_inp.ns() + SEC * self.rt.ttl_ts_msp().as_secs() / 20);
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
                self.rollover_at = TsNano::from_ns(ts_inp.ns() + SEC * self.rt.ttl_ts_msp().as_secs() / 20);
                (ts_msp, true, None)
            }
        };
        let ts_lsp = ts_inp.delta(ts_msp);
        (ts_msp, ts_lsp, changed, ts_msp_retired)
    }

    fn ts_msp_current(&self) -> Option<TsNano> {
        self.last
    }

    fn rt(&self) -> RetentionTime {
        self.rt.clone()
    }
}
