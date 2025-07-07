pub mod dyngrid;
pub mod fixgrid;

use netpod::DtNano;
use netpod::TsNano;
use netpod::ttl::RetentionTime;

pub trait MspSplit {
    // fn new() -> Self;
    fn split(&mut self, ts: TsNano, item_bytes: u32) -> (TsNano, DtNano, bool, Option<TsNano>);
    fn ts_msp_current(&self) -> Option<TsNano>;
    fn rt(&self) -> RetentionTime;
}
