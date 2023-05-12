use err::Error;
use items_0::timebin::TimeBinned;
use netpod::log::*;
use netpod::timeunits::SEC;
use netpod::TsNano;
use std::collections::VecDeque;
use std::mem;

pub struct PatchCollect {
    patch_len: TsNano,
    bin_len: TsNano,
    bin_count: u64,
    coll: Option<Box<dyn TimeBinned>>,
    locked: bool,
    outq: VecDeque<Box<dyn TimeBinned>>,
}

impl PatchCollect {
    pub fn new(bin_len: TsNano, bin_count: u64) -> Self {
        Self {
            patch_len: TsNano(bin_len.0 * bin_count),
            bin_len,
            bin_count,
            coll: None,
            locked: false,
            outq: VecDeque::new(),
        }
    }

    pub fn patch_len(&self) -> TsNano {
        self.patch_len.clone()
    }

    pub fn bin_len(&self) -> TsNano {
        self.bin_len.clone()
    }

    pub fn bin_count(&self) -> u64 {
        self.bin_count
    }

    pub fn ingest(&mut self, item: &mut dyn TimeBinned) -> Result<(), Error> {
        let mut n1 = 0;
        let mut item_len_exp = item.len();
        loop {
            n1 += 1;
            if n1 > 20 {
                return Err(Error::with_msg_no_trace("patchcollect too many iterations"));
            }
            info!("ingest loop  item len {}", item.len());
            if item.len() != item_len_exp {
                return Err(Error::with_msg_no_trace(format!(
                    "patchcollect item_len_exp mismatch  {} vs {}",
                    item.len(),
                    item_len_exp
                )));
            }
            if item.len() == 0 {
                break;
            }
            let coll = self.coll.get_or_insert_with(|| item.empty_like_self_box_time_binned());
            let (ts1s, ts2s) = item.edges_slice();
            let mut discard = false;
            let mut emit = false;
            let i1 = 0;
            let mut i3 = item.len();
            for (i2, (ts1, ts2)) in ts1s.iter().zip(ts2s).enumerate() {
                info!("EDGE {}", ts1 / SEC);
                if self.locked {
                    if ts2 % self.patch_len.0 == 0 {
                        info!("FOUND PATCH EDGE-END at {}", ts2 / SEC);
                        i3 = i2 + 1;
                        emit = true;
                    }
                } else {
                    if ts1 % self.patch_len.0 == 0 {
                        info!("FOUND PATCH EDGE-BEG at {}", ts1 / SEC);
                        self.locked = true;
                        i3 = i2;
                        discard = true;
                        break;
                    }
                }
            }
            if !self.locked {
                info!("drain all");
                item_len_exp = 0;
                item.reset();
            } else if discard {
                let range = i1..i3;
                info!("discard  range-len {}", range.len());
                item_len_exp -= range.len();
                item.drain_into_tb(coll.as_mut(), range)?;
                coll.reset();
            } else if emit {
                let range = i1..i3;
                info!("take and emit  range-len {}", range.len());
                item_len_exp -= range.len();
                item.drain_into_tb(coll.as_mut(), range)?;
                if coll.len() != self.bin_count as usize {
                    error!("PatchCollect bin count mismatch  {} vs {}", coll.len(), self.bin_count);
                }
                //info!("Patch EMIT  {coll:?}");
                let k = self.coll.take().unwrap();
                self.outq.push_back(k);
            } else {
                let range = i1..i3;
                info!("take all  range-len {}", range.len());
                item_len_exp = 0;
                item.drain_into_tb(coll.as_mut(), range)?;
            }
        }
        Ok(())
    }

    pub fn outq_len(&self) -> usize {
        self.outq.len()
    }

    pub fn take_outq(&mut self) -> VecDeque<Box<dyn TimeBinned>> {
        mem::replace(&mut self.outq, VecDeque::new())
    }
}
