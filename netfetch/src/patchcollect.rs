use err::Error;
use items_0::collect_s::Collectable;
use items_0::timebin::TimeBinned;
use items_2::merger::Mergeable;
use netpod::log::*;
use netpod::timeunits::SEC;
use netpod::TsNano;

pub struct PatchCollect {
    patch_len: TsNano,
    bin_len: TsNano,
    coll: Option<Box<dyn TimeBinned>>,
    locked: bool,
}

impl PatchCollect {
    pub fn new(bin_len: TsNano, bin_count: u64) -> Self {
        Self {
            patch_len: TsNano(bin_len.0 * bin_count),
            bin_len,
            coll: None,
            locked: false,
        }
    }

    pub fn ingest(&mut self, item: &mut dyn TimeBinned) -> Result<(), Error> {
        let coll = self.coll.get_or_insert_with(|| item.empty_like_self_box_time_binned());
        let mut n1 = 0;
        loop {
            n1 += 1;
            if n1 > 20 {
                return Err(Error::with_msg_no_trace("patchcollect too many iterations"));
            }
            info!("ingest loop  item len {}", item.len());
            let (ts1s, ts2s) = item.edges_slice();
            let mut discard = false;
            let mut emit = false;
            let mut i1 = 0;
            let mut i3 = item.len();
            let mut brk = false;
            for (i2, (ts1, ts2)) in ts1s.iter().zip(ts2s).enumerate() {
                info!("EDGE {}", ts1 / SEC);
                if ts2 % self.patch_len.0 == 0 {
                    info!("FOUND PATCH END-EDGE at {}", ts1 / SEC);
                    if self.locked {
                        info!("LOCKED AND FINAL RECEIVED, EMIT");
                        // do: drain into coll i1...i2
                        // do: emit!
                        i3 = i2 + 1;
                        emit = true;
                        brk = true;
                    } else {
                        info!("NOT LOCKED YET");
                        // do: drain into NIRVANA i1...i2  can reset coll.
                        i3 = i2 + 1;
                        discard = true;
                        brk = true;
                    }
                } else if ts1 % self.patch_len.0 == 0 {
                    info!("FOUND PATCH BEG-EDGE at {}", ts1 / SEC);
                    if self.locked {
                        info!("LOCKED");
                        if i2 > 0 {
                            error!("should never happen");
                            // ??? do: drain into coll i1..i2
                        } else {
                            // ??? nothing to do
                        }
                    } else {
                        // ??? do: drain into NIRVANA i1..i2  can reset coll.
                    }
                }
                if !self.locked && ts1 % self.patch_len.0 == 0 {
                    info!("NOT LOCKED YET, BUT NOW");
                    self.locked = true;
                    // do: drain into NIRVANA i1..i2  can reset coll.
                }
                if brk {
                    break;
                }
            }
            if discard {
                item.drain_into_tb(coll.as_mut(), i1..i3)?;
                item.reset();
            } else {
            }
        }
        // TODO need some way to know the bin edges of the bins in the container.
        // From there, derive a range of bins that I want to use.
        // Need some generic way (like Collect) to take out the chosen range of bins
        // into the local container which holds the current patch.
        // From there, for the first iteration, need a function to convert the
        // bin-container into a single standard TimeBins0 or TimeBins1 for everything.

        // PROBLEM need to handle BinsDim0 and BinsXbinDim0.
        // Need to use some of those because the retrieval also needs Rust-types to work with them.
        Ok(())
    }
}
