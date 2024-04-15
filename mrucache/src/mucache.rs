use hashbrown::HashMap;
use log::*;
use std::hash::Hash;
use std::sync::OnceLock;
use std::time::Instant;

// use std::sync::atomic::AtomicU64;
// use std::sync::atomic::Ordering;
// use std::sync::RwLock;

fn tsref() -> Instant {
    static C: OnceLock<Instant> = OnceLock::new();
    let c = C.get_or_init(Instant::now);
    c.clone()
}

fn dts_now() -> u64 {
    let tsref = tsref();
    let ts = Instant::now();
    let dt = ts.saturating_duration_since(tsref);
    let ts = 1000 * dt.as_secs() + dt.subsec_millis() as u64;
    ts
}

pub struct MuCache<K, V> {
    cap: usize,
    map: HashMap<K, (u64, V)>,
}

impl<K: Eq + Hash, V> MuCache<K, V> {
    pub fn new(cap: usize) -> Self {
        Self {
            cap,
            map: HashMap::with_capacity(cap),
        }
    }

    pub fn insert(&mut self, k: K, v: V) -> Result<(), ()> {
        let ts = dts_now();
        // let ts = AtomicU64::new(ts);
        // let mut map = self.map.write().unwrap();
        let map = &mut self.map;
        let nmax = self.cap * 5 / 4;
        if map.len() >= nmax {
            Self::remove_unused(map, self.cap);
        }
        if map.len() >= nmax {
            warn!("no space in MuCache");
            Err(())
        } else {
            map.insert(k, (ts, v));
            Ok(())
        }
    }

    pub fn get(&mut self, k: &K) -> Option<&V> {
        // let map = self.map.read().unwrap();
        let map = &mut self.map;
        match map.get_mut(k) {
            Some((lu, v)) => {
                let ts = dts_now();
                // lu.store(ts, Ordering::Release);
                *lu = ts;
                Some(v)
            }
            None => None,
        }
    }

    fn remove_unused(map: &mut HashMap<K, (u64, V)>, cap: usize) {
        let map1 = core::mem::replace(map, HashMap::new());
        let mut items: Vec<_> = map1.into_iter().map(|x| (x.1 .0, x.1 .1, x.0)).collect();
        items.sort_unstable_by_key(|x| x.0);
        let ts_cut = items[items.len() - cap].0;
        let map2 = items
            .into_iter()
            .filter(|x| x.0 > ts_cut)
            .map(|x| (x.2, (x.0, x.1)))
            .collect();
        *map = map2;
    }

    pub fn all_ref_mut(&mut self) -> Vec<&mut V> {
        self.map.iter_mut().map(|x| (&mut x.1 .1)).collect()
    }
}
