use hashbrown::HashMap;
use log::*;
use std::hash::Hash;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::OnceLock;
use std::sync::RwLock;
use std::time::Instant;

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
    map: RwLock<HashMap<K, (AtomicU64, V)>>,
}

impl<K: Clone + Eq + Hash, V: Clone> MuCache<K, V> {
    pub fn new(cap: usize) -> Self {
        Self {
            cap,
            map: RwLock::new(HashMap::with_capacity(cap)),
        }
    }

    pub fn insert(&self, k: K, v: V) -> Result<(), ()> {
        let ts = AtomicU64::new(dts_now());
        let mut map = self.map.write().unwrap();
        let nmax = self.cap * 5 / 4;
        if map.len() >= nmax {
            Self::remove_unused(&mut map, self.cap);
        }
        if map.len() >= nmax {
            warn!("no space in MuCache");
            Err(())
        } else {
            map.insert(k, (ts, v));
            Ok(())
        }
    }

    pub fn get(&self, k: &K) -> Option<V> {
        let map = self.map.read().unwrap();
        match map.get(k) {
            Some((lu, v)) => {
                lu.store(dts_now(), Ordering::Release);
                Some(v.clone())
            }
            None => None,
        }
    }

    fn remove_unused(map: &mut HashMap<K, (AtomicU64, V)>, cap: usize) {
        let map1 = core::mem::replace(map, HashMap::new());
        let mut items: Vec<_> = map1
            .into_iter()
            .map(|x| (x.1 .0.load(Ordering::Acquire), x.1 .1, x.0))
            .collect();
        items.sort_unstable_by_key(|x| x.0);
        let ts_cut = items[items.len() - cap].0;
        let map2 = items
            .into_iter()
            .filter(|x| x.0 > ts_cut)
            .map(|x| (x.2, (AtomicU64::new(x.0), x.1)))
            .collect();
        *map = map2;
    }
}
