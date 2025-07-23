pub mod mett;

pub use mettrics;
pub use rand_xoshiro;

use std::time::Duration;
use std::time::Instant;

pub type EMA = Ema32;

#[derive(Clone, Debug)]
pub struct Ema32 {
    ema: f32,
    emv: f32,
    k: f32,
    update_count: u64,
}

impl Ema32 {
    pub fn with_k(k: f32) -> Self {
        Self {
            ema: 0.0,
            emv: 0.0,
            k,
            update_count: 0,
        }
    }

    pub fn with_ema(ema: f32) -> Self {
        Self {
            ema,
            emv: 0.0,
            k: 0.05,
            update_count: 0,
        }
    }

    pub fn default() -> Self {
        Self {
            ema: 0.0,
            emv: 0.0,
            k: 0.05,
            update_count: 0,
        }
    }

    #[inline(always)]
    pub fn update<V>(&mut self, v: V)
    where
        V: Into<f32>,
    {
        self.update_count += 1;
        let k = self.k;
        let dv = v.into() - self.ema;
        self.ema += k * dv;
        self.emv = (1f32 - k) * (self.emv + k * dv * dv);
    }

    pub fn update_count(&self) -> u64 {
        self.update_count
    }

    pub fn ema(&self) -> f32 {
        self.ema
    }

    pub fn emv(&self) -> f32 {
        self.emv
    }

    pub fn k(&self) -> f32 {
        self.k
    }
}

#[derive(Clone, Debug)]
pub struct Ema64 {
    ema: f64,
    emv: f64,
    k: f64,
    update_count: u64,
}

impl Ema64 {
    pub fn with_k(k: f64) -> Self {
        Self {
            ema: 0.0,
            emv: 0.0,
            k,
            update_count: 0,
        }
    }

    pub fn with_ema(ema: f64, k: f64) -> Self {
        Self {
            ema,
            emv: 0.0,
            k,
            update_count: 0,
        }
    }

    pub fn default() -> Self {
        Self {
            ema: 0.0,
            emv: 0.0,
            k: 0.05,
            update_count: 0,
        }
    }

    #[inline(always)]
    pub fn update<V>(&mut self, v: V)
    where
        V: Into<f64>,
    {
        self.update_count += 1;
        let k = self.k;
        let dv = v.into() - self.ema;
        self.ema += k * dv;
        self.emv = (1f64 - k) * (self.emv + k * dv * dv);
    }

    pub fn update_count(&self) -> u64 {
        self.update_count
    }

    pub fn ema(&self) -> f64 {
        self.ema
    }

    pub fn emv(&self) -> f64 {
        self.emv
    }

    pub fn k(&self) -> f64 {
        self.k
    }
}

pub struct CheckEvery {
    ts_last: Instant,
    dt: Duration,
}

impl CheckEvery {
    pub fn new(dt: Duration) -> Self {
        Self {
            ts_last: Instant::now(),
            dt,
        }
    }

    pub fn is_elapsed_now(&mut self) -> f32 {
        let now = Instant::now();
        let dt = now.duration_since(self.ts_last);
        if dt >= self.dt {
            self.ts_last = now;
            dt.as_secs_f32()
        } else {
            -16f32
        }
    }
}

#[derive(Clone, Debug)]
pub struct IntervalEma {
    tslast: Option<Instant>,
    ema: EMA,
}

impl IntervalEma {
    pub fn new() -> Self {
        Self {
            tslast: None,
            ema: EMA::default(),
        }
    }

    pub fn tick(&mut self, tsnow: Instant) {
        match self.tslast {
            Some(tslast) => {
                let dt = tsnow.duration_since(tslast);
                self.tslast = Some(tsnow);
                self.ema.update(dt.as_secs_f32());
            }
            None => {
                self.tslast = Some(tsnow);
            }
        }
    }

    pub fn ema_preview(&self, tsnow: Instant) -> Option<f32> {
        match self.tslast {
            Some(tslast) => {
                let dt = tsnow.duration_since(tslast);
                let v = dt.as_secs_f32();
                let dv = v - self.ema.ema;
                Some(self.ema.ema + self.ema.k * dv)
            }
            None => None,
        }
    }

    pub fn ema(&self) -> &EMA {
        &self.ema
    }
}

pub struct CounterU64 {
    sum: u64,
}

impl CounterU64 {
    pub fn new() -> Self {
        Self { sum: 0 }
    }

    pub fn inc(&mut self) {
        self.sum += 1;
    }

    pub fn add(&mut self, x: u64) {
        self.sum += x;
    }
}

pub struct DoubleBuffer<T> {
    back: T,
    front: T,
}

impl<T> DoubleBuffer<T> {
    pub fn new() -> Self
    where
        T: Default,
    {
        Self {
            back: T::default(),
            front: T::default(),
        }
    }

    pub fn switch(&mut self)
    where
        T: Default,
    {
        self.back = ::core::mem::replace(&mut self.front, T::default());
    }
}

pub struct XorShift32 {
    state: u32,
}

impl XorShift32 {
    pub fn new(state: u32) -> Self {
        Self { state }
    }

    pub fn new_from_time() -> Self {
        use std::time::SystemTime;
        Self::new(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .subsec_micros(),
        )
    }

    pub fn next(&mut self) -> u32 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        self.state = x;
        x
    }
}

pub fn xoshiro_from_time() -> rand_xoshiro::Xoshiro128PlusPlus {
    use rand_xoshiro::rand_core::SeedableRng;
    use std::time::SystemTime;
    let a = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos() as u64;
    let b = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos() as u64;
    rand_xoshiro::Xoshiro128PlusPlus::seed_from_u64(a << 32 ^ b)
}
