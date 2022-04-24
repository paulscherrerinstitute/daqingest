use std::time::{Duration, Instant};

pub struct EMA {
    ema: f32,
    emv: f32,
    k: f32,
    update_count: u64,
}

impl EMA {
    pub fn with_k(k: f32) -> Self {
        Self {
            ema: 0.0,
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
