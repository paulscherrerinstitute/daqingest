use std::time::{Duration, Instant};

pub struct EMA {
    ema: f32,
    emv: f32,
    k: f32,
}

impl EMA {
    pub fn default() -> Self {
        Self {
            ema: 0.0,
            emv: 0.0,
            k: 0.05,
        }
    }

    pub fn update<V>(&mut self, v: V)
    where
        V: Into<f32>,
    {
        let k = self.k;
        let dv = v.into() - self.ema;
        self.ema += k * dv;
        self.emv = (1f32 - k) * (self.emv + k * dv * dv);
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
