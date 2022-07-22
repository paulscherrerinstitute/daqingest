use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

const US: u64 = 1000;
const MS: u64 = US * 1000;
const SEC: u64 = MS * 1000;

#[derive(Clone, Debug)]
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

stats_proc::stats_struct!((
    stats_struct(
        name(CaConnStats),
        counters(
            insert_item_create,
            inserts_val,
            inserts_msp,
            inserts_msp_grid,
            inserts_queue_pop_for_global,
            inserts_queue_push,
            inserts_queue_drop,
            channel_fast_item_drop,
            store_worker_item_recv,
            store_worker_item_insert,
            store_worker_item_drop,
            store_worker_item_error,
            poll_time_all,
            poll_time_handle_insert_futs,
            poll_time_get_series_futs,
            time_handle_conn_listen,
            time_handle_peer_ready,
            time_check_channels_state_init,
            time_handle_event_add_res,
            ioc_lookup,
            tcp_connected,
            get_series_id_ok,
            conn_item_count,
            conn_stream_ready,
            conn_stream_pending,
            ca_ts_off_1,
            ca_ts_off_2,
            ca_ts_off_3,
            ca_ts_off_4,
        ),
    ),
    agg(name(CaConnStatsAgg), parent(CaConnStats)),
    diff(name(CaConnStatsAggDiff), input(CaConnStatsAgg)),
));
