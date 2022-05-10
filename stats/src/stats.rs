use std::fmt;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{AcqRel, Acquire};
use std::sync::RwLock;
use std::time::{Duration, Instant};

const US: u64 = 1000;
const MS: u64 = US * 1000;
const SEC: u64 = MS * 1000;

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

pub struct CaConnStats {
    pub poll_time_all: AtomicU64,
    pub poll_time_handle_insert_futs: AtomicU64,
    pub poll_time_get_series_futs: AtomicU64,
    pub time_handle_conn_listen: AtomicU64,
    pub time_handle_peer_ready: AtomicU64,
    pub time_check_channels_state_init: AtomicU64,
    pub time_handle_event_add_res: AtomicU64,
    pub inserts_started: AtomicU64,
    pub inserts_discarded: AtomicU64,
}

impl CaConnStats {
    pub fn new() -> Self {
        Self {
            poll_time_all: AtomicU64::new(0),
            poll_time_handle_insert_futs: AtomicU64::new(0),
            poll_time_get_series_futs: AtomicU64::new(0),
            time_handle_conn_listen: AtomicU64::new(0),
            time_handle_peer_ready: AtomicU64::new(0),
            time_check_channels_state_init: AtomicU64::new(0),
            time_handle_event_add_res: AtomicU64::new(0),
            inserts_started: AtomicU64::new(0),
            inserts_discarded: AtomicU64::new(0),
        }
    }
}

pub struct CaConnVecStats {
    pub ts_create: RwLock<Instant>,
    pub poll_time_all: AtomicU64,
    pub poll_time_handle_insert_futs: AtomicU64,
    pub poll_time_get_series_futs: AtomicU64,
    pub time_handle_conn_listen: AtomicU64,
    pub time_handle_peer_ready: AtomicU64,
    pub time_check_channels_state_init: AtomicU64,
    pub time_handle_event_add_res: AtomicU64,
    pub inserts_started: AtomicU64,
    pub inserts_discarded: AtomicU64,
}

pub struct CaConnVecStatsDiff {
    pub dt: AtomicU64,
    pub poll_time_all: AtomicU64,
    pub poll_time_handle_insert_futs: AtomicU64,
    pub poll_time_get_series_futs: AtomicU64,
    pub time_handle_conn_listen: AtomicU64,
    pub time_handle_peer_ready: AtomicU64,
    pub time_check_channels_state_init: AtomicU64,
    pub time_handle_event_add_res: AtomicU64,
    pub inserts_started: AtomicU64,
    pub inserts_discarded: AtomicU64,
}

impl CaConnVecStats {
    pub fn new(ts_create: Instant) -> Self {
        Self {
            ts_create: RwLock::new(ts_create),
            poll_time_all: AtomicU64::new(0),
            poll_time_handle_insert_futs: AtomicU64::new(0),
            poll_time_get_series_futs: AtomicU64::new(0),
            time_handle_conn_listen: AtomicU64::new(0),
            time_handle_peer_ready: AtomicU64::new(0),
            time_check_channels_state_init: AtomicU64::new(0),
            time_handle_event_add_res: AtomicU64::new(0),
            inserts_started: AtomicU64::new(0),
            inserts_discarded: AtomicU64::new(0),
        }
    }

    pub fn push(&mut self, k: &CaConnStats) {
        self.poll_time_all.fetch_add(k.poll_time_all.load(Acquire), AcqRel);
        self.poll_time_handle_insert_futs
            .fetch_add(k.poll_time_handle_insert_futs.load(Acquire), AcqRel);
        self.poll_time_get_series_futs
            .fetch_add(k.poll_time_get_series_futs.load(Acquire), AcqRel);
        self.time_handle_conn_listen
            .fetch_add(k.time_handle_conn_listen.load(Acquire), AcqRel);
        self.time_handle_peer_ready
            .fetch_add(k.time_handle_peer_ready.load(Acquire), AcqRel);
        self.time_check_channels_state_init
            .fetch_add(k.time_check_channels_state_init.load(Acquire), AcqRel);
        self.time_handle_event_add_res
            .fetch_add(k.time_handle_event_add_res.load(Acquire), AcqRel);
        self.inserts_started.fetch_add(k.inserts_started.load(Acquire), AcqRel);
        self.inserts_discarded
            .fetch_add(k.inserts_discarded.load(Acquire), AcqRel);
    }

    pub fn diff_against(&self, k: &Self) -> CaConnVecStatsDiff {
        let dur = self
            .ts_create
            .read()
            .unwrap()
            .duration_since(*k.ts_create.read().unwrap());
        CaConnVecStatsDiff {
            dt: AtomicU64::new(dur.as_secs() * SEC + dur.subsec_nanos() as u64),
            poll_time_all: AtomicU64::new(self.poll_time_all.load(Acquire) - k.poll_time_all.load(Acquire)),
            poll_time_handle_insert_futs: AtomicU64::new(
                self.poll_time_handle_insert_futs.load(Acquire) - k.poll_time_handle_insert_futs.load(Acquire),
            ),
            poll_time_get_series_futs: AtomicU64::new(
                self.poll_time_get_series_futs.load(Acquire) - k.poll_time_get_series_futs.load(Acquire),
            ),
            time_handle_conn_listen: AtomicU64::new(
                self.time_handle_conn_listen.load(Acquire) - k.time_handle_conn_listen.load(Acquire),
            ),
            time_handle_peer_ready: AtomicU64::new(
                self.time_handle_peer_ready.load(Acquire) - k.time_handle_peer_ready.load(Acquire),
            ),
            time_check_channels_state_init: AtomicU64::new(
                self.time_check_channels_state_init.load(Acquire) - k.time_check_channels_state_init.load(Acquire),
            ),
            time_handle_event_add_res: AtomicU64::new(
                self.time_handle_event_add_res.load(Acquire) - k.time_handle_event_add_res.load(Acquire),
            ),
            inserts_started: AtomicU64::new(self.inserts_started.load(Acquire) - k.inserts_started.load(Acquire)),
            inserts_discarded: AtomicU64::new(self.inserts_discarded.load(Acquire) - k.inserts_discarded.load(Acquire)),
        }
    }
}

impl fmt::Display for CaConnVecStatsDiff {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let insert_freq = self.inserts_started.load(Acquire) / (self.dt.load(Acquire) / SEC);
        let poll_time = self.poll_time_all.load(Acquire);
        let poll_time_handle_insert_futs = self.poll_time_handle_insert_futs.load(Acquire);
        let poll_time_get_series_futs = self.poll_time_get_series_futs.load(Acquire);
        let time_handle_conn_listen = self.time_handle_conn_listen.load(Acquire);
        let time_handle_peer_ready = self.time_handle_peer_ready.load(Acquire);
        let time_check_channels_state_init = self.time_check_channels_state_init.load(Acquire);
        let time_handle_event_add_res = self.time_check_channels_state_init.load(Acquire);
        let poll_pct_handle_insert_futs = poll_time_handle_insert_futs * 100 / poll_time;
        let poll_pct_get_series_futs = poll_time_get_series_futs * 100 / poll_time;
        let pct_handle_conn_listen = time_handle_conn_listen * 100 / poll_time;
        let pct_handle_peer_ready = time_handle_peer_ready * 100 / poll_time;
        let pct_check_channels_state_init = time_check_channels_state_init * 100 / poll_time;
        let pct_handle_event_add_res = time_handle_event_add_res * 100 / poll_time;
        let inserts_discarded_freq = self.inserts_discarded.load(Acquire);
        write!(
            fmt,
            "insfreq {} disc {} poll_time {:5} ms inserts {:2}% seriesid {:2}% listen {:2}% peer {:2}% checkinit {:2}% evadd {:2}%",
            insert_freq,inserts_discarded_freq,
            poll_time / 1000,
            poll_pct_handle_insert_futs,
            poll_pct_get_series_futs,
            pct_handle_conn_listen,
            pct_handle_peer_ready,
            pct_check_channels_state_init,
            pct_handle_event_add_res
        )
    }
}
