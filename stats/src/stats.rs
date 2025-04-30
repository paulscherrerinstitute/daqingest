pub mod mett;

pub use rand_xoshiro;

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

const US: u64 = 1000;
const MS: u64 = US * 1000;
const SEC: u64 = MS * 1000;

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

stats_proc::stats_struct!((
    stats_struct(
        name(DaemonStats),
        prefix(daemon),
        counters(asdasd,),
        values(
            channel_unknown_address,
            channel_search_pending,
            channel_with_address,
            channel_no_address,
            connset_health_lat_ema,
            // iqtx_len_st_rf1,
            iqtx_len_st_rf3,
            iqtx_len_mt_rf3,
            iqtx_len_lt_rf3,
            iqtx_len_lt_rf3_lat5,
        ),
    ),
    agg(name(DaemonStatsAgg), parent(DaemonStats)),
    diff(name(DaemonStatsAggDiff), input(DaemonStatsAgg)),
    stats_struct(
        name(CaProtoStats),
        prefix(ca_proto),
        counters(
            // tcp_recv_count,
            // tcp_recv_bytes,
            protocol_issue,
            payload_std_too_large,
            payload_ext_but_small,
            payload_ext_very_large,
            out_msg_placed,
            out_bytes,
        ),
        histolog2s(payload_size, data_count, outbuf_len,),
    ),
    stats_struct(
        name(SeriesByChannelStats),
        prefix(seriesbychannel),
        counters(res_tx_fail, res_tx_timeout, recv_batch, recv_items,),
        histolog2s(commit_duration_ms),
    ),
    stats_struct(
        name(InsertWorkerStats),
        prefix(insert_worker),
        counters(
            logic_error,
            item_recv,
            inserted_values,
            inserted_connection_status,
            inserted_channel_status,
            fraction_drop,
            inserted_mute,
            inserted_interval,
            inserted_channel_info,
            inserted_binned,
            db_overload,
            db_timeout,
            db_unavailable,
            db_error,
            query_error,
            inserts_msp,
            inserts_msp_grid,
            inserts_value,
            ratelimit_drop,
            worker_start,
            worker_finish,
        ),
        histolog2s(item_lat_net_worker, item_lat_net_store,),
    ),
    stats_struct(
        name(IocFinderStats),
        prefix(ioc_finder),
        counters(
            dbsearcher_batch_recv,
            dbsearcher_item_recv,
            dbsearcher_select_res_0,
            dbsearcher_select_error_len_mismatch,
            dbsearcher_batch_send,
            dbsearcher_item_send,
            ca_udp_error,
            ca_udp_warn,
            ca_udp_unaccounted_data,
            ca_udp_batch_created,
            ca_udp_io_error,
            ca_udp_io_empty,
            ca_udp_io_recv,
            ca_udp_first_msg_not_version,
            ca_udp_recv_result,
            ca_udp_recv_timeout,
            ca_udp_logic_error,
        ),
        values(db_lookup_workers,)
    ),
    stats_struct(
        name(SeriesWriterEstablishStats),
        prefix(wrest),
        counters(job_recv, result_send_fail,),
    ),
));

stats_proc::stats_struct!((
    stats_struct(name(TestStats0), counters(count0,), values(val0),),
    diff(name(TestStats0Diff), input(TestStats0)),
    agg(name(TestStats0Agg), parent(TestStats0)),
    diff(name(TestStats0AggDiff), input(TestStats0Agg)),
));

#[test]
fn test0_diff() {
    let stats_a = TestStats0::new();
    stats_a.count0().inc();
    stats_a.val0().set(43);
    let stats_b = stats_a.snapshot();
    stats_b.count0().inc();
    stats_b.count0().inc();
    stats_b.count0().inc();
    let diff = TestStats0Diff::diff_from(&stats_a, &stats_b);
    assert_eq!(diff.count0.load(), 3);
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
