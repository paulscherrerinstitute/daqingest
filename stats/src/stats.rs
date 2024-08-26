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
        counters(
            critical_error,
            todo_mark,
            ticker_token_acquire_error,
            ticker_token_release_error,
            handle_timer_tick_count,
            ioc_search_err,
            ioc_search_some,
            ioc_search_none,
            lookupaddr_ok,
            events,
            event_ca_conn,
            ca_conn_status_done,
            ca_conn_status_feedback_timeout,
            ca_conn_status_feedback_recv,
            ca_conn_status_feedback_no_dst,
            ca_echo_timeout_total,
            caconn_done_channel_state_reset,
            insert_worker_spawned,
            insert_worker_join_ok,
            insert_worker_join_ok_err,
            insert_worker_join_err,
            caconnset_health_response,
        ),
        values(
            channel_unknown_address,
            channel_search_pending,
            channel_with_address,
            channel_no_address,
            connset_health_lat_ema,
        ),
    ),
    agg(name(DaemonStatsAgg), parent(DaemonStats)),
    diff(name(DaemonStatsAggDiff), input(DaemonStatsAgg)),
    stats_struct(
        name(CaConnStats),
        prefix(caconn),
        counters(
            insert_item_create,
            inserts_val,
            inserts_msp,
            inserts_msp_grid,
            inserts_queue_pop_for_global,
            inserts_queue_push,
            inserts_queue_drop,
            insert_item_queue_pressure,
            insert_item_queue_full,
            out_queue_full,
            channel_fast_item_drop,
            logic_error,
            // TODO maybe rename: this is now only the recv of the intermediate queue:
            store_worker_item_recv,
            // TODO rename to make clear that this drop is voluntary because of user config choice:
            // store_worker_fraction_drop,
            // store_worker_ratelimit_drop,
            // store_worker_insert_done,
            // store_worker_insert_binned_done,
            // store_worker_insert_overload,
            // store_worker_insert_timeout,
            // store_worker_insert_unavailable,
            // store_worker_insert_error,
            connection_status_insert_done,
            channel_status_insert_done,
            channel_info_insert_done,
            ivl_insert_done,
            mute_insert_done,
            poll_count,
            loop1_count,
            loop2_count,
            loop3_count,
            loop4_count,
            command_can_not_reply,
            time_handle_conn_listen,
            time_handle_peer_ready,
            time_check_channels_state_init,
            tcp_connected,
            get_series_id_ok,
            item_count,
            stream_ready,
            stream_pending,
            channel_all_count,
            channel_alive_count,
            channel_not_alive_count,
            channel_series_lookup_already_pending,
            ping_start,
            ping_no_proto,
            pong_timeout,
            poll_fn_begin,
            poll_loop_begin,
            poll_reloop,
            poll_pending,
            poll_no_progress_no_pending,
            poll_wake_break,
            storage_queue_send,
            storage_queue_pending,
            event_add_res_recv,
            caget_issued,
            caget_timeout,
            unknown_subid,
            unknown_ioid,
            transition_to_polling,
            transition_to_polling_already_in,
            transition_to_polling_bad_state,
            channel_add_exists,
            recv_event_add_while_wait_on_read_notify,
            recv_read_notify_state_passive_found_ioid,
            recv_read_notify_state_passive,
            recv_read_notify_state_read_pending_bad_ioid,
            recv_read_notify_state_read_pending,
            recv_read_notify_but_not_init_yet,
            recv_read_notify_but_no_longer_ready,
            recv_read_notify_while_enabling_monitoring,
            recv_read_notify_while_polling_idle,
            channel_not_alive_no_activity,
            monitor_stale_read_begin,
            monitor_stale_read_timeout,
        ),
        values(inter_ivl_ema, read_ioids_len, proto_out_len,),
        histolog2s(
            poll_all_dt,
            poll_op3_dt,
            poll_reloops,
            pong_recv_lat,
            ca_ts_off,
            iiq_len,
            iiq_batch_len,
            caget_lat,
        ),
    ),
    agg(name(CaConnStatsAgg), parent(CaConnStats)),
    diff(name(CaConnStatsAggDiff), input(CaConnStatsAgg)),
    stats_struct(
        name(CaProtoStats),
        prefix(ca_proto),
        counters(
            tcp_recv_count,
            tcp_recv_bytes,
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
        name(CaConnSetStats),
        prefix(connset),
        counters(
            channel_add,
            channel_status_series_found,
            channel_health_timeout_soon,
            channel_health_timeout,
            ioc_search_start,
            ioc_addr_found,
            ioc_addr_not_found,
            ioc_addr_result_for_unknown_channel,
            ca_conn_task_begin,
            ca_conn_task_done,
            ca_conn_task_join_done_ok,
            ca_conn_task_join_done_err,
            ca_conn_task_join_err,
            ca_conn_eos_ok,
            ca_conn_eos_err,
            ca_conn_eos_unexpected,
            response_tx_fail,
            try_push_ca_conn_cmds_sent,
            try_push_ca_conn_cmds_closed,
            logic_error,
            logic_issue,
            ready_for_end_of_stream,
            ready_for_end_of_stream_with_progress,
            poll_fn_begin,
            poll_loop_begin,
            poll_pending,
            poll_reloop,
            poll_no_progress_no_pending,
            handle_add_channel_with_addr,
            create_ca_conn,
            command_reply_fail,
            storage_insert_queue_send,
        ),
        values(
            storage_insert_queue_len,
            storage_insert_tx_len,
            channel_info_query_queue_len,
            channel_info_query_sender_len,
            channel_info_res_tx_len,
            find_ioc_query_sender_len,
            ca_conn_res_tx_len,
            channel_unknown_address,
            channel_search_pending,
            channel_no_address,
            channel_unassigned,
            channel_backoff,
            channel_assigned,
            channel_connected,
            channel_maybe_wrong_address,
            channel_assigned_without_health_update,
            channel_rogue,
        ),
        histolog2s(poll_all_dt,),
    ),
    // agg(name(CaConnSetStatsAgg), parent(CaConnSetStats)),
    // diff(name(CaConnSetStatsDiff), input(CaConnSetStats)),
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
