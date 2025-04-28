mod Metrics {
    type StructName = ScyllaInsertWorker;
    enum counters {
        metrics_emit,
        job_ok,
        job_err,
    }
    enum histolog2s {
        job_dt1,
        job_dt2,
        job_dt_net,
    }
}

mod Metrics {
    type StructName = CaConnMetrics;
    enum counters {
        metrics_emit,
        metrics_emit_final,
        ioid_read_begin,
        ioid_read_done,
        ioid_read_timeout,
        ioid_read_error_exists,
        ioid_read_error_not_found,
        recv_read_notify_state_passive_found_ioid,
        proto_out_push,
        logic_error,
        poll_fn_begin,
        poll_loop_begin,
        poll_no_progress_no_pending,
        poll_pending,
        poll_reloop,
        poll_wake_break,
        insert_item_queue_full,
        insert_item_queue_pressure,
        out_queue_full,
        loop2_count,
        loop3_count,
        tcp_connected,
        ca_proto_no_version_as_first,
        ca_proto_version_later,
        ca_msg_recv,
        event_add_res_recv,
        time_check_channels_state_init,
        ping_no_proto,
        ping_start,
        pong_timeout,
        caget_timeout,
        caget_issued,
        monitor_stale_read_timeout,
        monitor_stale_read_begin,
        unknown_ioid,
        recv_read_notify_state_read_pending,
        recv_read_notify_state_read_pending_bad_ioid,
        recv_read_notify_while_polling_idle,
        no_cid_for_subid,
        recv_read_notify_but_no_longer_ready,
        recv_read_notify_but_not_init_yet,
        recv_event_add_while_wait_on_read_notify,
        transition_to_polling,
        transition_to_polling_bad_state,
        transition_to_polling_already_in,
        unknown_subid,
        get_series_id_ok,
        channel_add_exists,
    }
    enum histolog2s {
        clock_ioc_diff_abs,
        caget_lat,
        poll_reloops,
        poll_all_dt,
        iiq_batch_len,
        pong_recv_lat,
    }
    mod Compose {
        type Input = ca_proto::mett::CaProtoMetrics;
        type Name = proto;
    }
}

mod Metrics {
    type StructName = CaConnSetMetrics;
    mod Compose {
        type Input = CaConnMetrics;
        type Name = ca_conn;
    }
}

mod Metrics {
    type StructName = DaemonMetrics;
    mod Compose {
        type Input = CaConnSetMetrics;
        type Name = ca_conn_set;
    }
    mod Compose {
        type Input = ScyllaInsertWorker;
        type Name = scy_inswork;
    }
    enum counters {
        handle_event,
        caconnset_health_response,
        channel_send_err,
    }
    enum values {
        proc_cpu_v0,
        proc_mem_rss,
        iqtx_len_st_rf1,
        iqtx_len_st_rf3,
        iqtx_len_mt_rf3,
        iqtx_len_lt_rf3,
        iqtx_len_lt_rf3_lat5,
    }
}
