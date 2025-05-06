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
        recv_read_notify_while_enabling_monitoring,
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
        poll_op3_dt,
        iiq_batch_len,
        pong_recv_lat,
        ca_ts_off,
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
    enum counters {
        poll_fn_begin,
        poll_loop_begin,
        ready_for_end_of_stream,
        ready_for_end_of_stream_with_progress,
        poll_reloop,
        poll_pending,
        poll_no_progress_no_pending,
        ioc_search_start,
        chan_send_err,
        cmd_res_send_err,
        logic_err,
        channel_status_series_found,
        ioc_addr_found,
        ioc_addr_not_found,
        ioc_addr_result_for_unknown_channel,
        ca_conn_eos_ok,
        ca_conn_eos_unexpected,
        handle_add_channel_with_addr,
        try_push_ca_conn_cmds_sent,
        try_push_ca_conn_cmds_closed,
        create_ca_conn,
        storage_insert_queue_send,
        ca_conn_task_join_done_ok,
        ca_conn_task_join_done_err,
        ca_conn_task_join_err,
    }
    enum values {
        channel_info_query_queue_len,
        channel_info_query_sender_len,
        channel_info_res_tx_len,
        ca_conn_res_tx_len,
        find_ioc_query_sender_len,
        channel_rogue,
        channel_unknown_address,
        channel_search_pending,
        channel_no_address,
        channel_unassigned,
        channel_assigned,
        channel_connected,
        channel_maybe_wrong_address,
        channel_assigned_without_health_update,
        channel_health_timeout_soon,
        channel_health_timeout_reached,
    }
    enum histolog2s {
        poll_all_dt,
    }
}

// mod Metrics {
//     type StructName = IocFinderMetrics;
//     enum counters {
//         dbsearcher_batch_recv,
//         dbsearcher_item_recv,
//         dbsearcher_select_res_0,
//         dbsearcher_select_error_len_mismatch,
//         dbsearcher_batch_send,
//         dbsearcher_item_send,
//         ca_udp_error,
//         ca_udp_warn,
//         ca_udp_unaccounted_data,
//         ca_udp_batch_created,
//         ca_udp_io_error,
//         ca_udp_io_empty,
//         ca_udp_io_recv,
//         ca_udp_first_msg_not_version,
//         ca_udp_recv_result,
//         ca_udp_recv_timeout,
//         ca_udp_logic_error,
//     }
// }

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
