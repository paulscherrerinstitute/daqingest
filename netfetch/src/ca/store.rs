use err::Error;
use futures_util::StreamExt;
use netpod::ScyllaConfig;
use scylla::execution_profile::ExecutionProfileBuilder;
use scylla::prepared_statement::PreparedStatement;
use scylla::statement::Consistency;
use scylla::Session as ScySession;
use std::sync::Arc;

pub struct DataStore {
    pub scy: Arc<ScySession>,
    pub qu_insert_ts_msp: Arc<PreparedStatement>,
    pub qu_insert_series_by_ts_msp: Arc<PreparedStatement>,
    pub qu_insert_scalar_i8: Arc<PreparedStatement>,
    pub qu_insert_scalar_i16: Arc<PreparedStatement>,
    pub qu_insert_scalar_i32: Arc<PreparedStatement>,
    pub qu_insert_scalar_f32: Arc<PreparedStatement>,
    pub qu_insert_scalar_f64: Arc<PreparedStatement>,
    pub qu_insert_scalar_string: Arc<PreparedStatement>,
    pub qu_insert_array_i8: Arc<PreparedStatement>,
    pub qu_insert_array_i16: Arc<PreparedStatement>,
    pub qu_insert_array_i32: Arc<PreparedStatement>,
    pub qu_insert_array_f32: Arc<PreparedStatement>,
    pub qu_insert_array_f64: Arc<PreparedStatement>,
    pub qu_insert_array_bool: Arc<PreparedStatement>,
    pub qu_insert_muted: Arc<PreparedStatement>,
    pub qu_insert_item_recv_ivl: Arc<PreparedStatement>,
    pub qu_insert_connection_status: Arc<PreparedStatement>,
    pub qu_insert_channel_status: Arc<PreparedStatement>,
    pub qu_insert_channel_status_by_ts_msp: Arc<PreparedStatement>,
    pub qu_insert_channel_ping: Arc<PreparedStatement>,
    pub qu_insert_binned_scalar_f32_v01: Arc<PreparedStatement>,
}

impl DataStore {
    pub async fn new(scyconf: &ScyllaConfig) -> Result<Self, Error> {
        let scy = scylla::SessionBuilder::new()
            .known_nodes(&scyconf.hosts)
            .use_keyspace(&scyconf.keyspace, true)
            .default_execution_profile_handle(
                ExecutionProfileBuilder::default()
                    .consistency(Consistency::LocalOne)
                    .build()
                    .into_handle(),
            )
            .build()
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let scy = Arc::new(scy);

        let q = scy
            .prepare("insert into ts_msp (series, ts_msp) values (?, ?) using ttl ?")
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let qu_insert_ts_msp = Arc::new(q);
        let q = scy
            .prepare(
                "insert into series_by_ts_msp (part, ts_msp, shape_kind, scalar_type, series) values (?, ?, ?, ?, ?) using ttl ?",
            )
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let qu_insert_series_by_ts_msp = Arc::new(q);

        // scalar:
        let q = scy
            .prepare("insert into events_scalar_i8 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?) using ttl ?")
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let qu_insert_scalar_i8 = Arc::new(q);
        let q = scy
            .prepare("insert into events_scalar_i16 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?) using ttl ?")
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let qu_insert_scalar_i16 = Arc::new(q);
        let q = scy
            .prepare("insert into events_scalar_i32 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?) using ttl ?")
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let qu_insert_scalar_i32 = Arc::new(q);
        let q = scy
            .prepare("insert into events_scalar_f32 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?) using ttl ?")
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let qu_insert_scalar_f32 = Arc::new(q);
        let q = scy
            .prepare("insert into events_scalar_f64 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?) using ttl ?")
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let qu_insert_scalar_f64 = Arc::new(q);
        let q = scy
            .prepare("insert into events_scalar_string (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?) using ttl ?")
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let qu_insert_scalar_string = Arc::new(q);

        // array
        let q = scy
            .prepare(
                "insert into events_array_i8 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?) using ttl ?",
            )
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let qu_insert_array_i8 = Arc::new(q);
        let q = scy
            .prepare("insert into events_array_i16 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?) using ttl ?")
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let qu_insert_array_i16 = Arc::new(q);
        let q = scy
            .prepare("insert into events_array_i32 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?) using ttl ?")
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let qu_insert_array_i32 = Arc::new(q);
        let q = scy
            .prepare("insert into events_array_f32 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?) using ttl ?")
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let qu_insert_array_f32 = Arc::new(q);
        let q = scy
            .prepare("insert into events_array_f64 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?) using ttl ?")
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let qu_insert_array_f64 = Arc::new(q);
        let q = scy
            .prepare("insert into events_array_bool (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?) using ttl ?")
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let qu_insert_array_bool = Arc::new(q);
        // Others:
        let q = scy
            .prepare("insert into muted (part, series, ts, ema, emd) values (?, ?, ?, ?, ?) using ttl ?")
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let qu_insert_muted = Arc::new(q);
        let q = scy
            .prepare("insert into item_recv_ivl (part, series, ts, ema, emd) values (?, ?, ?, ?, ?) using ttl ?")
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let qu_insert_item_recv_ivl = Arc::new(q);
        // Connection status:
        let q = scy
            .prepare("insert into connection_status (ts_msp, ts_lsp, kind, addr) values (?, ?, ?, ?) using ttl ?")
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let qu_insert_connection_status = Arc::new(q);
        let q = scy
            .prepare("insert into channel_status (series, ts_msp, ts_lsp, kind) values (?, ?, ?, ?) using ttl ?")
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let qu_insert_channel_status = Arc::new(q);
        let q = scy
            .prepare(
                "insert into channel_status_by_ts_msp (ts_msp, ts_lsp, series, kind) values (?, ?, ?, ?) using ttl ?",
            )
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let qu_insert_channel_status_by_ts_msp = Arc::new(q);
        let q = scy
            .prepare("insert into channel_ping (part, ts_msp, series, ivl, interest, evsize) values (?, ?, ?, ?, ?, ?) using ttl ?")
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let qu_insert_channel_ping = Arc::new(q);

        let q = scy
            .prepare("insert into binned_scalar_f32_v01 (series, bin_len_sec, bin_count, off_msp, off_lsp, counts, mins, maxs, avgs) values (?, ?, ?, ?, ?, ?, ?, ?, ?) using ttl ?")
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let qu_insert_binned_scalar_f32_v01 = Arc::new(q);
        let ret = Self {
            scy,
            qu_insert_ts_msp,
            qu_insert_series_by_ts_msp,
            qu_insert_scalar_i8,
            qu_insert_scalar_i16,
            qu_insert_scalar_i32,
            qu_insert_scalar_f32,
            qu_insert_scalar_f64,
            qu_insert_scalar_string,
            qu_insert_array_i8,
            qu_insert_array_i16,
            qu_insert_array_i32,
            qu_insert_array_f32,
            qu_insert_array_f64,
            qu_insert_array_bool,
            qu_insert_muted,
            qu_insert_item_recv_ivl,
            qu_insert_connection_status,
            qu_insert_channel_status,
            qu_insert_channel_status_by_ts_msp,
            qu_insert_channel_ping,
            qu_insert_binned_scalar_f32_v01,
        };
        Ok(ret)
    }
}
