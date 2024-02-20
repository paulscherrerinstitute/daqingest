use crate::config::ScyllaIngestConfig;
use crate::session::create_session;
use err::thiserror;
use err::ThisError;
use scylla::prepared_statement::PreparedStatement;
use scylla::transport::errors::NewSessionError;
use scylla::transport::errors::QueryError;
use scylla::Session as ScySession;
use std::sync::Arc;

#[derive(Debug, ThisError)]
pub enum Error {
    NewSessionError(#[from] NewSessionError),
    QueryError(#[from] QueryError),
    NewSession,
}

pub struct DataStore {
    pub scy: Arc<ScySession>,
    pub qu_insert_ts_msp: Arc<PreparedStatement>,
    pub qu_insert_series_by_ts_msp: Arc<PreparedStatement>,
    pub qu_insert_scalar_i8: Arc<PreparedStatement>,
    pub qu_insert_scalar_i16: Arc<PreparedStatement>,
    pub qu_insert_scalar_i32: Arc<PreparedStatement>,
    pub qu_insert_scalar_i64: Arc<PreparedStatement>,
    pub qu_insert_scalar_f32: Arc<PreparedStatement>,
    pub qu_insert_scalar_f64: Arc<PreparedStatement>,
    pub qu_insert_scalar_bool: Arc<PreparedStatement>,
    pub qu_insert_scalar_string: Arc<PreparedStatement>,
    pub qu_insert_array_i8: Arc<PreparedStatement>,
    pub qu_insert_array_i16: Arc<PreparedStatement>,
    pub qu_insert_array_i32: Arc<PreparedStatement>,
    pub qu_insert_array_i64: Arc<PreparedStatement>,
    pub qu_insert_array_f32: Arc<PreparedStatement>,
    pub qu_insert_array_f64: Arc<PreparedStatement>,
    pub qu_insert_array_bool: Arc<PreparedStatement>,
    pub qu_insert_connection_status: Arc<PreparedStatement>,
    pub qu_insert_channel_status: Arc<PreparedStatement>,
    pub qu_insert_channel_status_by_ts_msp: Arc<PreparedStatement>,
    pub qu_insert_binned_scalar_f32_v02: Arc<PreparedStatement>,
    pub qu_account_00: Arc<PreparedStatement>,
}

impl DataStore {
    pub async fn new(scyconf: &ScyllaIngestConfig) -> Result<Self, Error> {
        let scy = create_session(scyconf).await.map_err(|_| Error::NewSession)?;

        let q = scy.prepare("insert into ts_msp (series, ts_msp) values (?, ?)").await?;
        let qu_insert_ts_msp = Arc::new(q);

        let cql = "insert into series_by_ts_msp (part, ts_msp, shape_kind, scalar_type, series) values (?, ?, ?, ?, ?)";
        let q = scy.prepare(cql).await?;
        let qu_insert_series_by_ts_msp = Arc::new(q);

        // scalar:
        let cql = "insert into events_scalar_i8 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)";
        let q = scy.prepare(cql).await?;
        let qu_insert_scalar_i8 = Arc::new(q);

        let cql = "insert into events_scalar_i16 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)";
        let q = scy.prepare(cql).await?;
        let qu_insert_scalar_i16 = Arc::new(q);

        let cql = "insert into events_scalar_i32 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)";
        let q = scy.prepare(cql).await?;
        let qu_insert_scalar_i32 = Arc::new(q);

        let cql = "insert into events_scalar_i64 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)";
        let q = scy.prepare(cql).await?;
        let qu_insert_scalar_i64 = Arc::new(q);

        let cql = "insert into events_scalar_f32 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)";
        let q = scy.prepare(cql).await?;
        let qu_insert_scalar_f32 = Arc::new(q);

        let cql = "insert into events_scalar_f64 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)";
        let q = scy.prepare(cql).await?;
        let qu_insert_scalar_f64 = Arc::new(q);

        let cql = "insert into events_scalar_bool (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)";
        let q = scy.prepare(cql).await?;
        let qu_insert_scalar_bool = Arc::new(q);

        let cql = "insert into events_scalar_string (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)";
        let q = scy.prepare(cql).await?;
        let qu_insert_scalar_string = Arc::new(q);

        // array
        let cql = "insert into events_array_i8 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)";
        let q = scy.prepare(cql).await?;
        let qu_insert_array_i8 = Arc::new(q);

        let cql = "insert into events_array_i16 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)";
        let q = scy.prepare(cql).await?;
        let qu_insert_array_i16 = Arc::new(q);

        let cql = "insert into events_array_i32 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)";
        let q = scy.prepare(cql).await?;
        let qu_insert_array_i32 = Arc::new(q);

        let cql = "insert into events_array_i64 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)";
        let q = scy.prepare(cql).await?;
        let qu_insert_array_i64 = Arc::new(q);

        let cql = "insert into events_array_f32 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)";
        let q = scy.prepare(cql).await?;
        let qu_insert_array_f32 = Arc::new(q);

        let cql = "insert into events_array_f64 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)";
        let q = scy.prepare(cql).await?;
        let qu_insert_array_f64 = Arc::new(q);

        let cql = "insert into events_array_bool (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)";
        let q = scy.prepare(cql).await?;
        let qu_insert_array_bool = Arc::new(q);

        // Connection status:
        let cql = "insert into connection_status (ts_msp, ts_lsp, kind, addr) values (?, ?, ?, ?)";
        let q = scy.prepare(cql).await?;
        let qu_insert_connection_status = Arc::new(q);

        let cql = "insert into channel_status (series, ts_msp, ts_lsp, kind) values (?, ?, ?, ?)";
        let q = scy.prepare(cql).await?;
        let qu_insert_channel_status = Arc::new(q);

        let cql = "insert into channel_status_by_ts_msp (ts_msp, ts_lsp, series, kind) values (?, ?, ?, ?)";
        let q = scy.prepare(cql).await?;
        let qu_insert_channel_status_by_ts_msp = Arc::new(q);

        let cql = concat!(
            "insert into binned_scalar_f32 (",
            "series, bin_len_ms, ts_msp, off, count, min, max, avg)",
            " values (?, ?, ?, ?, ?, ?, ?, ?)"
        );
        let q = scy.prepare(cql).await?;
        let qu_insert_binned_scalar_f32_v02 = Arc::new(q);

        let cql = concat!(
            "insert into account_00",
            " (part, ts, series, count, bytes)",
            " values (?, ?, ?, ?, ?)"
        );
        let q = scy.prepare(cql).await?;
        let qu_account_00 = Arc::new(q);

        let ret = Self {
            scy,
            qu_insert_ts_msp,
            qu_insert_series_by_ts_msp,
            qu_insert_scalar_i8,
            qu_insert_scalar_i16,
            qu_insert_scalar_i32,
            qu_insert_scalar_i64,
            qu_insert_scalar_f32,
            qu_insert_scalar_f64,
            qu_insert_scalar_bool,
            qu_insert_scalar_string,
            qu_insert_array_i8,
            qu_insert_array_i16,
            qu_insert_array_i32,
            qu_insert_array_i64,
            qu_insert_array_f32,
            qu_insert_array_f64,
            qu_insert_array_bool,
            qu_insert_connection_status,
            qu_insert_channel_status,
            qu_insert_channel_status_by_ts_msp,
            qu_insert_binned_scalar_f32_v02,
            qu_account_00,
        };
        Ok(ret)
    }
}
