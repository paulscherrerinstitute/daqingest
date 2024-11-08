use crate::config::ScyllaIngestConfig;
use crate::session::create_session;
use err::thiserror;
use err::ThisError;
use netpod::ttl::RetentionTime;
use scylla::prepared_statement::PreparedStatement;
use scylla::transport::errors::NewSessionError;
use scylla::transport::errors::QueryError;
use scylla::Session as ScySession;
use std::sync::Arc;

#[derive(Debug, ThisError)]
#[cstm(name = "ScyllaStore")]
pub enum Error {
    NewSessionError(#[from] NewSessionError),
    QueryError(#[from] QueryError),
    NewSession,
}

pub struct DataStore {
    pub rett: RetentionTime,
    pub scy: Arc<ScySession>,
    pub qu_insert_ts_msp: Arc<PreparedStatement>,
    pub qu_insert_scalar_u8: Arc<PreparedStatement>,
    pub qu_insert_scalar_u16: Arc<PreparedStatement>,
    pub qu_insert_scalar_u32: Arc<PreparedStatement>,
    pub qu_insert_scalar_u64: Arc<PreparedStatement>,
    pub qu_insert_scalar_i8: Arc<PreparedStatement>,
    pub qu_insert_scalar_i16: Arc<PreparedStatement>,
    pub qu_insert_scalar_i32: Arc<PreparedStatement>,
    pub qu_insert_scalar_i64: Arc<PreparedStatement>,
    pub qu_insert_scalar_f32: Arc<PreparedStatement>,
    pub qu_insert_scalar_f64: Arc<PreparedStatement>,
    pub qu_insert_scalar_bool: Arc<PreparedStatement>,
    pub qu_insert_scalar_string: Arc<PreparedStatement>,
    pub qu_insert_scalar_enum: Arc<PreparedStatement>,
    pub qu_insert_array_u8: Arc<PreparedStatement>,
    pub qu_insert_array_u16: Arc<PreparedStatement>,
    pub qu_insert_array_u32: Arc<PreparedStatement>,
    pub qu_insert_array_u64: Arc<PreparedStatement>,
    pub qu_insert_array_i8: Arc<PreparedStatement>,
    pub qu_insert_array_i16: Arc<PreparedStatement>,
    pub qu_insert_array_i32: Arc<PreparedStatement>,
    pub qu_insert_array_i64: Arc<PreparedStatement>,
    pub qu_insert_array_f32: Arc<PreparedStatement>,
    pub qu_insert_array_f64: Arc<PreparedStatement>,
    pub qu_insert_array_bool: Arc<PreparedStatement>,
    pub qu_insert_binned_scalar_f32_v02: Arc<PreparedStatement>,
    pub qu_account_00: Arc<PreparedStatement>,
    pub qu_account_recv_00: Arc<PreparedStatement>,
    pub qu_dummy: Arc<PreparedStatement>,
}

macro_rules! prep_qu_ins_a {
    ($id1:expr, $rett:expr, $scy:expr) => {{
        let cql = format!(
            concat!(
                "insert into {}{} (series, ts_msp, ts_lsp, value)",
                " values (?, ?, ?, ?)"
            ),
            $rett.table_prefix(),
            $id1
        );
        let q = $scy.prepare(cql).await?;
        Arc::new(q)
    }};
}

macro_rules! prep_qu_ins_b {
    ($id1:expr, $rett:expr, $scy:expr) => {{
        let cql = format!(
            concat!(
                "insert into {}{} (series, ts_msp, ts_lsp, valueblob)",
                " values (?, ?, ?, ?)"
            ),
            $rett.table_prefix(),
            $id1
        );
        let q = $scy.prepare(cql).await?;
        Arc::new(q)
    }};
}

macro_rules! prep_qu_ins_enum {
    ($id1:expr, $rett:expr, $scy:expr) => {{
        let cql = format!(
            concat!(
                "insert into {}{} (series, ts_msp, ts_lsp, value, valuestr)",
                " values (?, ?, ?, ?, ?)"
            ),
            $rett.table_prefix(),
            $id1
        );
        let q = $scy.prepare(cql).await?;
        Arc::new(q)
    }};
}

macro_rules! prep_qu_ins_c {
    ($id1:expr, $fields:expr, $values:expr, $rett:expr, $scy:expr) => {{
        let cql = format!(
            concat!("insert into {}{} ({})", " values ({})"),
            $rett.table_prefix(),
            $id1,
            $fields,
            $values,
        );
        let q = $scy.prepare(cql).await?;
        Arc::new(q)
    }};
}

impl DataStore {
    pub async fn new(scyconf: &ScyllaIngestConfig, rett: RetentionTime) -> Result<Self, Error> {
        let scy = create_session(scyconf).await.map_err(|_| Error::NewSession)?;

        let q = scy
            .prepare(format!(
                concat!("insert into {}{} (series, ts_msp) values (?, ?)"),
                rett.table_prefix(),
                "ts_msp"
            ))
            .await?;
        let qu_insert_ts_msp = Arc::new(q);

        let qu_insert_scalar_u8 = prep_qu_ins_a!("events_scalar_u8", rett, scy);
        let qu_insert_scalar_u16 = prep_qu_ins_a!("events_scalar_u16", rett, scy);
        let qu_insert_scalar_u32 = prep_qu_ins_a!("events_scalar_u32", rett, scy);
        let qu_insert_scalar_u64 = prep_qu_ins_a!("events_scalar_u64", rett, scy);
        let qu_insert_scalar_i8 = prep_qu_ins_a!("events_scalar_i8", rett, scy);
        let qu_insert_scalar_i16 = prep_qu_ins_a!("events_scalar_i16", rett, scy);
        let qu_insert_scalar_i32 = prep_qu_ins_a!("events_scalar_i32", rett, scy);
        let qu_insert_scalar_i64 = prep_qu_ins_a!("events_scalar_i64", rett, scy);
        let qu_insert_scalar_f32 = prep_qu_ins_a!("events_scalar_f32", rett, scy);
        let qu_insert_scalar_f64 = prep_qu_ins_a!("events_scalar_f64", rett, scy);
        let qu_insert_scalar_bool = prep_qu_ins_a!("events_scalar_bool", rett, scy);
        let qu_insert_scalar_string = prep_qu_ins_a!("events_scalar_string", rett, scy);
        let qu_insert_scalar_enum = prep_qu_ins_enum!("events_scalar_enum", rett, scy);

        let qu_insert_array_u8 = prep_qu_ins_b!("events_array_u8", rett, scy);
        let qu_insert_array_u16 = prep_qu_ins_b!("events_array_u16", rett, scy);
        let qu_insert_array_u32 = prep_qu_ins_b!("events_array_u32", rett, scy);
        let qu_insert_array_u64 = prep_qu_ins_b!("events_array_u64", rett, scy);
        let qu_insert_array_i8 = prep_qu_ins_b!("events_array_i8", rett, scy);
        let qu_insert_array_i16 = prep_qu_ins_b!("events_array_i16", rett, scy);
        let qu_insert_array_i32 = prep_qu_ins_b!("events_array_i32", rett, scy);
        let qu_insert_array_i64 = prep_qu_ins_b!("events_array_i64", rett, scy);
        let qu_insert_array_f32 = prep_qu_ins_b!("events_array_f32", rett, scy);
        let qu_insert_array_f64 = prep_qu_ins_b!("events_array_f64", rett, scy);
        let qu_insert_array_bool = prep_qu_ins_b!("events_array_bool", rett, scy);

        let qu_insert_binned_scalar_f32_v02 = prep_qu_ins_c!(
            "binned_scalar_f32_v02",
            "series, binlen, msp, off, cnt, min, max, avg, dev",
            "?, ?, ?, ?, ?, ?, ?, ?, ?",
            rett,
            scy
        );

        let qu_account_00 = prep_qu_ins_c!(
            "account_00",
            "part, ts, series, count, bytes",
            "?, ?, ?, ?, ?",
            rett,
            scy
        );

        let qu_account_recv_00 = prep_qu_ins_c!(
            "account_recv_00",
            "part, ts, series, count, bytes",
            "?, ?, ?, ?, ?",
            rett,
            scy
        );

        let q = scy
            .prepare(format!(
                concat!("select * from {}{} limit 1"),
                rett.table_prefix(),
                "ts_msp"
            ))
            .await?;
        let qu_dummy = Arc::new(q);

        let ret = Self {
            rett,
            scy,
            qu_insert_ts_msp,
            qu_insert_scalar_u8,
            qu_insert_scalar_u16,
            qu_insert_scalar_u32,
            qu_insert_scalar_u64,
            qu_insert_scalar_i8,
            qu_insert_scalar_i16,
            qu_insert_scalar_i32,
            qu_insert_scalar_i64,
            qu_insert_scalar_f32,
            qu_insert_scalar_f64,
            qu_insert_scalar_bool,
            qu_insert_scalar_string,
            qu_insert_scalar_enum,
            qu_insert_array_u8,
            qu_insert_array_u16,
            qu_insert_array_u32,
            qu_insert_array_u64,
            qu_insert_array_i8,
            qu_insert_array_i16,
            qu_insert_array_i32,
            qu_insert_array_i64,
            qu_insert_array_f32,
            qu_insert_array_f64,
            qu_insert_array_bool,
            qu_insert_binned_scalar_f32_v02,
            qu_account_00,
            qu_account_recv_00,
            qu_dummy,
        };
        Ok(ret)
    }
}
