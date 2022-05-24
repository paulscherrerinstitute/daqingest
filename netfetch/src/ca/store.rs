use crate::bsread::ChannelDescDecoded;
use crate::series::{Existence, SeriesId};
use crate::store::{CommonInsertQueue, CommonInsertQueueSender};
use async_channel::{Receiver, Sender};
use err::Error;
use scylla::prepared_statement::PreparedStatement;
use scylla::Session as ScySession;
use std::sync::Arc;
use tokio_postgres::Client as PgClient;

pub struct RegisterJob {
    desc: ChannelDescDecoded,
}

impl RegisterJob {
    pub fn new(desc: ChannelDescDecoded) -> Self {
        Self { desc }
    }
}

pub struct RegisterChannel {
    tx: Sender<RegisterJob>,
    rx: Receiver<RegisterJob>,
}

pub struct ChannelRegistry {
    scy: Arc<ScySession>,
    pg_client: Arc<PgClient>,
}

impl ChannelRegistry {
    pub fn new(pg_client: Arc<PgClient>, scy: Arc<ScySession>) -> Self {
        Self { pg_client, scy }
    }

    pub async fn get_series_id(&self, cd: ChannelDescDecoded) -> Result<Existence<SeriesId>, Error> {
        crate::series::get_series_id(&self.pg_client, &cd).await
    }
}

pub struct DataStore {
    pub scy: Arc<ScySession>,
    pub qu_insert_series: Arc<PreparedStatement>,
    pub qu_insert_ts_msp: Arc<PreparedStatement>,
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
    pub chan_reg: Arc<ChannelRegistry>,
    pub ciqs: CommonInsertQueueSender,
}

impl DataStore {
    pub async fn new(
        pg_client: Arc<PgClient>,
        scy: Arc<ScySession>,
        ciqs: CommonInsertQueueSender,
    ) -> Result<Self, Error> {
        let q = scy
            .prepare("insert into series (part, series, ts_msp, scalar_type, shape_dims) values (?, ?, ?, ?, ?)")
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let qu_insert_series = Arc::new(q);
        let q = scy
            .prepare("insert into ts_msp (series, ts_msp) values (?, ?)")
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let qu_insert_ts_msp = Arc::new(q);
        let q = scy
            .prepare("insert into events_scalar_i8 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)")
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let qu_insert_scalar_i8 = Arc::new(q);
        let q = scy
            .prepare("insert into events_scalar_i16 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)")
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let qu_insert_scalar_i16 = Arc::new(q);
        let q = scy
            .prepare("insert into events_scalar_i32 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)")
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let qu_insert_scalar_i32 = Arc::new(q);
        let q = scy
            .prepare("insert into events_scalar_f32 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)")
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let qu_insert_scalar_f32 = Arc::new(q);
        let q = scy
            .prepare("insert into events_scalar_f64 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)")
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let qu_insert_scalar_f64 = Arc::new(q);
        let q = scy
            .prepare("insert into events_scalar_string (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)")
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let qu_insert_scalar_string = Arc::new(q);
        // array
        let q = scy
            .prepare("insert into events_array_i8 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)")
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let qu_insert_array_i8 = Arc::new(q);
        let q = scy
            .prepare("insert into events_array_i16 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)")
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let qu_insert_array_i16 = Arc::new(q);
        let q = scy
            .prepare("insert into events_array_i32 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)")
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let qu_insert_array_i32 = Arc::new(q);
        let q = scy
            .prepare("insert into events_array_f32 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)")
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let qu_insert_array_f32 = Arc::new(q);
        let q = scy
            .prepare("insert into events_array_f64 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)")
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let qu_insert_array_f64 = Arc::new(q);
        let ret = Self {
            chan_reg: Arc::new(ChannelRegistry::new(pg_client, scy.clone())),
            scy,
            qu_insert_series,
            qu_insert_ts_msp,
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
            ciqs,
        };
        Ok(ret)
    }
}
