use crate::bsread::ChannelDescDecoded;
use crate::series::{Existence, SeriesId};
use async_channel::{Receiver, Sender};
use err::Error;
use scylla::prepared_statement::PreparedStatement;
use scylla::Session as ScySession;
use std::sync::Arc;

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
}

impl ChannelRegistry {
    pub fn new(scy: Arc<ScySession>) -> Self {
        Self { scy }
    }

    pub async fn get_series_id(&self, cd: ChannelDescDecoded) -> Result<Existence<SeriesId>, Error> {
        crate::series::get_series_id(&self.scy, &cd).await
    }
}

pub struct DataStore {
    pub scy: Arc<ScySession>,
    pub qu_insert_ts_msp: Arc<PreparedStatement>,
    pub qu_insert_scalar_f64: Arc<PreparedStatement>,
    pub chan_reg: Arc<ChannelRegistry>,
}

impl DataStore {
    pub async fn new(scy: Arc<ScySession>) -> Result<Self, Error> {
        let q = scy
            .prepare("insert into ts_msp (series, ts_msp) values (?, ?)")
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let qu_insert_ts_msp = Arc::new(q);
        let q = scy
            .prepare("insert into events_scalar_f64 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)")
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let qu_insert_scalar_f64 = Arc::new(q);
        let ret = Self {
            chan_reg: Arc::new(ChannelRegistry::new(scy.clone())),
            scy,
            qu_insert_ts_msp,
            qu_insert_scalar_f64,
        };
        Ok(ret)
    }
}
