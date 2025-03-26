use scywr::insertqueues::InsertQueuesTx;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct CaConnMetrics {
    pub ca_conn_event_out_queue_len: usize,
}

pub struct InsertQueuesTxMetrics {
    pub st_rf1_len: usize,
    pub st_rf3_len: usize,
    pub mt_rf3_len: usize,
    pub lt_rf3_len: usize,
    pub lt_rf3_lat5_len: usize,
}

impl From<&InsertQueuesTx> for InsertQueuesTxMetrics {
    fn from(value: &InsertQueuesTx) -> Self {
        Self {
            st_rf1_len: value.st_rf1_tx.len(),
            st_rf3_len: value.st_rf3_tx.len(),
            mt_rf3_len: value.mt_rf3_tx.len(),
            lt_rf3_len: value.lt_rf3_tx.len(),
            lt_rf3_lat5_len: value.lt_rf3_lat5_tx.len(),
        }
    }
}
