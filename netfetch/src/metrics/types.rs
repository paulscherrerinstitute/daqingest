use scywr::insertqueues::InsertQueuesTx;
use serde::Serialize;

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

#[derive(Debug, Serialize)]
pub struct MetricsPrometheusShort {
    counters: Vec<String>,
}

impl MetricsPrometheusShort {
    pub fn prometheus(&self) -> String {
        use std::fmt::Write;
        let mut s = String::new();
        for e in self.counters.iter() {
            write!(&mut s, "{}\n", e).unwrap();
        }
        s
    }
}

impl From<&stats::mett::DaemonMetrics> for MetricsPrometheusShort {
    fn from(value: &stats::mett::DaemonMetrics) -> Self {
        Self {
            counters: value.to_flatten_prometheus("daemon"),
        }
    }
}
