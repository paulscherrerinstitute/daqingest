use scywr::insertqueues::InsertQueuesTx;
use serde::Serialize;

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
