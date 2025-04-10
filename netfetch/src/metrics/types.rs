use scywr::insertqueues::InsertQueuesTx;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct CaConnMetrics {
    // a value
    pub ca_conn_event_out_queue_len: usize,
    // a term of a running counter sum
    pub ca_msg_recv_cnt: u64,
}

#[derive(Debug, Serialize)]
pub struct CaConnMetricsAgg {
    // derived from values
    pub ca_conn_event_out_queue_len_max: usize,
    // derived from counter terms
    pub ca_msg_recv_cnt_all: u64,
}

impl CaConnMetricsAgg {
    pub fn new() -> Self {
        Self {
            ca_conn_event_out_queue_len_max: 0,
            ca_msg_recv_cnt_all: 0,
        }
    }

    pub fn ingest(&mut self, inp: CaConnMetrics) {
        self.ca_conn_event_out_queue_len_max = self
            .ca_conn_event_out_queue_len_max
            .max(inp.ca_conn_event_out_queue_len);
        self.ca_msg_recv_cnt_all += inp.ca_msg_recv_cnt;
    }
}

#[derive(Debug, Serialize)]
pub struct CaConnMetricsAggAgg {
    // derived from values
    pub ca_conn_event_out_queue_len_max: usize,
    // derived from counter terms
    pub ca_msg_recv_cnt_all: u64,
}

impl CaConnMetricsAggAgg {
    pub fn new() -> Self {
        Self {
            ca_conn_event_out_queue_len_max: 0,
            ca_msg_recv_cnt_all: 0,
        }
    }

    pub fn ingest(&mut self, inp: CaConnMetricsAgg) {
        // take again the max of the maxs
        self.ca_conn_event_out_queue_len_max = self
            .ca_conn_event_out_queue_len_max
            .max(inp.ca_conn_event_out_queue_len_max);
        // sum up again to a total
        self.ca_msg_recv_cnt_all += inp.ca_msg_recv_cnt_all;
    }
}

#[derive(Debug, Serialize)]
pub struct CaConnSetMetrics {
    pub ca_conn_agg: CaConnMetricsAgg,
    pub mett: stats::mett::CaConnSetMetrics,
}

impl CaConnSetMetrics {
    pub fn new() -> Self {
        Self {
            ca_conn_agg: CaConnMetricsAgg::new(),
            mett: stats::mett::CaConnSetMetrics::new(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct CaConnSetAggMetrics {
    pub ca_conn_agg_agg: CaConnMetricsAggAgg,
}

impl CaConnSetAggMetrics {
    pub fn new() -> Self {
        Self {
            ca_conn_agg_agg: CaConnMetricsAggAgg::new(),
        }
    }

    pub fn ingest(&mut self, inp: CaConnSetMetrics) {
        {
            let src = inp.ca_conn_agg;
            let dst = &mut self.ca_conn_agg_agg;
            // take again the max of the maxs
            dst.ca_conn_event_out_queue_len_max = dst
                .ca_conn_event_out_queue_len_max
                .max(src.ca_conn_event_out_queue_len_max);
            dst.ca_msg_recv_cnt_all += src.ca_msg_recv_cnt_all;
        }
    }
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
