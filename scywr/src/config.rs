use netpod::ttl::RetentionTime;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct ScyllaIngestConfig {
    keyspace: String,
    hosts: Vec<String>,
}

impl ScyllaIngestConfig {
    pub fn new<I, H, K1>(hosts: I, ks: K1) -> Self
    where
        I: IntoIterator<Item = H>,
        H: Into<String>,
        K1: Into<String>,
    {
        Self {
            keyspace: ks.into(),
            hosts: hosts.into_iter().map(Into::into).collect(),
        }
    }

    pub fn keyspace(&self) -> &String {
        &self.keyspace
    }

    pub fn hosts(&self) -> &Vec<String> {
        &self.hosts
    }

    pub fn short_name(&self, rt: RetentionTime) -> String {
        format!("Scyconf {{ {:?}, {:?}, {:?} }}", self.hosts.get(0), self.keyspace, rt)
    }
}
