use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct ScyllaIngestConfig {
    hosts: Vec<String>,
    keyspace: String,
    rf: u8,
}

impl ScyllaIngestConfig {
    pub fn new<I, H, K1>(hosts: I, ks: K1, rf: u8) -> Self
    where
        I: IntoIterator<Item = H>,
        H: Into<String>,
        K1: Into<String>,
    {
        Self {
            hosts: hosts.into_iter().map(Into::into).collect(),
            keyspace: ks.into(),
            rf,
        }
    }

    pub fn hosts(&self) -> &Vec<String> {
        &self.hosts
    }

    pub fn keyspace(&self) -> &String {
        &self.keyspace
    }

    pub fn rf(&self) -> u8 {
        self.rf
    }
}
