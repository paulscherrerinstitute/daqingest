use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct ScyllaIngestConfig {
    hosts: Vec<String>,
    keyspace: String,
    keyspace_rf1: Option<String>,
}

impl ScyllaIngestConfig {
    pub fn new<I, H, K1, K2>(hosts: I, ks_rf3: K1, ks_rf1: K2) -> Self
    where
        I: IntoIterator<Item = H>,
        H: Into<String>,
        K1: Into<String>,
        K2: Into<String>,
    {
        Self {
            hosts: hosts.into_iter().map(Into::into).collect(),
            keyspace: ks_rf3.into(),
            keyspace_rf1: Some(ks_rf1.into()),
        }
    }

    pub fn hosts(&self) -> &Vec<String> {
        &self.hosts
    }

    pub fn keyspace(&self) -> &String {
        &self.keyspace
    }

    pub fn keyspace_rf1(&self) -> Option<&String> {
        self.keyspace_rf1.as_ref()
    }
}
