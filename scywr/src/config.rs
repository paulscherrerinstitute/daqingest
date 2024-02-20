use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct ScyllaIngestConfig {
    hosts: Vec<String>,
    keyspace: String,
}

impl ScyllaIngestConfig {
    pub fn new<I, H, K>(hosts: I, ks: K) -> Self
    where
        I: IntoIterator<Item = H>,
        H: Into<String>,
        K: Into<String>,
    {
        Self {
            hosts: hosts.into_iter().map(Into::into).collect(),
            keyspace: ks.into(),
        }
    }

    pub fn hosts(&self) -> &Vec<String> {
        &self.hosts
    }

    pub fn keyspace(&self) -> &String {
        &self.keyspace
    }
}
