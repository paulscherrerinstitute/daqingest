pub use scylla::Session;
pub use Session as ScySession;

use crate::config::ScyllaIngestConfig;
use err::thiserror;
use err::ThisError;
use scylla::execution_profile::ExecutionProfileBuilder;
use scylla::statement::Consistency;
use scylla::transport::errors::NewSessionError;
use std::num::NonZeroUsize;
use std::sync::Arc;

#[derive(Debug, ThisError)]
pub enum Error {
    NewSession(String),
}

impl From<NewSessionError> for Error {
    fn from(value: NewSessionError) -> Self {
        Self::NewSession(value.to_string())
    }
}

pub async fn create_session_no_ks(scyconf: &ScyllaIngestConfig) -> Result<Arc<Session>, Error> {
    let profile = ExecutionProfileBuilder::default()
        .consistency(Consistency::LocalOne)
        .build()
        .into_handle();
    let scy = scylla::transport::session_builder::GenericSessionBuilder::new()
        .pool_size(scylla::transport::session::PoolSize::PerShard(
            NonZeroUsize::new(1).unwrap(),
        ))
        .known_nodes(scyconf.hosts())
        .default_execution_profile_handle(profile)
        .write_coalescing(true)
        .build()
        .await?;
    let scy = Arc::new(scy);
    Ok(scy)
}

pub async fn create_session(scyconf: &ScyllaIngestConfig) -> Result<Arc<Session>, Error> {
    let scy = create_session_no_ks(scyconf).await?;
    scy.use_keyspace(scyconf.keyspace(), true)
        .await
        .map_err(|e| Error::NewSession(e.to_string()))?;
    Ok(scy)
}
