pub use Session as ScySession;
pub use scylla::client::session::Session;

use crate::config::ScyllaIngestConfig;
use scylla::client::PoolSize;
use scylla::client::execution_profile::ExecutionProfileBuilder;
use scylla::client::session_builder::GenericSessionBuilder;
use scylla::errors::NewSessionError;
use scylla::routing::ShardAwarePortRange;
use scylla::statement::Consistency;
use std::num::NonZero;
use std::ops::RangeInclusive;
use std::sync::Arc;

autoerr::create_error_v1!(
    name(Error, "ScyllaSession"),
    enum variants {
        NewSession(String),
    },
);

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
    let port_range: RangeInclusive<u16> = 32000u16..=36000u16;
    let _port_range = ShardAwarePortRange::new(port_range).unwrap();
    let scy = GenericSessionBuilder::new()
        // .disallow_shard_aware_port(true)
        // .shard_aware_local_port_range(port_range)
        // .pool_size(PoolSize::default())
        .pool_size(PoolSize::PerHost(NonZero::new(1).unwrap()))
        .known_nodes(scyconf.hosts())
        .default_execution_profile_handle(profile)
        .write_coalescing(true)
        .compression(None)
        // .compression(Some(scylla::frame::Compression::Snappy))
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
