pub use netpod::ScyllaConfig;
pub use scylla::Session;
pub use Session as ScySession;

use err::thiserror;
use err::ThisError;
use scylla::execution_profile::ExecutionProfileBuilder;
use scylla::statement::Consistency;
use scylla::transport::errors::NewSessionError;
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

pub async fn create_session(scyconf: &ScyllaConfig) -> Result<Arc<Session>, Error> {
    let scy = scylla::SessionBuilder::new()
        .known_nodes(&scyconf.hosts)
        .use_keyspace(&scyconf.keyspace, true)
        .default_execution_profile_handle(
            ExecutionProfileBuilder::default()
                .consistency(Consistency::LocalOne)
                .build()
                .into_handle(),
        )
        .build()
        .await?;
    let scy = Arc::new(scy);
    Ok(scy)
}
