use err::thiserror;
use err::ThisError;
use scylla::transport::errors::DbError;
use scylla::transport::errors::QueryError;

#[derive(Debug, ThisError)]
pub enum Error {
    DbError(#[from] DbError),
    QueryError(#[from] QueryError),
    NoKeyspaceChosen,
}
