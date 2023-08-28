use err::thiserror;
use err::ThisError;

#[derive(Debug, ThisError)]
pub enum Error {
    Postgres(#[from] tokio_postgres::Error),
}
