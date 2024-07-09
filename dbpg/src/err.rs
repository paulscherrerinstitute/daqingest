use err::thiserror;
use err::ThisError;

#[derive(Debug, ThisError)]
#[cstm(name = "Postgres")]
pub enum Error {
    Postgres(#[from] tokio_postgres::Error),
}
