use err::thiserror;
use err::ThisError;

// TODO still needed?
#[derive(Debug, ThisError)]
pub enum Error {
    Postgres(#[from] tokio_postgres::Error),
    IocAddrNotFound,
    BadIdGenerated,
    CanNotInsertSeriesId,
}
