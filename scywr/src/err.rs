use scylla::transport::errors::DbError;
use scylla::transport::errors::QueryError;

#[derive(Debug)]
pub enum Error {
    DbUnavailable,
    DbOverload,
    DbTimeout,
    DbError(String),
}

impl From<Error> for err::Error {
    fn from(e: Error) -> Self {
        err::Error::with_msg_no_trace(format!("{e:?}"))
    }
}

pub trait IntoSimplerError {
    fn into_simpler(self) -> Error;
}

impl IntoSimplerError for QueryError {
    fn into_simpler(self) -> Error {
        let e = self;
        match e {
            QueryError::DbError(e, msg) => match e {
                DbError::Unavailable { .. } => Error::DbUnavailable,
                DbError::Overloaded => Error::DbOverload,
                DbError::IsBootstrapping => Error::DbUnavailable,
                DbError::ReadTimeout { .. } => Error::DbTimeout,
                DbError::WriteTimeout { .. } => Error::DbTimeout,
                _ => Error::DbError(format!("{e} {msg}")),
            },
            QueryError::BadQuery(e) => Error::DbError(e.to_string()),
            QueryError::IoError(e) => Error::DbError(e.to_string()),
            QueryError::ProtocolError(e) => Error::DbError(e.to_string()),
            QueryError::InvalidMessage(e) => Error::DbError(e.to_string()),
            QueryError::TimeoutError => Error::DbTimeout,
            QueryError::TooManyOrphanedStreamIds(e) => Error::DbError(e.to_string()),
            QueryError::UnableToAllocStreamId => Error::DbError(e.to_string()),
            QueryError::RequestTimeout(e) => Error::DbError(e.to_string()),
            QueryError::TranslationError(e) => Error::DbError(e.to_string()),
        }
    }
}

impl<T: IntoSimplerError> From<T> for Error {
    fn from(e: T) -> Self {
        e.into_simpler()
    }
}
