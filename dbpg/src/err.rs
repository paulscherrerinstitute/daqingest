use err::thiserror;
use err::ThisError;

#[derive(Debug)]
pub struct Msg(pub String);

#[derive(Debug, ThisError)]
pub enum Error {
    Postgres(#[from] tokio_postgres::Error),
    Msg(Msg),
}

impl Error {
    pub fn from_msg<T>(msg: T) -> Self
    where
        T: Into<String>,
    {
        Self::Msg(Msg(msg.into()))
    }
}
