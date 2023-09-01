use err::thiserror;
use err::ThisError;
use std::fmt;

#[derive(Debug)]
struct TestError {
    msg: String,
}

impl fmt::Display for TestError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}", self.msg)
    }
}

impl std::error::Error for TestError {}

#[derive(Debug, ThisError)]
enum Error {
    Postgres(#[from] tokio_postgres::Error),
    Dummy(#[from] TestError),
}

#[test]
fn err_msg_01() {
    let err: Error = TestError {
        msg: "some-error".into(),
    }
    .into();
    assert_eq!(err.to_string(), "Error::Dummy(some-error)")
}
