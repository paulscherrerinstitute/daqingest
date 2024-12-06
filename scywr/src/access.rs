use scylla::transport::errors::DbError;
use scylla::transport::errors::QueryError;

autoerr::create_error_v1!(
    name(Error, "ScyllaAccess"),
    enum variants {
        DbError(#[from] DbError),
        QueryError(#[from] QueryError),
        NoKeyspaceChosen,
    },
);
