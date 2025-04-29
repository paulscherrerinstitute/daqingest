use scylla::errors::DbError;

autoerr::create_error_v1!(
    name(Error, "ScyllaAccess"),
    enum variants {
        DbError(#[from] DbError),
        Execution(#[from] scylla::errors::ExecutionError),
        NoKeyspaceChosen,
    },
);
