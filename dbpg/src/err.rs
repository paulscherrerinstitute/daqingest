autoerr::create_error_v1!(
    name(Error, "Postgres"),
    enum variants {
        Postgres(#[from] tokio_postgres::Error),
    },
);
