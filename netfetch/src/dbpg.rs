use crate::errconv::ErrConv;
use err::Error;
use netpod::Database;
use tokio_postgres::Client as PgClient;

pub async fn make_pg_client(d: &Database) -> Result<PgClient, Error> {
    let (client, pg_conn) = tokio_postgres::connect(
        &format!("postgresql://{}:{}@{}:{}/{}", d.user, d.pass, d.host, d.port, d.name),
        tokio_postgres::tls::NoTls,
    )
    .await
    .err_conv()?;
    // TODO allow clean shutdown on ctrl-c and join the pg_conn in the end:
    tokio::spawn(pg_conn);
    Ok(client)
}
