use crate::err::Error;
use log::*;
use netpod::Database;
use taskrun::tokio;
use tokio::task::JoinHandle;
use tokio_postgres::Client;

pub type PgClient = Client;

pub async fn make_pg_client(dbconf: &Database) -> Result<(PgClient, JoinHandle<Result<(), Error>>), Error> {
    let d = dbconf;
    let url = format!("postgresql://{}:{}@{}:{}/{}", d.user, d.pass, d.host, d.port, d.name);
    info!("connect to {url}");
    let (client, pg_conn) = tokio_postgres::connect(&url, tokio_postgres::tls::NoTls).await?;
    // TODO allow clean shutdown on ctrl-c and join the pg_conn in the end:
    let jh = tokio::spawn(async move {
        pg_conn.await?;
        Ok(())
    });
    Ok((client, jh))
}
