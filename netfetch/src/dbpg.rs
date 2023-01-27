use crate::errconv::ErrConv;
use err::Error;
use netpod::log::*;
use netpod::Database;
use tokio_postgres::Client as PgClient;

pub async fn make_pg_client(d: &Database) -> Result<PgClient, Error> {
    let url = format!("postgresql://{}:{}@{}:{}/{}", d.user, d.pass, d.host, d.port, d.name);
    info!("connect to {url}");
    let (client, pg_conn) = tokio_postgres::connect(&url, tokio_postgres::tls::NoTls)
        .await
        .err_conv()?;
    // TODO allow clean shutdown on ctrl-c and join the pg_conn in the end:
    tokio::spawn(pg_conn);
    Ok(client)
}

async fn has_column(table: &str, column: &str, pgc: &PgClient) -> Result<bool, Error> {
    let rows = pgc
        .query(
            "select count(*) as c from information_schema.columns where table_name = $1 and column_name = $2 limit 10",
            &[&table, &column],
        )
        .await
        .err_conv()?;
    if rows.len() == 1 {
        let c: i64 = rows[0].get(0);
        if c == 0 {
            Ok(false)
        } else if c == 1 {
            Ok(true)
        } else {
            Err(Error::with_msg_no_trace(format!("has_columns bad count {}", c)))
        }
    } else if rows.len() == 0 {
        Ok(false)
    } else {
        Err(Error::with_msg_no_trace(format!(
            "has_columns bad row count {}",
            rows.len()
        )))
    }
}

async fn migrate_00(pgc: &PgClient) -> Result<(), Error> {
    if !has_column("ioc_by_channel_log", "tscreate", pgc).await? {
        pgc.execute(
            "alter table ioc_by_channel_log add tscreate timestamptz not null default now()",
            &[],
        )
        .await
        .err_conv()?;
    }
    if !has_column("ioc_by_channel_log", "archived", pgc).await? {
        pgc.execute(
            "alter table ioc_by_channel_log add archived int not null default 0",
            &[],
        )
        .await
        .err_conv()?;
    }
    {
        match pgc.execute("alter table series_by_channel add constraint series_by_channel_nondup unique (facility, channel, scalar_type, shape_dims, agg_kind)", &[]).await {
            Ok(_) => {
                info!("constraint added");
            }
            Err(_)=>{}
        }
    }
    Ok(())
}

pub async fn schema_check(db: &Database) -> Result<(), Error> {
    let pgc = make_pg_client(db).await?;
    migrate_00(&pgc).await?;
    info!("schema_check done");
    Ok(())
}
