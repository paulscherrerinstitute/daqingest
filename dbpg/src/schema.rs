use crate::conn::PgClient;
use err::thiserror;
use err::ThisError;
use log::*;

#[derive(Debug, ThisError)]
pub enum Error {
    Postgres(#[from] tokio_postgres::Error),
    LogicError(String),
}

impl Error {
    pub fn from_logic_msg<T>(msg: T) -> Self
    where
        T: Into<String>,
    {
        Self::LogicError(msg.into())
    }
}

async fn has_table(table: &str, pgc: &PgClient) -> Result<bool, Error> {
    let rows = pgc
        .query(
            "select count(*) as c from information_schema.tables where table_name = $1 and table_type = 'BASE TABLE' limit 10",
            &[&table],
        )
        .await?;
    if rows.len() == 1 {
        let c: i64 = rows[0].get(0);
        if c == 0 {
            Ok(false)
        } else if c == 1 {
            Ok(true)
        } else {
            Err(Error::from_logic_msg(format!("has_table bad count {}", c)))
        }
    } else {
        Err(Error::from_logic_msg(format!(
            "has_columns bad row count {}",
            rows.len()
        )))
    }
}

async fn has_column(table: &str, column: &str, pgc: &PgClient) -> Result<bool, Error> {
    let rows = pgc
        .query(
            "select count(*) as c from information_schema.columns where table_name = $1 and column_name = $2 limit 10",
            &[&table, &column],
        )
        .await?;
    if rows.len() == 1 {
        let c: i64 = rows[0].get(0);
        if c == 0 {
            Ok(false)
        } else if c == 1 {
            Ok(true)
        } else {
            Err(Error::from_logic_msg(format!("has_column bad count {}", c)))
        }
    } else {
        Err(Error::from_logic_msg(format!(
            "has_column bad row count {}",
            rows.len()
        )))
    }
}

async fn migrate_00(pgc: &PgClient) -> Result<(), Error> {
    let sql = "
create table if not exists series_by_channel (
    series bigint not null primary key,
    facility text not null,
    channel text not null,
    scalar_type int not null,
    shape_dims int[] not null,
    agg_kind int not null,
    tscreate timestamptz not null default now()
)";
    let _ = pgc.execute(sql, &[]).await;

    let sql = "alter table series_by_channel add tscreate timestamptz not null default now()";
    let _ = pgc.execute(sql, &[]).await;

    if !has_table("ioc_by_channel_log", pgc).await? {
        let _ = pgc
            .execute(
                "
create table if not exists ioc_by_channel_log (
    facility text not null,
    channel text not null,
    tscreate timestamptz not null default now(),
    tsmod timestamptz not null default now(),
    archived int not null default 0,
    queryaddr text,
    responseaddr text,
    addr text
)
",
                &[],
            )
            .await;
        let _ = pgc
            .execute(
                "
create index if not exists ioc_by_channel_log_channel on ioc_by_channel_log (
    facility,
    channel
)
",
                &[],
            )
            .await;
    }
    Ok(())
}

async fn migrate_01(pgc: &PgClient) -> Result<(), Error> {
    if !has_column("ioc_by_channel_log", "tscreate", pgc).await? {
        pgc.execute(
            "alter table ioc_by_channel_log add tscreate timestamptz not null default now()",
            &[],
        )
        .await?;
    }
    if !has_column("ioc_by_channel_log", "archived", pgc).await? {
        pgc.execute(
            "alter table ioc_by_channel_log add archived int not null default 0",
            &[],
        )
        .await?;
    }
    if !has_column("ioc_by_channel_log", "modcount", pgc).await? {
        pgc.execute(
            "alter table ioc_by_channel_log add modcount int not null default 0",
            &[],
        )
        .await?;
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

async fn migrate_02(pgc: &PgClient) -> Result<(), Error> {
    // TODO after all migrations, should check that the schema is as expected.
    let sql = "alter table series_by_channel add tscs timestamptz[] default array[now()]";
    let _ = pgc.execute(sql, &[]).await;
    Ok(())
}

pub async fn schema_check(pgc: &PgClient) -> Result<(), Error> {
    pgc.execute("set client_min_messages = 'warning'", &[]).await?;
    migrate_00(&pgc).await?;
    migrate_01(&pgc).await?;
    migrate_02(&pgc).await?;
    pgc.execute("reset client_min_messages", &[]).await?;
    info!("schema_check done");
    Ok(())
}
