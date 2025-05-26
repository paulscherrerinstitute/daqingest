use crate::conn::PgClient;
use log::*;

autoerr::create_error_v1!(
    name(Error, "PgSchema"),
    enum variants {
        Postgres(#[from] tokio_postgres::Error),
        LogicError(String),
    },
);

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
    kind int2 not null,
    scalar_type int not null,
    shape_dims int[] not null,
    agg_kind int not null,
    tscs timestamptz[] not null default array[now()]
)";
    pgc.execute(sql, &[]).await?;

    let sql = "alter table series_by_channel drop if exists tscreate";
    pgc.execute(sql, &[]).await?;

    if !has_table("ioc_by_channel_log", pgc).await? {
        let sql = "
create table if not exists ioc_by_channel_log (
    facility text not null,
    channel text not null,
    tscreate timestamptz not null default now(),
    tsmod timestamptz not null default now(),
    archived int not null default 0,
    queryaddr text,
    responseaddr text,
    addr text
)";
        let _ = pgc.execute(sql, &[]).await;
        let sql = "
create index if not exists ioc_by_channel_log_channel on ioc_by_channel_log (
    facility,
    channel
)
";
        let _ = pgc.execute(sql, &[]).await;
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
    Ok(())
}

async fn migrate_02(pgc: &PgClient) -> Result<(), Error> {
    // TODO after all migrations, should check that the schema is as expected.
    let sql = "alter table series_by_channel alter shape_dims set storage plain";
    pgc.execute(sql, &[]).await?;
    let sql = "alter table series_by_channel add if not exists tscs timestamptz[] not null default array[now()]";
    pgc.execute(sql, &[]).await?;
    let sql = "alter table series_by_channel alter tscs set storage plain";
    pgc.execute(sql, &[]).await?;
    let sql = "alter table series_by_channel alter tscs set not null";
    pgc.execute(sql, &[]).await?;

    let sql = concat!("alter table series_by_channel drop constraint if exists series_by_channel_nondup");
    let _ = pgc.execute(sql, &[]).await?;
    let sql = "alter table series_by_channel add if not exists kind int2 not null default 0";
    let _ = pgc.execute(sql, &[]).await?;
    let sql = "alter table series_by_channel alter kind drop default";
    let _ = pgc.execute(sql, &[]).await?;
    let sql = "update series_by_channel set kind = 1 where kind = 0 and scalar_type = 14";
    let _ = pgc.execute(sql, &[]).await?;
    let sql = "update series_by_channel set kind = 2 where kind = 0 and scalar_type >= 0 and scalar_type <= 13";
    let _ = pgc.execute(sql, &[]).await?;

    // TODO this can fail if exists, but must verify that it exists in proper form
    let sql = concat!(
        "alter table series_by_channel add constraint series_by_channel_nondup_02",
        " unique (facility, channel, kind, scalar_type, shape_dims, agg_kind)"
    );
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

#[allow(unused)]
fn ignore_does_not_exist<T>(x: Result<T, tokio_postgres::Error>) -> Result<(), tokio_postgres::Error> {
    match x {
        Ok(_) => Ok(()),
        // STYLE is there a better way instead of string comparison?
        Err(e) => {
            if e.to_string().contains("does not exist") {
                Ok(())
            } else {
                Err(e)
            }
        }
    }
}
