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
            Err(Error::from_logic_msg(format!("has_columns bad count {}", c)))
        }
    } else if rows.len() == 0 {
        Ok(false)
    } else {
        Err(Error::from_logic_msg(format!(
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
        .await?;
    }
    if !has_column("ioc_by_channel_log", "archived", pgc).await? {
        pgc.execute(
            "alter table ioc_by_channel_log add archived int not null default 0",
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

pub async fn schema_check(pgc: &PgClient) -> Result<(), Error> {
    migrate_00(&pgc).await?;
    info!("schema_check done");
    Ok(())
}
