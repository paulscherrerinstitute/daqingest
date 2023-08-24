use crate::access::Error;
use crate::session::ScySession;
use futures_util::StreamExt;
// use netpod::ScyllaConfig;

pub async fn has_table(name: &str, scy: &ScySession) -> Result<bool, Error> {
    let ks = scy.get_keyspace().ok_or_else(|| Error::NoKeyspaceChosen)?;
    let mut res = scy
        .query_iter(
            "select table_name from system_schema.tables where keyspace_name = ?",
            (ks.as_ref(),),
        )
        .await?;
    while let Some(k) = res.next().await {
        let row = k?;
        if let Some(table_name) = row.columns[0].as_ref().unwrap().as_text() {
            if table_name == name {
                return Ok(true);
            }
        }
    }
    Ok(false)
}
