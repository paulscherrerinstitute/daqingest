use crate::config::ScyllaIngestConfig;
use crate::session::create_session_no_ks;
use crate::session::ScySession;
use err::thiserror;
use err::ThisError;
use futures_util::StreamExt;
use log::*;
use netpod::ttl::RetentionTime;
use scylla::transport::errors::DbError;
use scylla::transport::errors::QueryError;
use std::fmt;
use std::time::Duration;

#[derive(Debug, ThisError)]
pub enum Error {
    NoKeyspaceChosen,
    Fmt(#[from] fmt::Error),
    Query(#[from] QueryError),
    NewSession(String),
}

impl From<crate::session::Error> for Error {
    fn from(value: crate::session::Error) -> Self {
        match value {
            crate::session::Error::NewSession(x) => Self::NewSession(x),
        }
    }
}

pub async fn has_keyspace(name: &str, scy: &ScySession) -> Result<bool, Error> {
    let cql = "select keyspace_name from system_schema.keyspaces where keyspace_name = ?";
    let mut res = scy.query_iter(cql, (name,)).await?;
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

pub async fn has_table(name: &str, scy: &ScySession) -> Result<bool, Error> {
    let cql = "select table_name from system_schema.tables where keyspace_name = ?";
    let ks = scy.get_keyspace().ok_or_else(|| Error::NoKeyspaceChosen)?;
    let mut res = scy.query_iter(cql, (ks.as_ref(),)).await?;
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

pub async fn check_table_readable(name: &str, scy: &ScySession) -> Result<bool, Error> {
    match scy.query(format!("select * from {} limit 1", name), ()).await {
        Ok(_) => Ok(true),
        Err(e) => match &e {
            QueryError::DbError(e2, msg) => match e2 {
                DbError::Invalid => {
                    if msg.contains("unconfigured table") {
                        Ok(false)
                    } else {
                        Err(e.into())
                    }
                }
                _ => Err(e.into()),
            },
            _ => Err(e.into()),
        },
    }
}

pub async fn create_table_ts_msp(table_name: &str, scy: &ScySession) -> Result<(), Error> {
    use std::fmt::Write;
    // seconds:
    let default_time_to_live = 60 * 60 * 5;
    // hours:
    let twcs_window_index = 24 * 4;
    let mut s = String::new();
    s.write_str("create table ")?;
    s.write_str(table_name)?;
    s.write_str(" (series bigint, ts_msp bigint, primary key (series, ts_msp))")?;
    write!(s, " with default_time_to_live = {}", default_time_to_live)?;
    s.write_str(" and compaction = { 'class': 'TimeWindowCompactionStrategy'")?;
    s.write_str(", 'compaction_window_unit': 'HOURS'")?;
    write!(s, ", 'compaction_window_size': {}", twcs_window_index)?;
    s.write_str(" }")?;
    eprintln!("create table cql  {s}");
    scy.query(s, ()).await?;
    Ok(())
}

fn dhours(x: u64) -> Duration {
    Duration::from_secs(60 * 60 * x)
}

fn ddays(x: u64) -> Duration {
    Duration::from_secs(60 * 60 * 24 * x)
}

struct GenTwcsTab {
    name: String,
    col_names: Vec<String>,
    col_types: Vec<String>,
    partition_keys: Vec<String>,
    cluster_keys: Vec<String>,
    default_time_to_live: Duration,
    compaction_window_size: Duration,
}

impl GenTwcsTab {
    // name: "series_by_ts_msp".into(),
    // cql: "(part int, ts_msp int, shape_kind int, scalar_type int, series bigint, primary key ((part, ts_msp, shape_kind, scalar_type), series))".into(),
    // default_time_to_live: 60 * 60 * 5,
    // compaction_window_size: 24 * 4,
    pub fn new<'a, PRE, N, CI, A, B, I2, I2A, I3, I3A>(
        pre: PRE,
        name: N,
        cols: CI,
        partition_keys: I2,
        cluster_keys: I3,
        default_time_to_live: Duration,
        compaction_window_size: Duration,
    ) -> Self
    where
        PRE: AsRef<str>,
        N: AsRef<str>,
        CI: IntoIterator<Item = &'a (A, B)>,
        // TODO could make for idiomatic to skip extra clone if passed value is already String
        A: AsRef<str> + 'a,
        B: AsRef<str> + 'a,
        I2: IntoIterator<Item = I2A>,
        I3: IntoIterator<Item = I3A>,
        I2A: Into<String>,
        I3A: Into<String>,
    {
        Self::new_inner(
            pre.as_ref(),
            name.as_ref(),
            cols,
            partition_keys,
            cluster_keys,
            default_time_to_live,
            compaction_window_size,
        )
    }

    fn new_inner<'a, CI, A, B, I2, I2A, I3, I3A>(
        pre: &str,
        name: &str,
        cols: CI,
        partition_keys: I2,
        cluster_keys: I3,
        default_time_to_live: Duration,
        compaction_window_size: Duration,
    ) -> Self
    where
        CI: IntoIterator<Item = &'a (A, B)>,
        A: AsRef<str> + 'a,
        B: AsRef<str> + 'a,
        I2: IntoIterator<Item = I2A>,
        I3: IntoIterator<Item = I3A>,
        I2A: Into<String>,
        I3A: Into<String>,
    {
        let mut col_names = Vec::new();
        let mut col_types = Vec::new();
        cols.into_iter().for_each(|(a, b)| {
            col_names.push(a.as_ref().into());
            col_types.push(b.as_ref().into());
        });
        Self {
            name: format!("{}{}", pre, name),
            col_names,
            col_types,
            partition_keys: partition_keys.into_iter().map(Into::into).collect(),
            cluster_keys: cluster_keys.into_iter().map(Into::into).collect(),
            default_time_to_live,
            compaction_window_size,
        }
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn cql(&self) -> String {
        use std::fmt::Write;
        let pkey = if self.partition_keys.len() == 0 {
            panic!("some partition key is required");
        } else {
            self.partition_keys.join(", ")
        };
        let pkeys = format!("({})", pkey);
        let pkeys = if self.cluster_keys.len() == 0 {
            format!("({})", pkeys)
        } else {
            format!("({}, {})", pkeys, self.cluster_keys.join(", "))
        };
        let mut s = String::new();
        write!(s, "create table {}", self.name()).unwrap();
        let mut cols: Vec<_> = self
            .col_names
            .iter()
            .zip(self.col_types.iter())
            .map(|(n, t)| format!("{} {}", n, t))
            .collect();
        cols.push(format!("primary key {pkeys}"));
        let cols = cols.join(", ");
        write!(s, " ({})", cols).unwrap();
        write!(
            s,
            " with default_time_to_live = {}",
            self.default_time_to_live.as_secs()
        )
        .unwrap();
        s.write_str(" and compaction = { 'class': 'TimeWindowCompactionStrategy', 'compaction_window_unit': 'HOURS'")
            .unwrap();
        write!(
            s,
            ", 'compaction_window_size': {}",
            self.compaction_window_size.as_secs() / 60 / 60
        )
        .unwrap();
        s.write_str(" }").unwrap();
        s
    }

    async fn create_if_missing(&self, scy: &ScySession) -> Result<(), Error> {
        // TODO check for more details (all columns, correct types, correct kinds, etc)
        if !has_table(self.name(), scy).await? {
            let cql = self.cql();
            info!("scylla create table {}  {}", self.name(), cql);
            scy.query(cql, ()).await?;
        }
        Ok(())
    }
}

fn table_param_compaction(compaction_window_size: Duration) -> String {
    table_param_compaction_twcs(compaction_window_size)
}

#[allow(unused)]
fn table_param_compaction_stcs() -> String {
    format!(concat!(
        "{{ 'class': 'SizeTieredCompactionStrategy'",
        // ", 'min_sstable_size': 200",
        // ", 'max_threshold': 10",
        " }}"
    ))
}

#[allow(unused)]
fn table_param_compaction_twcs(compaction_window_size: Duration) -> String {
    format!(
        concat!(
            "{{ 'class': 'TimeWindowCompactionStrategy'",
            ", 'compaction_window_unit': 'HOURS'",
            ", 'compaction_window_size': {}",
            " }}"
        ),
        compaction_window_size.as_secs() / 60 / 60
    )
}

struct EvTabDim0 {
    pre: String,
    sty: String,
    cqlsty: String,
    // SCYLLA_TTL_EVENTS_DIM0
    default_time_to_live: Duration,
    // TWCS_WINDOW_0D
    compaction_window_size: Duration,
}

impl EvTabDim0 {
    fn name(&self) -> String {
        format!("{}events_scalar_{}", self.pre, self.sty)
    }

    fn cql_create(&self) -> String {
        use std::fmt::Write;
        let ttl = self.default_time_to_live.as_secs();
        let compaction = table_param_compaction(self.compaction_window_size);
        let mut s = String::new();
        write!(s, "create table {}", self.name()).unwrap();
        write!(s, " (series bigint, ts_msp bigint, ts_lsp bigint, pulse bigint, value {}, primary key ((series, ts_msp), ts_lsp))", self.cqlsty).unwrap();
        write!(s, " with default_time_to_live = {}", ttl).unwrap();
        write!(s, " and compaction = {}", compaction).unwrap();
        s
    }
}

struct EvTabDim1 {
    pre: String,
    sty: String,
    cqlsty: String,
    // SCYLLA_TTL_EVENTS_DIM1
    default_time_to_live: Duration,
    // TWCS_WINDOW_1D
    compaction_window_size: Duration,
}

impl EvTabDim1 {
    fn name(&self) -> String {
        format!("{}events_array_{}", self.pre, self.sty)
    }

    fn cql_create(&self) -> String {
        use std::fmt::Write;
        let mut s = String::new();
        let ttl = self.default_time_to_live.as_secs();
        let compaction = table_param_compaction(self.compaction_window_size);
        write!(s, "create table {}", self.name()).unwrap();
        write!(s, " (series bigint, ts_msp bigint, ts_lsp bigint, pulse bigint, value {}, primary key ((series, ts_msp), ts_lsp))", self.cqlsty).unwrap();
        write!(s, " with default_time_to_live = {}", ttl).unwrap();
        write!(s, " and compaction = {}", compaction).unwrap();
        s
    }
}

#[allow(unused)]
async fn get_columns(keyspace: &str, table: &str, scy: &ScySession) -> Result<Vec<String>, Error> {
    let mut ret = Vec::new();
    let cql = "select column_name, kind, type from system_schema.columns where keyspace_name = ? and table_name = ?";
    let params = (keyspace, table);
    let mut res = scy.query_iter(cql, params).await?;
    while let Some(row) = res.next().await {
        // columns:
        // kind (text): regular, clustering, partition_key.
        // column_name (text)
        // type (text): text, blob, int, ...
        let row = row?;
        let name = row.columns[0].as_ref().unwrap().as_text().unwrap();
        ret.push(name.into());
    }
    Ok(ret)
}

async fn check_event_tables(rett: RetentionTime, scy: &ScySession) -> Result<(), Error> {
    let stys = [
        "u8", "u16", "u32", "u64", "i8", "i16", "i32", "i64", "f32", "f64", "bool", "string",
    ];
    let cqlstys = [
        "tinyint", "smallint", "int", "bigint", "tinyint", "smallint", "int", "bigint", "float", "double", "boolean",
        "text",
    ];
    for (sty, cqlsty) in stys.into_iter().zip(cqlstys) {
        let desc = EvTabDim0 {
            pre: rett.table_prefix().into(),
            sty: sty.into(),
            cqlsty: cqlsty.into(),
            // ttl is set in actual data inserts
            default_time_to_live: dhours(1),
            compaction_window_size: dhours(48),
        };
        if !has_table(&desc.name(), scy).await? {
            info!("scylla create table {}", desc.name());
            scy.query(desc.cql_create(), ()).await?;
        }
        let desc = EvTabDim1 {
            pre: rett.table_prefix().into(),
            sty: sty.into(),
            cqlsty: format!("frozen<list<{}>>", cqlsty),
            // ttl is set in actual data inserts
            default_time_to_live: dhours(1),
            compaction_window_size: dhours(12),
        };
        if !has_table(&desc.name(), scy).await? {
            info!("scylla create table {}", desc.name());
            scy.query(desc.cql_create(), ()).await?;
        }
    }
    Ok(())
}

pub async fn migrate_scylla_data_schema(scyconf: &ScyllaIngestConfig, rett: RetentionTime) -> Result<(), Error> {
    let scy2 = create_session_no_ks(scyconf).await?;
    let scy = &scy2;

    if !has_keyspace(scyconf.keyspace(), scy).await? {
        let replication = 3;
        let durable = false;
        let cql = format!(
            concat!(
                "create keyspace {}",
                " with replication = {{ 'class': 'SimpleStrategy', 'replication_factor': {} }}",
                " and durable_writes = {};"
            ),
            scyconf.keyspace(),
            replication,
            durable
        );
        info!("scylla create keyspace  {cql}");
        scy.query_iter(cql, ()).await?;
        info!("keyspace created");
    }

    scy.use_keyspace(scyconf.keyspace(), true).await?;

    {
        let table_name = format!("{}ts_msp", rett.table_prefix());
        if !has_table(&table_name, &scy).await? {
            create_table_ts_msp(&table_name, scy).await?;
        }
    }
    check_event_tables(rett.clone(), scy).await?;

    {
        let tab = GenTwcsTab::new(
            rett.table_prefix(),
            "ts_msp_ms",
            &[("series", "bigint"), ("ts_msp_ms", "bigint")],
            ["series"],
            ["ts_msp_ms"],
            rett.ttl_ts_msp() / 40,
            rett.ttl_ts_msp(),
        );
        tab.create_if_missing(scy).await?;
    }
    {
        let tab = GenTwcsTab::new(
            rett.table_prefix(),
            "series_by_ts_msp",
            &[
                ("part", "int"),
                ("ts_msp", "int"),
                ("shape_kind", "int"),
                ("scalar_type", "int"),
                ("series", "bigint"),
            ],
            ["part", "ts_msp", "shape_kind", "scalar_type"],
            ["series"],
            dhours(5),
            ddays(4),
        );
        tab.create_if_missing(scy).await?;
    }
    {
        let tab = GenTwcsTab::new(
            rett.table_prefix(),
            "connection_status",
            &[
                ("ts_msp", "bigint"),
                ("ts_lsp", "bigint"),
                ("kind", "int"),
                ("addr", "text"),
            ],
            ["ts_msp"],
            ["ts_lsp"],
            dhours(1),
            ddays(4),
        );
        tab.create_if_missing(scy).await?;
    }
    {
        let tab = GenTwcsTab::new(
            rett.table_prefix(),
            "channel_status",
            &[
                ("series", "bigint"),
                ("ts_msp", "bigint"),
                ("ts_lsp", "bigint"),
                ("kind", "int"),
            ],
            ["series", "ts_msp"],
            ["ts_lsp"],
            dhours(1),
            ddays(4),
        );
        tab.create_if_missing(scy).await?;
    }
    {
        let tab = GenTwcsTab::new(
            rett.table_prefix(),
            "channel_status_by_ts_msp",
            &[
                ("ts_msp", "bigint"),
                ("ts_lsp", "bigint"),
                ("series", "bigint"),
                ("kind", "int"),
            ],
            ["ts_msp"],
            ["ts_lsp"],
            dhours(1),
            ddays(4),
        );
        tab.create_if_missing(scy).await?;
    }
    {
        let tab = GenTwcsTab::new(
            rett.table_prefix(),
            "binned_scalar_f32",
            &[
                ("series", "bigint"),
                ("bin_len_ms", "int"),
                ("ts_msp", "bigint"),
                ("off", "int"),
                ("count", "bigint"),
                ("min", "float"),
                ("max", "float"),
                ("avg", "float"),
            ],
            ["series", "bin_len_ms", "ts_msp"],
            ["off"],
            ddays(30),
            ddays(4),
        );
        tab.create_if_missing(scy).await?;
    }
    {
        let tab = GenTwcsTab::new(
            rett.table_prefix(),
            "account_00",
            &[
                ("part", "int"),
                ("ts", "bigint"),
                ("series", "bigint"),
                ("count", "bigint"),
                ("bytes", "bigint"),
            ],
            ["part", "ts"],
            ["series"],
            ddays(30),
            ddays(4),
        );
        tab.create_if_missing(scy).await?;
    }
    Ok(())
}
