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
use scylla::transport::iterator::NextRowError;
use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

#[derive(Debug, ThisError)]
#[cstm(name = "ScyllaSchema")]
pub enum Error {
    NoKeyspaceChosen,
    Fmt(#[from] fmt::Error),
    Query(#[from] QueryError),
    NewSession(String),
    ScyllaNextRow(#[from] NextRowError),
    MissingData,
    AddColumnImpossible,
    BadSchema,
}

impl From<crate::session::Error> for Error {
    fn from(value: crate::session::Error) -> Self {
        match value {
            crate::session::Error::NewSession(x) => Self::NewSession(x),
        }
    }
}

struct Changeset {
    do_change: bool,
    would_do: Vec<String>,
    done: Vec<String>,
}

impl Changeset {
    fn new() -> Self {
        Self {
            do_change: false,
            would_do: Vec::new(),
            done: Vec::new(),
        }
    }

    fn with_do_change(self, do_change: bool) -> Self {
        let mut x = self;
        x.do_change = do_change;
        x
    }

    fn do_change(&self) -> bool {
        self.do_change
    }

    fn add_would_do(&mut self, cql: String) {
        self.would_do.push(cql);
    }

    fn add_done(&mut self, cql: String) {
        self.done.push(cql);
    }

    fn differs(&self) -> bool {
        if self.would_do.len() != 0 {
            true
        } else {
            false
        }
    }

    fn log_statements(&self) {
        for q in &self.done {
            info!("DONE      {q}");
        }
        for q in &self.would_do {
            info!("WOULD DO  {q}");
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
    match scy.query_unpaged(format!("select * from {} limit 1", name), ()).await {
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

#[allow(unused)]
fn dhours(x: u64) -> Duration {
    Duration::from_secs(60 * 60 * x)
}

#[allow(unused)]
fn ddays(x: u64) -> Duration {
    Duration::from_secs(60 * 60 * 24 * x)
}

struct GenTwcsTab {
    keyspace: String,
    name: String,
    col_names: Vec<String>,
    col_types: Vec<String>,
    partition_keys: Vec<String>,
    cluster_keys: Vec<String>,
    default_time_to_live: Duration,
    compaction_window_size: Duration,
    gc_grace: Duration,
}

impl GenTwcsTab {
    pub fn new<'a, KS, PRE, N, CI, A, B, I2, I2A, I3, I3A>(
        keyspace: KS,
        pre: PRE,
        name: N,
        cols: CI,
        partition_keys: I2,
        cluster_keys: I3,
        default_time_to_live: Duration,
    ) -> Self
    where
        KS: Into<String>,
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
            keyspace.into(),
            pre.as_ref(),
            name.as_ref(),
            cols,
            partition_keys,
            cluster_keys,
            default_time_to_live,
            default_time_to_live / 40,
        )
    }

    fn new_inner<'a, CI, A, B, I2, I2A, I3, I3A>(
        keyspace: String,
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
            keyspace,
            name: format!("{}{}", pre, name),
            col_names,
            col_types,
            partition_keys: partition_keys.into_iter().map(Into::into).collect(),
            cluster_keys: cluster_keys.into_iter().map(Into::into).collect(),
            default_time_to_live,
            compaction_window_size,
            gc_grace: Duration::from_secs(60 * 60 * 12),
        }
    }

    fn keyspace(&self) -> &str {
        &self.keyspace
    }

    fn name(&self) -> &str {
        &self.name
    }

    async fn setup(&self, chs: &mut Changeset, scy: &ScySession) -> Result<(), Error> {
        self.create_if_missing(chs, scy).await?;
        self.check_table_options(chs, scy).await?;
        self.check_columns(chs, scy).await?;
        Ok(())
    }

    async fn create_if_missing(&self, chs: &mut Changeset, scy: &ScySession) -> Result<(), Error> {
        // TODO check for more details (all columns, correct types, correct kinds, etc)
        if !has_table(self.name(), scy).await? {
            let cql = self.cql();
            if chs.do_change() {
                info!("scylla create table {}  {}", self.name(), cql);
                scy.query_unpaged(cql.clone(), ()).await?;
                chs.add_done(cql);
            } else {
                chs.add_would_do(cql);
            }
        }
        Ok(())
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
        s.write_str(" and compaction = { ").unwrap();
        write!(
            s,
            concat!(
                "'class': 'TimeWindowCompactionStrategy'",
                ", 'compaction_window_unit': 'MINUTES'",
                ", 'compaction_window_size': {}",
            ),
            self.compaction_window_size.as_secs() / 60
        )
        .unwrap();
        s.write_str(" }").unwrap();
        s
    }

    fn compaction_options(&self) -> BTreeMap<String, String> {
        let win_mins = self.compaction_window_size.as_secs() / 60;
        let mut map = BTreeMap::new();
        map.insert("class".into(), "TimeWindowCompactionStrategy".into());
        map.insert("compaction_window_unit".into(), "MINUTES".into());
        map.insert("compaction_window_size".into(), win_mins.to_string());
        map
    }

    async fn check_table_options(&self, chs: &mut Changeset, scy: &ScySession) -> Result<(), Error> {
        let cql = concat!(
            "select default_time_to_live, gc_grace_seconds, compaction",
            " from system_schema.tables where keyspace_name = ? and table_name = ?"
        );
        let x = scy.query_iter(cql, (self.keyspace(), self.name())).await?;
        let mut it = x.into_typed::<(i32, i32, BTreeMap<String, String>)>();
        let mut rows = Vec::new();
        while let Some(u) = it.next().await {
            let row = u?;
            rows.push((row.0 as u64, row.1 as u64, row.2));
        }
        if let Some(row) = rows.get(0) {
            let mut set_opts = Vec::new();
            if row.0 != self.default_time_to_live.as_secs() {
                set_opts.push(format!(
                    "default_time_to_live = {}",
                    self.default_time_to_live.as_secs()
                ));
            }
            if row.1 != self.gc_grace.as_secs() {
                set_opts.push(format!("gc_grace_seconds = {}", self.gc_grace.as_secs()));
            }
            if row.2 != self.compaction_options() {
                let params: Vec<_> = self
                    .compaction_options()
                    .iter()
                    .map(|(k, v)| format!("'{k}': '{v}'"))
                    .collect();
                let params = params.join(", ");
                set_opts.push(format!("compaction = {{ {} }}", params));
            }
            if set_opts.len() != 0 {
                let cql = format!(concat!("alter table {} with {}"), self.name(), set_opts.join(" and "));
                if chs.do_change() {
                    info!("EXECUTE  {cql}");
                    scy.query_unpaged(cql.clone(), ()).await?;
                    chs.add_done(cql);
                } else {
                    chs.add_would_do(cql);
                }
            }
        } else {
            return Err(Error::MissingData);
        }
        Ok(())
    }

    async fn check_columns(&self, chs: &mut Changeset, scy: &ScySession) -> Result<(), Error> {
        let cql = concat!(
            "select column_name, type from system_schema.columns",
            " where keyspace_name = ?",
            " and table_name = ?",
        );
        let mut it = scy
            .query_iter(cql, (self.keyspace(), self.name()))
            .await?
            .into_typed::<(String, String)>();
        let mut names_exist = Vec::new();
        let mut types_exist = Vec::new();
        while let Some(x) = it.next().await {
            let row = x?;
            names_exist.push(row.0);
            types_exist.push(row.1);
        }
        debug!("names_exist {:?}  types_exist {:?}", names_exist, types_exist);
        for (cn, ct) in self.col_names.iter().zip(self.col_types.iter()) {
            if names_exist.contains(cn) {
                let i = names_exist.binary_search(cn).unwrap();
                let ty2 = types_exist.get(i).unwrap();
                if ct != ty2 {
                    error!(
                        "type mismatch for existing column  {}  {}  {}  {}",
                        self.name(),
                        cn,
                        ct,
                        ty2
                    );
                    return Err(Error::AddColumnImpossible);
                }
            } else {
                if self.partition_keys.contains(cn) {
                    error!("pk {} {}", cn, ct);
                    return Err(Error::AddColumnImpossible);
                }
                if self.cluster_keys.contains(cn) {
                    error!("ck {} {}", cn, ct);
                    return Err(Error::AddColumnImpossible);
                }
                self.add_column(cn, ct, chs, scy).await?;
            }
        }
        Ok(())
    }

    async fn add_column(&self, name: &str, ty: &str, chs: &mut Changeset, scy: &ScySession) -> Result<(), Error> {
        let cql = format!(concat!("alter table {} add {} {}"), self.name(), name, ty);
        if chs.do_change() {
            info!("EXECUTE  add_column  CQL {}", cql);
            scy.query_unpaged(cql.clone(), ()).await?;
            chs.add_done(cql);
        } else {
            chs.add_would_do(cql);
        }
        Ok(())
    }
}

#[allow(unused)]
async fn get_columns(keyspace: &str, table: &str, scy: &ScySession) -> Result<Vec<String>, Error> {
    let mut ret = Vec::new();
    // kind (text) can be one of: "regular", "clustering", "partition_key".
    // clustering_order (text) can be one of: "NONE", "ASC", "DESC".
    // type (text) examples: "bigint", "frozen<list<float>>", etc.
    let cql = concat!(
        "select column_name, clustering_order, kind, position, type",
        " from system_schema.columns where keyspace_name = ? and table_name = ?"
    );
    let mut it = scy
        .query_iter(cql, (keyspace, table))
        .await?
        .into_typed::<(String, String, String, i32, String)>();
    while let Some(x) = it.next().await {
        let row = x?;
        // columns:
        // column_name (text)
        // type (text): text, blob, int, ...
        ret.push(row.0);
    }
    Ok(ret)
}

async fn check_event_tables(
    keyspace: &str,
    rett: RetentionTime,
    chs: &mut Changeset,
    scy: &ScySession,
) -> Result<(), Error> {
    let stys = [
        "u8", "u16", "u32", "u64", "i8", "i16", "i32", "i64", "f32", "f64", "bool", "string",
    ];
    let cqlstys = [
        "tinyint", "smallint", "int", "bigint", "tinyint", "smallint", "int", "bigint", "float", "double", "boolean",
        "text",
    ];
    for (sty, cqlsty) in stys.into_iter().zip(cqlstys) {
        {
            let tab = GenTwcsTab::new(
                keyspace,
                rett.table_prefix(),
                format!("events_scalar_{}", sty),
                &[
                    ("series", "bigint"),
                    ("ts_msp", "bigint"),
                    ("ts_lsp", "bigint"),
                    ("pulse", "bigint"),
                    ("value", cqlsty),
                    ("ts_alt_1", "bigint"),
                ],
                ["series", "ts_msp"],
                ["ts_lsp"],
                rett.ttl_events_d0(),
            );
            tab.setup(chs, scy).await?;
        }
        {
            let tab = GenTwcsTab::new(
                keyspace,
                rett.table_prefix(),
                format!("events_array_{}", sty),
                &[
                    ("series", "bigint"),
                    ("ts_msp", "bigint"),
                    ("ts_lsp", "bigint"),
                    ("pulse", "bigint"),
                    ("value", &format!("frozen<list<{}>>", cqlsty)),
                    ("valueblob", "blob"),
                    ("ts_alt_1", "bigint"),
                ],
                ["series", "ts_msp"],
                ["ts_lsp"],
                rett.ttl_events_d1(),
            );
            tab.setup(chs, scy).await?;
        }
    }
    {
        let tab = GenTwcsTab::new(
            keyspace,
            rett.table_prefix(),
            format!("events_scalar_enum"),
            &[
                ("series", "bigint"),
                ("ts_msp", "bigint"),
                ("ts_lsp", "bigint"),
                ("value", "smallint"),
                ("valuestr", "text"),
            ],
            ["series", "ts_msp"],
            ["ts_lsp"],
            rett.ttl_events_d1(),
        );
        tab.setup(chs, scy).await?;
    }
    {
        let tab = GenTwcsTab::new(
            keyspace,
            rett.table_prefix(),
            format!("events_scalar_castatus"),
            &[
                ("series", "bigint"),
                ("ts_msp", "bigint"),
                ("ts_lsp", "bigint"),
                ("value", "smallint"),
            ],
            ["series", "ts_msp"],
            ["ts_lsp"],
            rett.ttl_events_d1(),
        );
        tab.setup(chs, scy).await?;
    }
    {
        let tab = GenTwcsTab::new(
            keyspace,
            rett.table_prefix(),
            format!("events_scalar_caseverity"),
            &[
                ("series", "bigint"),
                ("ts_msp", "bigint"),
                ("ts_lsp", "bigint"),
                ("value", "smallint"),
            ],
            ["series", "ts_msp"],
            ["ts_lsp"],
            rett.ttl_events_d1(),
        );
        tab.setup(chs, scy).await?;
    }
    Ok(())
}

pub async fn migrate_scylla_data_schema(
    scyconf: &ScyllaIngestConfig,
    rett: RetentionTime,
    do_change: bool,
) -> Result<(), Error> {
    let mut chsv = Changeset::new().with_do_change(do_change);
    let chs = &mut chsv;
    let scy2 = create_session_no_ks(scyconf).await?;
    let scy = &scy2;
    let durable = true;

    if !has_keyspace(scyconf.keyspace(), scy).await? {
        if chs.do_change() {
            // TODO
            let replication = 3;
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
        } else {
            error!("missing keyspace  {:?}", scyconf.keyspace());
            return Err(Error::BadSchema);
        }
    }

    if let Some(ks) = scyconf.keyspace_rf1() {
        if !has_keyspace(ks, scy).await? {
            if chs.do_change() {
                let replication = 1;
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
            } else {
                error!("missing keyspace  {:?}", scyconf.keyspace_rf1());
                return Err(Error::BadSchema);
            }
        }
    }

    let ks = scyconf.keyspace();

    scy.use_keyspace(ks, true).await?;

    check_event_tables(ks, rett.clone(), chs, scy).await?;

    {
        let tab = GenTwcsTab::new(
            ks,
            rett.table_prefix(),
            "ts_msp",
            &[("series", "bigint"), ("ts_msp", "bigint")],
            ["series"],
            ["ts_msp"],
            rett.ttl_ts_msp(),
        );
        tab.setup(chs, scy).await?;
    }
    {
        let tab = GenTwcsTab::new(
            ks,
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
            rett.ttl_channel_status(),
        );
        tab.setup(chs, scy).await?;
    }
    {
        let tab = GenTwcsTab::new(
            ks,
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
            rett.ttl_channel_status(),
        );
        tab.setup(chs, scy).await?;
    }
    {
        let tab = GenTwcsTab::new(
            ks,
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
            rett.ttl_channel_status(),
        );
        tab.setup(chs, scy).await?;
    }
    {
        let tab = GenTwcsTab::new(
            ks,
            rett.table_prefix(),
            "binned_scalar_f32_v02",
            &[
                ("series", "bigint"),
                ("binlen", "int"),
                ("msp", "bigint"),
                ("off", "int"),
                ("cnt", "bigint"),
                ("min", "float"),
                ("max", "float"),
                ("avg", "float"),
                ("dev", "float"),
                ("lst", "float"),
            ],
            ["series", "binlen", "msp"],
            ["off"],
            rett.ttl_binned(),
        );
        tab.setup(chs, scy).await?;
    }
    {
        let tab = GenTwcsTab::new(
            ks,
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
            rett.ttl_channel_status(),
        );
        tab.setup(chs, scy).await?;
    }
    {
        let tab = GenTwcsTab::new(
            ks,
            rett.table_prefix(),
            "account_recv_00",
            &[
                ("part", "int"),
                ("ts", "bigint"),
                ("series", "bigint"),
                ("count", "bigint"),
                ("bytes", "bigint"),
            ],
            ["part", "ts"],
            ["series"],
            rett.ttl_channel_status(),
        );
        tab.setup(chs, scy).await?;
    }

    if chs.differs() {
        chs.log_statements();
        Err(Error::BadSchema)
    } else {
        Ok(())
    }
}
