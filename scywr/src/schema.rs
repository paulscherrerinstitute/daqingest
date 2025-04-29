use crate::config::ScyllaIngestConfig;
use crate::session::ScySession;
use crate::session::create_session_no_ks;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use log::*;
use netpod::ttl::RetentionTime;
use scylla::errors::NextRowError;
use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

autoerr::create_error_v1!(
    name(Error, "ScyllaSchema"),
    enum variants {
        NoKeyspaceChosen,
        Fmt(#[from] fmt::Error),
        NewSession(String),
        ScyllaExecution(#[from] scylla::errors::ExecutionError),
        ScyllaPagerExecution(#[from] scylla::errors::PagerExecutionError),
        ScyllaNextRow(#[from] NextRowError),
        ScyllaTypecheck(#[from] scylla::deserialize::TypeCheckError),
        MissingData,
        AddColumnExists(String, String, String),
        AddColumnPk(String, String, String, String),
        AddColumnCk(String, String, String, String),
        BadSchema,
    },
);

impl From<crate::session::Error> for Error {
    fn from(value: crate::session::Error) -> Self {
        match value {
            crate::session::Error::NewSession(x) => Self::NewSession(x),
        }
    }
}

struct Changeset {
    todo: Vec<String>,
}

impl Changeset {
    fn new() -> Self {
        Self { todo: Vec::new() }
    }

    fn add_todo(&mut self, cql: String) {
        self.todo.push(cql);
    }

    fn has_to_do(&self) -> bool {
        if self.todo.len() != 0 { true } else { false }
    }

    fn log_statements(&self) {
        for q in &self.todo {
            info!("would execute:\n{q}\n");
        }
    }
}

pub async fn has_keyspace(name: &str, scy: &ScySession) -> Result<bool, Error> {
    let cql = "select keyspace_name from system_schema.keyspaces where keyspace_name = ?";
    let mut res = scy.query_iter(cql, (name,)).await?.rows_stream::<(String,)>()?;
    while let Some((table_name,)) = res.try_next().await? {
        if table_name == name {
            return Ok(true);
        }
    }
    Ok(false)
}

pub async fn has_table(ks: &str, name: &str, scy: &ScySession) -> Result<bool, Error> {
    let cql = "select table_name from system_schema.tables where keyspace_name = ?";
    let mut res = scy.query_iter(cql, (ks,)).await?.rows_stream::<(String,)>()?;
    while let Some((table_name,)) = res.try_next().await? {
        if table_name == name {
            return Ok(true);
        }
    }
    Ok(false)
}

pub async fn check_table_readable(ks: &str, name: &str, scy: &ScySession) -> Result<bool, Error> {
    use scylla::errors::ExecutionError;
    match scy
        .query_unpaged(format!("select * from {}.{} limit 1", ks, name), ())
        .await
    {
        Ok(_) => Ok(true),
        Err(e) => match &e {
            ExecutionError::BadQuery(_) => Ok(false),
            ExecutionError::PrepareError(_) => Ok(false),
            ExecutionError::EmptyPlan => Err(Error::ScyllaExecution(e)),
            ExecutionError::ConnectionPoolError(_) => Err(Error::ScyllaExecution(e)),
            ExecutionError::LastAttemptError(_) => Err(Error::ScyllaExecution(e)),
            ExecutionError::RequestTimeout(_) => Err(Error::ScyllaExecution(e)),
            ExecutionError::UseKeyspaceError(_) => Err(Error::ScyllaExecution(e)),
            ExecutionError::SchemaAgreementError(_) => Err(Error::ScyllaExecution(e)),
            ExecutionError::MetadataError(_) => Err(Error::ScyllaExecution(e)),
            _ => Err(Error::ScyllaExecution(e)),
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
        if self.has_table_name(scy).await? {
            self.check_table_options(chs, scy).await?;
            self.check_columns(chs, scy).await?;
        } else {
            chs.add_todo(self.cql());
        }
        Ok(())
    }

    async fn has_table_name(&self, scy: &ScySession) -> Result<bool, Error> {
        has_table(self.keyspace(), self.name(), scy).await
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
        write!(s, "create table {}.{}", self.keyspace(), self.name()).unwrap();
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
            " with default_time_to_live = {} and gc_grace_seconds = {}",
            self.default_time_to_live.as_secs(),
            self.gc_grace.as_secs()
        )
        .unwrap();
        s.write_str(" and compaction = { ").unwrap();
        {
            let mut s2 = String::new();
            // TODO merge with builder code in check_table_options
            for e in self.compaction_options() {
                if s2.len() != 0 {
                    s2.push_str(", ");
                }
                let op = format!("'{}': '{}'", e.0, e.1);
                s2.push_str(&op);
            }
            s.write_str(&s2).unwrap();
        }
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
        let mut it = x.rows_stream::<(i32, i32, BTreeMap<String, String>)>()?;
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
                info!(
                    "compaction options differ  {:?}  vs  {:?}",
                    row.2,
                    self.compaction_options()
                );
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
                chs.add_todo(cql);
            }
        } else {
            chs.add_todo(self.cql());
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
            .rows_stream::<(String, String)>()?;
        let mut names_exist = Vec::new();
        let mut types_exist = Vec::new();
        while let Some((name, ty)) = it.try_next().await? {
            names_exist.push(name);
            types_exist.push(ty);
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
                    return Err(Error::AddColumnExists(cn.into(), ct.into(), ty2.into()));
                }
            } else {
                if self.partition_keys.contains(cn) {
                    error!("pk {} {}", cn, ct);
                    return Err(Error::AddColumnPk(
                        self.keyspace().into(),
                        self.name().into(),
                        cn.into(),
                        ct.into(),
                    ));
                }
                if self.cluster_keys.contains(cn) {
                    error!("ck {} {}", cn, ct);
                    return Err(Error::AddColumnCk(
                        self.keyspace().into(),
                        self.name().into(),
                        cn.into(),
                        ct.into(),
                    ));
                }
                self.add_column(cn, ct, chs).await?;
            }
        }
        Ok(())
    }

    async fn add_column(&self, name: &str, ty: &str, chs: &mut Changeset) -> Result<(), Error> {
        let cql = format!(concat!("alter table {} add {} {}"), self.name(), name, ty);
        chs.add_todo(cql);
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
        .rows_stream::<(String, String, String, i32, String)>()?;
    while let Some((name, ..)) = it.try_next().await? {
        // columns:
        // column_name (text)
        // type (text): text, blob, int, ...
        ret.push(name);
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
            rett.ttl_events_d0(),
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
            rett.ttl_events_d0(),
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
            rett.ttl_events_d0(),
        );
        tab.setup(chs, scy).await?;
    }
    Ok(())
}

async fn migrate_scylla_data_schema(
    scyconf: &ScyllaIngestConfig,
    rett: RetentionTime,
    rf: u8,
    chs: &mut Changeset,
) -> Result<(), Error> {
    let scy2 = create_session_no_ks(scyconf).await?;
    let scy = &scy2;
    let durable = true;
    let ks = scyconf.keyspace();

    if !has_keyspace(ks, scy).await? {
        let cql = format!(
            concat!(
                "create keyspace {}",
                " with replication = {{ 'class': 'SimpleStrategy', 'replication_factor': {} }}",
                " and durable_writes = {};"
            ),
            ks, rf, durable
        );
        info!("scylla create keyspace  {cql}");
        chs.add_todo(cql);
    } else {
        info!("scylla has keyspace  {ks}");
    }

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
            "bin_write_index_v03",
            &[
                ("series", "bigint"),
                ("pbp", "smallint"),
                ("msp", "int"),
                ("rt", "smallint"),
                ("lsp", "int"),
                ("binlen", "int"),
            ],
            ["series", "pbp", "msp"],
            ["rt", "lsp", "binlen"],
            rett.ttl_binned(),
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
    {
        let tn = format!("{}{}", rett.table_prefix(), "bin_write_index_v00");
        if has_table(ks, &tn, scy).await? {
            chs.add_todo(format!("drop table {}.{}", ks, tn));
        }
    }
    {
        let tn = format!("{}{}", rett.table_prefix(), "bin_write_index_v01");
        if has_table(&ks, &tn, scy).await? {
            chs.add_todo(format!("drop table {}.{}", ks, tn));
        }
    }
    {
        let tn = format!("{}{}", rett.table_prefix(), "bin_write_index_v02");
        if has_table(&ks, &tn, scy).await? {
            chs.add_todo(format!("drop table {}.{}", ks, tn));
        }
    }
    Ok(())
}

pub async fn migrate_scylla_data_schema_all_rt(
    scyconfs: [&ScyllaIngestConfig; 4],
    do_change: bool,
) -> Result<(), Error> {
    let mut chsa = [Changeset::new(), Changeset::new(), Changeset::new(), Changeset::new()];
    let rts = [
        RetentionTime::Short,
        RetentionTime::Medium,
        RetentionTime::Long,
        RetentionTime::Short,
    ];
    let rfs = [3, 3, 3, 1];
    for (((rt, scyconf), chs), rf) in rts
        .clone()
        .into_iter()
        .zip(scyconfs.iter())
        .zip(chsa.iter_mut())
        .zip(rfs.iter().map(|&x| x))
    {
        migrate_scylla_data_schema(scyconf, rt, rf, chs).await?;
    }
    let todo = chsa.iter().any(|x| x.has_to_do());
    if do_change {
        if todo {
            for ((_rt, scyconf), chs) in rts.into_iter().zip(scyconfs.iter()).zip(chsa.iter_mut()) {
                if chs.has_to_do() {
                    let scy2 = create_session_no_ks(scyconf).await?;
                    let scy = &scy2;
                    for cql in chs.todo.iter() {
                        match scy.query_unpaged(cql.as_str(), ()).await {
                            Ok(_) => {}
                            Err(e) => {
                                info!("cql error {}", cql);
                                return Err(e.into());
                            }
                        }
                    }
                }
            }
            let fut = migrate_scylla_data_schema_all_rt(scyconfs, false);
            Box::pin(fut).await?;
            Ok(())
        } else {
            Ok(())
        }
    } else {
        if todo {
            for chs in chsa.iter_mut() {
                chs.log_statements();
            }
            Err(Error::BadSchema)
        } else {
            Ok(())
        }
    }
}
