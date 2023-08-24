use err::Error;
use futures_util::StreamExt;
#[allow(unused)]
use netpod::log::*;
use netpod::ScyllaConfig;
use std::sync::Arc;

async fn check_table_exist(name: &str, scy: &Session) -> Result<bool, Error> {
    match scy.query(format!("select * from {} limit 1", name), ()).await {
        Ok(_) => Ok(true),
        Err(e) => match &e {
            QueryError::DbError(e2, msg) => match e2 {
                DbError::Invalid => {
                    if msg.contains("unconfigured table") {
                        Ok(false)
                    } else {
                        Err(Error::from(format!("{e}")))
                    }
                }
                _ => Err(Error::from(format!("{e}"))),
            },
            _ => Err(Error::from(format!("{e}"))),
        },
    }
}

async fn create_table_ts_msp(scy: &Session) -> Result<(), Error> {
    use std::fmt::Write;
    // seconds:
    let default_time_to_live = 60 * 60 * 5;
    // hours:
    let twcs_window_index = 24 * 4;
    let mut s = String::new();
    s.write_str("create table ts_msp (series bigint, ts_msp bigint, primary key (series, ts_msp))")?;
    write!(s, " with default_time_to_live = {}", default_time_to_live)?;
    s.write_str(" and compaction = { 'class': 'TimeWindowCompactionStrategy'")?;
    s.write_str(", 'compaction_window_unit': 'HOURS'")?;
    write!(s, ", 'compaction_window_size': {}", twcs_window_index)?;
    s.write_str(" }")?;
    scy.query(s, ()).await.map_err(|e| Error::from(format!("{e}")))?;
    Ok(())
}

struct GenTwcsTab {
    name: String,
    cql: String,
    default_time_to_live: usize,
    compaction_window_size: usize,
}

impl GenTwcsTab {
    fn name(&self) -> String {
        self.name.clone()
    }

    fn cql(&self) -> String {
        use std::fmt::Write;
        let mut s = String::new();
        write!(s, "create table {}", self.name()).unwrap();
        s.write_str(&self.cql).unwrap();
        write!(s, " with default_time_to_live = {}", self.default_time_to_live).unwrap();
        s.write_str(" and compaction = { 'class': 'TimeWindowCompactionStrategy', 'compaction_window_unit': 'HOURS'")
            .unwrap();
        write!(s, ", 'compaction_window_size': {}", self.compaction_window_size).unwrap();
        s.write_str(" }").unwrap();
        s
    }
}

struct EvTabDim0 {
    sty: String,
    cqlsty: String,
    // SCYLLA_TTL_EVENTS_DIM0
    default_time_to_live: usize,
    // TWCS_WINDOW_0D
    compaction_window_size: usize,
}

impl EvTabDim0 {
    fn name(&self) -> String {
        format!("events_scalar_{}", self.sty)
    }

    fn cql(&self) -> String {
        use std::fmt::Write;
        let mut s = String::new();
        write!(s, "create table {}", self.name()).unwrap();
        write!(s, " (series bigint, ts_msp bigint, ts_lsp bigint, pulse bigint, value {}, primary key ((series, ts_msp), ts_lsp))", self.cqlsty).unwrap();
        write!(s, " with default_time_to_live = {}", self.default_time_to_live).unwrap();
        s.write_str(" and compaction = { 'class': 'TimeWindowCompactionStrategy', 'compaction_window_unit': 'HOURS'")
            .unwrap();
        write!(s, ", 'compaction_window_size': {}", self.compaction_window_size).unwrap();
        s.write_str(" }").unwrap();
        s
    }
}

struct EvTabDim1 {
    sty: String,
    cqlsty: String,
    // SCYLLA_TTL_EVENTS_DIM1
    default_time_to_live: usize,
    // TWCS_WINDOW_1D
    compaction_window_size: usize,
}

impl EvTabDim1 {
    fn name(&self) -> String {
        format!("events_array_{}", self.sty)
    }

    fn cql(&self) -> String {
        use std::fmt::Write;
        let mut s = String::new();
        write!(s, "create table {}", self.name()).unwrap();
        write!(s, " (series bigint, ts_msp bigint, ts_lsp bigint, pulse bigint, value {}, primary key ((series, ts_msp), ts_lsp))", self.cqlsty).unwrap();
        write!(s, " with default_time_to_live = {}", self.default_time_to_live).unwrap();
        s.write_str(" and compaction = { 'class': 'TimeWindowCompactionStrategy', 'compaction_window_unit': 'HOURS'")
            .unwrap();
        write!(s, ", 'compaction_window_size': {}", self.compaction_window_size).unwrap();
        s.write_str(" }").unwrap();
        s
    }
}

async fn check_event_tables(scy: &Session) -> Result<(), Error> {
    let stys = [
        "u8", "u16", "u32", "u64", "i8", "i16", "i32", "i64", "f32", "f64", "bool", "string",
    ];
    let cqlstys = [
        "tinyint", "smallint", "int", "bigint", "tinyint", "smallint", "int", "bigint", "float", "double", "boolean",
        "text",
    ];
    for (sty, cqlsty) in stys.into_iter().zip(cqlstys) {
        let desc = EvTabDim0 {
            sty: sty.into(),
            cqlsty: cqlsty.into(),
            // ttl is set in actual data inserts
            default_time_to_live: 60 * 60 * 1,
            compaction_window_size: 48,
        };
        if !check_table_exist(&desc.name(), scy).await? {
            scy.query(desc.cql(), ())
                .await
                .map_err(|e| Error::from(format!("{e}")))?;
        }
        let desc = EvTabDim1 {
            sty: sty.into(),
            cqlsty: format!("frozen<list<{}>>", cqlsty),
            // ttl is set in actual data inserts
            default_time_to_live: 60 * 60 * 1,
            compaction_window_size: 12,
        };
        if !check_table_exist(&desc.name(), scy).await? {
            scy.query(desc.cql(), ())
                .await
                .map_err(|e| Error::from(format!("{e}")))?;
        }
    }
    Ok(())
}

pub async fn migrate_keyspace(scyconf: &ScyllaConfig) -> Result<(), Error> {
    let scy2 = create_session(scyconf).await?;
    let scy = &scy2;
    if !check_table_exist("ts_msp", &scy).await? {
        create_table_ts_msp(scy).await?;
    }
    check_event_tables(scy).await?;
    {
        let desc = GenTwcsTab {
            name: "series_by_ts_msp".into(),
            cql: "(part int, ts_msp int, shape_kind int, scalar_type int, series bigint, primary key ((part, ts_msp, shape_kind, scalar_type), series))".into(),
            default_time_to_live: 60 * 60 * 5,
            compaction_window_size: 24 * 4,
        };
        if !check_table_exist(&desc.name(), scy).await? {
            scy.query(desc.cql(), ())
                .await
                .map_err(|e| Error::from(format!("{e}")))?;
        }
    }
    {
        let desc = GenTwcsTab {
            name: "connection_status".into(),
            cql: "(ts_msp bigint, ts_lsp bigint, kind int, addr text, primary key (ts_msp, ts_lsp))".into(),
            default_time_to_live: 60 * 60 * 1,
            compaction_window_size: 24 * 4,
        };
        if !check_table_exist(&desc.name(), scy).await? {
            scy.query(desc.cql(), ())
                .await
                .map_err(|e| Error::from(format!("{e}")))?;
        }
    }
    {
        let desc = GenTwcsTab {
            name: "channel_status".into(),
            cql: "(series bigint, ts_msp bigint, ts_lsp bigint, kind int, primary key ((series, ts_msp), ts_lsp))"
                .into(),
            default_time_to_live: 60 * 60 * 1,
            compaction_window_size: 24 * 4,
        };
        if !check_table_exist(&desc.name(), scy).await? {
            scy.query(desc.cql(), ())
                .await
                .map_err(|e| Error::from(format!("{e}")))?;
        }
    }
    {
        let desc = GenTwcsTab {
            name: "channel_status_by_ts_msp".into(),
            cql: "(ts_msp bigint, ts_lsp bigint, series bigint, kind int, primary key (ts_msp, ts_lsp))".into(),
            default_time_to_live: 60 * 60 * 1,
            compaction_window_size: 24 * 4,
        };
        if !check_table_exist(&desc.name(), scy).await? {
            scy.query(desc.cql(), ())
                .await
                .map_err(|e| Error::from(format!("{e}")))?;
        }
    }
    {
        let desc = GenTwcsTab {
            name: "channel_ping".into(),
            cql: "(part int, ts_msp int, series bigint, ivl float, interest float, evsize int, primary key ((part, ts_msp), series))"
                .into(),
            default_time_to_live: 60 * 60 * 1,
            compaction_window_size: 24 * 4,
        };
        if !check_table_exist(&desc.name(), scy).await? {
            scy.query(desc.cql(), ())
                .await
                .map_err(|e| Error::from(format!("{e}")))?;
        }
    }
    {
        let desc = GenTwcsTab {
            name: "muted".into(),
            cql: "(part int, series bigint, ts bigint, ema float, emd float, primary key (part, series, ts))".into(),
            default_time_to_live: 60 * 60 * 4,
            compaction_window_size: 24 * 1,
        };
        if !check_table_exist(&desc.name(), scy).await? {
            scy.query(desc.cql(), ())
                .await
                .map_err(|e| Error::from(format!("{e}")))?;
        }
    }
    {
        let desc = GenTwcsTab {
            name: "item_recv_ivl".into(),
            cql: "(part int, series bigint, ts bigint, ema float, emd float, primary key (part, series, ts))".into(),
            default_time_to_live: 60 * 60 * 4,
            compaction_window_size: 24 * 1,
        };
        if !check_table_exist(&desc.name(), scy).await? {
            scy.query(desc.cql(), ())
                .await
                .map_err(|e| Error::from(format!("{e}")))?;
        }
    }
    {
        let desc = GenTwcsTab {
            name: "binned_scalar_f32_v01".into(),
            cql: "(series bigint, bin_len_sec int, bin_count int, off_msp int, off_lsp int, counts frozen<list<bigint>>, mins frozen<list<float>>, maxs frozen<list<float>>, avgs frozen<list<float>>, primary key ((series, bin_len_sec, bin_count, off_msp), off_lsp))"
                .into(),
            default_time_to_live: 60 * 60 * 24 * 30,
            compaction_window_size: 24 * 4,
        };
        if !check_table_exist(&desc.name(), scy).await? {
            scy.query(desc.cql(), ())
                .await
                .map_err(|e| Error::from(format!("{e}")))?;
        }
    }
    Ok(())
}
