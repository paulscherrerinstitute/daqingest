use err::Error;
use netpod::Database;
use netpod::log::*;
use regex::Regex;
use scywr::config::ScyllaIngestConfig;
use serde::Deserialize;
use serde::Serialize;
use serieswriter::rtwriter::MinQuiets;
use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use taskrun::tokio;
use tokio::fs::OpenOptions;
use tokio::io::AsyncReadExt;

#[derive(Clone, Debug, Deserialize)]
pub struct CaIngestOpts {
    backend: String,
    channels: Option<PathBuf>,
    api_bind: String,
    udp_broadcast_bind: Option<String>,
    search: Vec<String>,
    #[serde(default)]
    search_blacklist: Vec<String>,
    #[allow(unused)]
    #[serde(default, with = "humantime_serde")]
    timeout: Option<Duration>,
    postgresql: Database,
    scylla: Option<ScyllaDefaultHosts>,
    scylla_st: ScyllaRtConf,
    scylla_mt: ScyllaRtConf,
    scylla_lt: ScyllaRtConf,
    scylla_st_rf1: ScyllaRtConf,
    array_truncate: Option<u64>,
    insert_worker_count: Option<usize>,
    insert_worker_concurrency: Option<usize>,
    insert_scylla_sessions: Option<usize>,
    insert_item_queue_cap: Option<usize>,
    store_workers_rate: Option<u64>,
    insert_frac: Option<u64>,
    use_rate_limit_queue: Option<bool>,
    pub test_bsread_addr: Option<String>,
    #[serde(default)]
    scylla_disable: bool,
    #[serde(default)]
    scylla_ignore_writes: bool,
}

impl CaIngestOpts {
    pub fn backend(&self) -> &str {
        &self.backend
    }

    pub fn api_bind(&self) -> String {
        self.api_bind.clone()
    }

    pub fn channels(&self) -> Option<PathBuf> {
        self.channels.clone()
    }

    pub fn udp_broadcast_bind(&self) -> Option<&str> {
        self.udp_broadcast_bind.as_ref().map(String::as_str)
    }

    pub fn postgresql_config(&self) -> &Database {
        &self.postgresql
    }

    pub fn scylla_config_st(&self) -> ScyllaIngestConfig {
        let d = &self.scylla;
        let c = &self.scylla_st;
        let hosts = c
            .hosts
            .as_ref()
            .map_or(d.as_ref().map_or(Vec::new(), |x| x.hosts.clone()), |x| x.clone());
        ScyllaIngestConfig::new(hosts, c.keyspace.clone())
    }

    pub fn scylla_config_mt(&self) -> ScyllaIngestConfig {
        let d = &self.scylla;
        let c = &self.scylla_mt;
        let hosts = c
            .hosts
            .as_ref()
            .map_or(d.as_ref().map_or(Vec::new(), |x| x.hosts.clone()), |x| x.clone());
        ScyllaIngestConfig::new(hosts, c.keyspace.clone())
    }

    pub fn scylla_config_lt(&self) -> ScyllaIngestConfig {
        let d = &self.scylla;
        let c = &self.scylla_lt;
        let hosts = c
            .hosts
            .as_ref()
            .map_or(d.as_ref().map_or(Vec::new(), |x| x.hosts.clone()), |x| x.clone());
        ScyllaIngestConfig::new(hosts, c.keyspace.clone())
    }

    pub fn scylla_config_st_rf1(&self) -> ScyllaIngestConfig {
        let d = &self.scylla;
        let c = &self.scylla_st_rf1;
        let hosts = c
            .hosts
            .as_ref()
            .map_or(d.as_ref().map_or(Vec::new(), |x| x.hosts.clone()), |x| x.clone());
        ScyllaIngestConfig::new(hosts, c.keyspace.clone())
    }

    pub fn search(&self) -> &Vec<String> {
        &self.search
    }

    pub fn search_blacklist(&self) -> &Vec<String> {
        &self.search_blacklist
    }

    pub fn timeout(&self) -> Duration {
        Duration::from_millis(1200)
    }

    pub fn insert_scylla_sessions(&self) -> usize {
        self.insert_scylla_sessions.unwrap_or(1)
    }

    pub fn insert_worker_count(&self) -> usize {
        self.insert_worker_count.unwrap_or(10)
    }

    pub fn insert_worker_concurrency(&self) -> usize {
        self.insert_worker_concurrency.unwrap_or(64)
    }

    pub fn array_truncate(&self) -> u64 {
        self.array_truncate.unwrap_or(1024 * 200)
    }

    pub fn insert_item_queue_cap(&self) -> usize {
        self.insert_item_queue_cap.unwrap_or(1000 * 1000) * 2
    }

    pub fn store_workers_rate(&self) -> u64 {
        self.store_workers_rate.unwrap_or(1000 * 500)
    }

    pub fn insert_frac(&self) -> u64 {
        self.insert_frac.unwrap_or(1000)
    }

    pub fn use_rate_limit_queue(&self) -> bool {
        self.use_rate_limit_queue.unwrap_or(false)
    }

    pub fn scylla_disable(&self) -> bool {
        self.scylla_disable
    }

    pub fn scylla_ignore_writes(&self) -> bool {
        self.scylla_ignore_writes
    }

    pub fn is_valid(&self) -> bool {
        let confs = [&self.scylla_st, &self.scylla_mt, &self.scylla_lt, &self.scylla_st_rf1];
        let has_default_hosts = self.scylla.is_some();
        for c in confs.iter() {
            if c.hosts.is_none() && !has_default_hosts {
                warn!("scylla config is missing hosts");
                return false;
            }
        }
        return true;
    }
}

#[test]
fn parse_config_minimal() {
    let conf = r###"
backend: test_backend
timeout: 10m 3s 45ms
api_bind: "0.0.0.0:3011"
channels: /some/path/file.txt
search:
  - 172.26.0.255
  - 172.26.2.255
postgresql:
  host: host.example.com
  port: 5432
  user: USER
  pass: PASS
  name: NAME
scylla_st:
  keyspace: ks_st
  hosts:
    - node-st-1:19042
    - node-st-2:19042
scylla_mt:
  keyspace: ks_mt
  hosts:
    - node-mt-1:19042
    - node-mt-2:19042
scylla_st_rf1:
  keyspace: ks_st_rf1
  hosts:
    - node-st-rf1-1:19042
    - node-st-rf1-2:19042
scylla_lt:
  keyspace: ks_lt
  hosts:
    - node-lt-1:19042
    - node-lt-2:19042
"###;
    let res: Result<CaIngestOpts, _> = serde_yaml::from_slice(conf.as_bytes());
    let conf = res.unwrap();
    assert_eq!(conf.is_valid(), true);
    assert_eq!(conf.channels, Some(PathBuf::from("/some/path/file.txt")));
    assert_eq!(&conf.api_bind, "0.0.0.0:3011");
    assert_eq!(conf.search.get(0), Some(&"172.26.0.255".to_string()));
    assert_eq!(
        conf.scylla_config_st().hosts().get(1),
        Some(&"node-st-2:19042".to_string())
    );
    assert_eq!(
        conf.scylla_config_lt().hosts().get(1),
        Some(&"node-lt-2:19042".to_string())
    );
    assert_eq!(conf.timeout, Some(Duration::from_millis(1000 * (60 * 10 + 3) + 45)));
}

#[test]
fn parse_config_with_scylla_default() {
    let conf = r###"
backend: test_backend
timeout: 10m 3s 45ms
api_bind: "0.0.0.0:3011"
channels: /some/path/file.txt
search:
  - 172.26.0.255
  - 172.26.2.255
postgresql:
  host: host.example.com
  port: 5432
  user: USER
  pass: PASS
  name: NAME
scylla:
  hosts:
    - node1:19042
    - node2:19042
scylla_st:
  keyspace: ks_st
scylla_mt:
  keyspace: ks_mt
scylla_st_rf1:
  keyspace: ks_st_rf1
scylla_lt:
  keyspace: ks_lt
  hosts:
    - node3:19042
    - node4:19042
"###;
    let res: Result<CaIngestOpts, _> = serde_yaml::from_slice(conf.as_bytes());
    let conf = res.unwrap();
    assert_eq!(conf.is_valid(), true);
    assert_eq!(conf.channels, Some(PathBuf::from("/some/path/file.txt")));
    assert_eq!(&conf.api_bind, "0.0.0.0:3011");
    assert_eq!(conf.search.get(0), Some(&"172.26.0.255".to_string()));
    assert_eq!(conf.scylla_config_st().hosts().get(1), Some(&"node2:19042".to_string()));
    assert_eq!(conf.scylla_config_lt().hosts().get(1), Some(&"node4:19042".to_string()));
    assert_eq!(conf.timeout, Some(Duration::from_millis(1000 * (60 * 10 + 3) + 45)));
}

#[test]
fn test_duration_parse() {
    #[derive(Serialize, Deserialize)]
    struct A {
        #[serde(with = "humantime_serde")]
        dur: Duration,
    }
    let a = A {
        dur: Duration::from_millis(12000),
    };
    let s = serde_json::to_string(&a).unwrap();
    assert_eq!(s, r#"{"dur":"12s"}"#);
    let a = A {
        dur: Duration::from_millis(12012),
    };
    let s = serde_json::to_string(&a).unwrap();
    assert_eq!(s, r#"{"dur":"12s 12ms"}"#);
    let a: A = serde_json::from_str(r#"{"dur":"3s170ms"}"#).unwrap();
    assert_eq!(a.dur, Duration::from_millis(3170));
}

async fn parse_channel_config_txt(fname: &Path) -> Result<ChannelsConfig, Error> {
    let basename = fname.file_stem().unwrap().to_str().unwrap();
    let re_p = Regex::new("--------------------------").unwrap();
    let re_n = Regex::new("--------------------------").unwrap();
    let mut file = OpenOptions::new().read(true).open(fname).await?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).await?;
    let lines = buf.split(|&x| x == 0x0a);
    let mut conf = ChannelsConfig::new();
    for line in lines {
        let line = String::from_utf8_lossy(line);
        let line = line.trim();
        let use_line = if line.is_empty() {
            false
        } else if let Some(_cs) = re_p.captures(&line) {
            true
        } else if re_n.is_match(&line) {
            false
        } else {
            true
        };
        if use_line {
            let item = ChannelConfig {
                name: line.into(),
                arch: IngestConfigArchiving {
                    replication: true,
                    short_term: Some(ChannelReadConfig::Monitor),
                    medium_term: None,
                    long_term: None,
                    is_polled: false,
                    timestamp: ChannelTimestamp::Archiver,
                },
                config_file_basename: basename.to_string(),
            };
            conf.channels.push(item);
        }
    }
    info!("Parsed {} channels", conf.channels.len());
    Ok(conf)
}

pub async fn parse_channels(channels_dir: Option<PathBuf>) -> Result<Option<ChannelsConfig>, Error> {
    if let Some(fname) = channels_dir.as_ref() {
        let meta = tokio::fs::metadata(fname).await?;
        if meta.is_file() {
            if fname.ends_with(".txt") {
                Ok(Some(parse_channel_config_txt(fname).await?))
            } else {
                let e = Error::with_msg_no_trace(format!("unsupported channel config file {:?}", fname));
                return Err(e);
            }
        } else if meta.is_dir() {
            Ok(Some(parse_config_dir(&fname).await?))
        } else {
            let e = Error::with_msg_no_trace(format!("unsupported channel config input {:?}", fname));
            return Err(e);
        }
    } else {
        Ok(None)
    }
}

pub async fn parse_config(config: PathBuf) -> Result<(CaIngestOpts, Option<ChannelsConfig>), Error> {
    let mut file = OpenOptions::new().read(true).open(config).await?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).await?;
    let conf: CaIngestOpts = serde_yaml::from_slice(&buf).map_err(Error::from_string)?;
    drop(file);
    if !conf.is_valid() {
        let e = Error::with_msg_no_trace(format!("invalid config file"));
        return Err(e);
    }
    // let re_p = regex::Regex::new(&conf.whitelist.clone().unwrap_or("--nothing-ur9nc23ur98c--".into()))?;
    // let re_n = regex::Regex::new(&conf.blacklist.clone().unwrap_or("--nothing-ksm2u98rcm28--".into()))?;
    let channels = parse_channels(conf.channels.clone()).await?;
    Ok((conf, channels))
}

async fn parse_config_dir(dir: &Path) -> Result<ChannelsConfig, Error> {
    let mut ret = ChannelsConfig::new();
    let mut rd = tokio::fs::read_dir(dir).await?;
    loop {
        let e = rd.next_entry().await?;
        let e = if let Some(x) = e {
            x
        } else {
            break;
        };
        let fnp = e.path();
        let fns = fnp.to_str().unwrap();
        if fns.ends_with(".yml") || fns.ends_with(".yaml") {
            let basename = fnp.file_stem().unwrap().to_str().unwrap();
            let buf = tokio::fs::read(e.path()).await?;
            let conf: BTreeMap<String, ChannelConfigParse> =
                serde_yaml::from_slice(&buf).map_err(Error::from_string)?;
            info!("parsed {} channels from {}", conf.len(), fns);
            ret.push_from_parsed(&conf, basename);
        } else {
            debug!("ignore channel config file {:?}", e.path());
        }
    }
    Ok(ret)
}

#[derive(Debug, Deserialize)]
pub struct ChannelConfigParse {
    archiving_configuration: IngestConfigArchiving,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ChannelTimestamp {
    Archiver,
    IOC,
}

impl ChannelTimestamp {
    fn default_config() -> Self {
        Self::Archiver
    }

    fn is_default(&self) -> bool {
        if let ChannelTimestamp::Archiver = self {
            true
        } else {
            false
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct ScyllaDefaultHosts {
    hosts: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ScyllaRtConf {
    keyspace: String,
    hosts: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct IngestConfigArchiving {
    #[serde(default = "bool_true")]
    #[serde(with = "serde_replication_bool")]
    replication: bool,
    #[serde(default)]
    #[serde(with = "serde_option_channel_read_config")]
    short_term: Option<ChannelReadConfig>,
    #[serde(default)]
    #[serde(with = "serde_option_channel_read_config")]
    medium_term: Option<ChannelReadConfig>,
    #[serde(default)]
    #[serde(with = "serde_option_channel_read_config")]
    long_term: Option<ChannelReadConfig>,
    #[serde(default)]
    is_polled: bool,
    #[serde(default = "ChannelTimestamp::default_config")]
    timestamp: ChannelTimestamp,
}

impl IngestConfigArchiving {
    // TODO remove when no longer needed
    pub fn dummy() -> Self {
        Self {
            replication: false,
            short_term: None,
            medium_term: None,
            long_term: None,
            is_polled: false,
            timestamp: ChannelTimestamp::Archiver,
        }
    }
}

fn bool_true() -> bool {
    true
}

mod serde_ingest_config_archiving {
    use super::ChannelReadConfigApiFormat;
    use super::IngestConfigArchiving;
    use serde::Serializer;
    use serde::ser;
    use serde::ser::SerializeMap;

    impl ser::Serialize for IngestConfigArchiving {
        fn serialize<S>(&self, ser: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let mut map = ser.serialize_map(None)?;
            // ser.is_human_readable()
            if !self.replication {
                map.serialize_entry("replication", &self.replication)?;
            }
            if let Some(v) = self.short_term.as_ref() {
                map.serialize_entry("short_term", &ChannelReadConfigApiFormat(&v))?;
            }
            if let Some(v) = self.medium_term.as_ref() {
                map.serialize_entry("medium_term", &ChannelReadConfigApiFormat(&v))?;
            }
            if let Some(v) = self.long_term.as_ref() {
                map.serialize_entry("long_term", &ChannelReadConfigApiFormat(&v))?;
            }
            let anymon = [&self.short_term, &self.medium_term, &self.long_term]
                .into_iter()
                .map(|c| c.as_ref().map_or(false, |x| x.is_monitor()))
                .fold(false, |a, x| a || x);
            if anymon && self.is_polled || !anymon && !self.is_polled {
                map.serialize_entry("is_polled", &self.is_polled)?;
            }
            if !self.timestamp.is_default() {
                map.serialize_entry("timestamp", &self.timestamp)?;
            }
            map.end()
        }
    }
}

struct ChannelReadConfigApiFormat<'a>(&'a ChannelReadConfig);

#[allow(non_snake_case)]
mod serde_ChannelReadConfigApiFormat {
    use super::ChannelReadConfig;
    use super::ChannelReadConfigApiFormat;
    use serde::ser;

    impl<'a> ser::Serialize for ChannelReadConfigApiFormat<'a> {
        fn serialize<S>(&self, ser: S) -> Result<S::Ok, S::Error>
        where
            S: ser::Serializer,
        {
            match &self.0 {
                ChannelReadConfig::Monitor => ser.serialize_str("Monitor"),
                ChannelReadConfig::Poll(n) => ser.serialize_u32(n.as_secs() as u32),
            }
        }
    }
}

mod serde_replication_bool {
    use serde::Deserializer;
    use serde::Serializer;
    use serde::de;
    use std::fmt;

    #[allow(unused)]
    pub fn serialize<S>(v: &bool, ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        ser.serialize_bool(*v)
    }

    pub fn deserialize<'de, D>(de: D) -> Result<bool, D::Error>
    where
        D: Deserializer<'de>,
    {
        de.deserialize_any(Vis)
    }

    struct Vis;

    impl<'de> de::Visitor<'de> for Vis {
        type Value = bool;

        fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
            write!(fmt, "a keyword `Enabled`, `Disabled`, null, or not this field at all")
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            let e = E::custom(format!("could accept `null` value, but it's not in specification"));
            return Err(e);
        }

        fn visit_bool<E>(self, _v: bool) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            let e = E::custom(format!("could accept bool, but it's not in specification"));
            return Err(e);
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            let ret = if v == "Enabled" {
                true
            } else if v == "Disabled" {
                false
            } else {
                let e = E::custom(format!("unexpected value {v:?}"));
                return Err(e);
            };
            Ok(ret)
        }
    }
}

mod serde_option_channel_read_config {
    use super::ChannelReadConfig;
    use serde::Deserializer;
    use serde::Serializer;
    use serde::de;
    use std::fmt;
    use std::time::Duration;

    #[allow(unused)]
    pub fn serialize<S>(v: &Option<ChannelReadConfig>, ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match v {
            Some(x) => match x {
                ChannelReadConfig::Monitor => ser.serialize_str("Monitor"),
                ChannelReadConfig::Poll(n) => ser.serialize_u32(n.as_secs() as u32),
            },
            None => ser.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(de: D) -> Result<Option<ChannelReadConfig>, D::Error>
    where
        D: Deserializer<'de>,
    {
        de.deserialize_any(Vis)
    }

    struct Vis;

    impl<'de> de::Visitor<'de> for Vis {
        type Value = Option<ChannelReadConfig>;

        fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
            write!(fmt, "keyword `Monitor`, keyword `None`, an integer, or missing")
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            let ret = if v == "Monitor" {
                Some(ChannelReadConfig::Monitor)
            } else if v == "None" {
                None
            } else {
                let e = E::custom(format!("unexpected value {v:?}"));
                return Err(e);
            };
            Ok(ret)
        }

        fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            let max = 108000;
            if v < 1 || v > max {
                let e = E::custom(format!("unsupported value {v:?}, polling must be in range 1..{max:?}"));
                return Err(e);
            }
            Ok(Some(ChannelReadConfig::Poll(Duration::from_secs(v as u64))))
        }

        fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            let max = 108000;
            if v < 1 || v > max {
                let e = E::custom(format!("unsupported value {v:?}, polling must be in range 1..{max:?}"));
                return Err(e);
            }
            self.visit_u64(v as u64)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ChannelReadConfig {
    Monitor,
    Poll(Duration),
}

impl ChannelReadConfig {
    pub fn is_monitor(&self) -> bool {
        if let Self::Monitor = self { true } else { false }
    }
}

#[test]
fn test_channel_config_00() {
    let inp = r###"
CH-00:
  archiving_configuration:
    replication: Enabled
    short_term: Monitor
    medium_term: 60
    long_term: 3600
CH-01:
  archiving_configuration:
    replication: Enabled
    short_term: 1
    medium_term: 10
    long_term: 60
CH-02:
  archiving_configuration:
    short_term: Monitor
CH-03:
  archiving_configuration:
CH-04:
  archiving_configuration:
    short_term: None
    medium_term: None
    long_term: 3600
    is_polled: true
CH-05:
  archiving_configuration:
    short_term: None
    medium_term: None
    long_term: Monitor
"###;
    let x: BTreeMap<String, ChannelConfigParse> = serde_yaml::from_str(inp).unwrap();
    assert_eq!(
        x.get("CH-00").as_ref().unwrap().archiving_configuration.medium_term,
        Some(ChannelReadConfig::Poll(Duration::from_millis(1000 * 60)))
    );
}

#[derive(Debug)]
pub struct ChannelsConfig {
    channels: Vec<ChannelConfig>,
}

impl ChannelsConfig {
    fn new() -> Self {
        Self { channels: Vec::new() }
    }

    pub fn len(&self) -> usize {
        self.channels.len()
    }

    pub fn channels(&self) -> &Vec<ChannelConfig> {
        &self.channels
    }

    fn push_from_parsed(&mut self, rhs: &BTreeMap<String, ChannelConfigParse>, config_file_basename: &str) {
        for (k, v) in rhs.iter() {
            let item = ChannelConfig {
                name: k.into(),
                arch: v.archiving_configuration.clone(),
                config_file_basename: config_file_basename.into(),
            };
            self.channels.push(item);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ChannelConfig {
    name: String,
    arch: IngestConfigArchiving,
    config_file_basename: String,
}

impl ChannelConfig {
    pub fn st_monitor<S: Into<String>>(name: S, config_file_basename: &str) -> Self {
        Self {
            name: name.into(),
            arch: IngestConfigArchiving {
                replication: true,
                short_term: Some(ChannelReadConfig::Monitor),
                medium_term: None,
                long_term: None,
                is_polled: false,
                timestamp: ChannelTimestamp::Archiver,
            },
            config_file_basename: config_file_basename.into(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn config_file_basename(&self) -> &str {
        &self.config_file_basename
    }

    pub fn is_polled(&self) -> bool {
        self.arch.is_polled
    }

    pub fn replication(&self) -> bool {
        // self.arch.replication
        true
    }

    pub fn poll_conf(&self) -> Option<(u64,)> {
        if self.is_polled() {
            if let Some(ChannelReadConfig::Poll(x)) = self.arch.short_term {
                Some((x.as_millis() as u64,))
            } else if let Some(ChannelReadConfig::Poll(x)) = self.arch.medium_term {
                Some((x.as_millis() as u64,))
            } else if let Some(ChannelReadConfig::Poll(x)) = self.arch.long_term {
                Some((x.as_millis() as u64,))
            } else {
                Some((60,))
            }
        } else {
            None
        }
    }

    /// Only used when in monitoring mode. If we do not see activity for this Duration then
    /// we issue a manual read to see if the channel is alive.
    pub fn manual_poll_on_quiet_after(&self) -> Duration {
        Duration::from_secs(300)
    }

    pub fn expect_activity_within(&self) -> Duration {
        let dur = if self.is_polled() {
            // It would be anyway invalid to be polled and specify a monitor record policy.
            match self.arch.short_term {
                Some(ChannelReadConfig::Poll(x)) => x,
                Some(ChannelReadConfig::Monitor) => self.manual_poll_on_quiet_after(),
                None => match self.arch.medium_term {
                    Some(ChannelReadConfig::Poll(x)) => x,
                    Some(ChannelReadConfig::Monitor) => self.manual_poll_on_quiet_after(),
                    None => match self.arch.long_term {
                        Some(ChannelReadConfig::Poll(x)) => x,
                        Some(ChannelReadConfig::Monitor) => self.manual_poll_on_quiet_after(),
                        None => {
                            // This is an invalid configuration, so just a fallback
                            self.manual_poll_on_quiet_after()
                        }
                    },
                },
            }
        } else {
            self.manual_poll_on_quiet_after()
        };
        dur + Duration::from_millis(1000 * 10)
    }

    pub fn min_quiets(&self) -> MinQuiets {
        MinQuiets {
            st: match self.arch.short_term {
                Some(ChannelReadConfig::Monitor) => Duration::ZERO,
                Some(ChannelReadConfig::Poll(x)) => x,
                None => Duration::MAX,
            },
            mt: match self.arch.medium_term {
                Some(ChannelReadConfig::Monitor) => Duration::ZERO,
                Some(ChannelReadConfig::Poll(x)) => x,
                None => Duration::MAX,
            },
            lt: match self.arch.long_term {
                Some(ChannelReadConfig::Monitor) => Duration::ZERO,
                Some(ChannelReadConfig::Poll(x)) => x,
                None => Duration::MAX,
            },
        }
    }

    pub fn use_ioc_time(&self) -> bool {
        match &self.arch.timestamp {
            ChannelTimestamp::Archiver => false,
            ChannelTimestamp::IOC => true,
        }
    }

    // TODO remove when no longer needed.
    pub fn dummy() -> Self {
        Self {
            name: String::from("dummy"),
            arch: IngestConfigArchiving::dummy(),
            config_file_basename: String::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ChannelConfigForStatesApi {
    arch: IngestConfigArchiving,
}

impl From<ChannelConfig> for ChannelConfigForStatesApi {
    fn from(value: ChannelConfig) -> Self {
        Self { arch: value.arch }
    }
}
