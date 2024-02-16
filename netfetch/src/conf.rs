use err::Error;
use netpod::log::*;
use netpod::Database;
use netpod::ScyllaConfig;
use regex::Regex;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use taskrun::tokio;
use tokio::fs::OpenOptions;
use tokio::io::AsyncReadExt;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CaIngestOpts {
    backend: String,
    channels: Option<PathBuf>,
    api_bind: String,
    search: Vec<String>,
    #[serde(default)]
    search_blacklist: Vec<String>,
    whitelist: Option<String>,
    blacklist: Option<String>,
    max_simul: Option<usize>,
    #[serde(default, with = "humantime_serde")]
    timeout: Option<Duration>,
    postgresql: Database,
    scylla: ScyllaConfig,
    array_truncate: Option<u64>,
    insert_worker_count: Option<usize>,
    insert_worker_concurrency: Option<usize>,
    insert_scylla_sessions: Option<usize>,
    insert_item_queue_cap: Option<usize>,
    store_workers_rate: Option<u64>,
    insert_frac: Option<u64>,
    use_rate_limit_queue: Option<bool>,
    #[serde(default, with = "humantime_serde")]
    ttl_index: Option<Duration>,
    #[serde(default, with = "humantime_serde")]
    ttl_d0: Option<Duration>,
    #[serde(default, with = "humantime_serde")]
    ttl_d1: Option<Duration>,
    #[serde(default, with = "humantime_serde")]
    ttl_binned: Option<Duration>,
    pub test_bsread_addr: Option<String>,
}

impl CaIngestOpts {
    pub fn backend(&self) -> &str {
        &self.backend
    }

    pub fn api_bind(&self) -> String {
        self.api_bind.clone()
    }

    pub fn postgresql_config(&self) -> &Database {
        &self.postgresql
    }

    pub fn scylla_config(&self) -> &ScyllaConfig {
        &self.scylla
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

    pub fn insert_worker_count(&self) -> usize {
        self.insert_worker_count.unwrap_or(4)
    }

    pub fn insert_worker_concurrency(&self) -> usize {
        self.insert_worker_concurrency.unwrap_or(32)
    }

    pub fn insert_scylla_sessions(&self) -> usize {
        self.insert_scylla_sessions.unwrap_or(1)
    }

    pub fn array_truncate(&self) -> u64 {
        self.array_truncate.unwrap_or(512)
    }

    pub fn insert_item_queue_cap(&self) -> usize {
        self.insert_item_queue_cap.unwrap_or(80000)
    }

    pub fn store_workers_rate(&self) -> u64 {
        self.store_workers_rate.unwrap_or(5000)
    }

    pub fn insert_frac(&self) -> u64 {
        self.insert_frac.unwrap_or(1000)
    }

    pub fn use_rate_limit_queue(&self) -> bool {
        self.use_rate_limit_queue.unwrap_or(false)
    }

    pub fn ttl_index(&self) -> Duration {
        self.ttl_index
            .clone()
            .unwrap_or_else(|| Duration::from_secs(60 * 60 * 24 * 3))
    }

    pub fn ttl_d0(&self) -> Duration {
        self.ttl_d0
            .clone()
            .unwrap_or_else(|| Duration::from_secs(60 * 60 * 24 * 1))
    }

    pub fn ttl_d1(&self) -> Duration {
        self.ttl_d1.clone().unwrap_or_else(|| Duration::from_secs(60 * 60 * 12))
    }

    pub fn ttl_binned(&self) -> Duration {
        self.ttl_binned
            .clone()
            .unwrap_or_else(|| Duration::from_secs(60 * 60 * 24 * 40))
    }
}

#[test]
fn parse_config_minimal() {
    let conf = r###"
backend: scylla
ttl_d1: 10m 3s 45ms
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
    - sf-nube-11:19042
    - sf-nube-12:19042
  keyspace: ks1
"###;
    let res: Result<CaIngestOpts, _> = serde_yaml::from_slice(conf.as_bytes());
    let conf = res.unwrap();
    assert_eq!(conf.channels, Some(PathBuf::from("/some/path/file.txt")));
    assert_eq!(&conf.api_bind, "0.0.0.0:3011");
    assert_eq!(conf.search.get(0), Some(&"172.26.0.255".to_string()));
    assert_eq!(conf.scylla.hosts.get(1), Some(&"sf-nube-12:19042".to_string()));
    assert_eq!(conf.ttl_d1, Some(Duration::from_millis(1000 * (60 * 10 + 3) + 45)));
    assert_eq!(conf.ttl_binned, None);
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

pub async fn parse_config(config: PathBuf) -> Result<(CaIngestOpts, Option<ChannelsConfig>), Error> {
    let mut file = OpenOptions::new().read(true).open(config).await?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).await?;
    let conf: CaIngestOpts = serde_yaml::from_slice(&buf).map_err(|e| Error::with_msg_no_trace(format!("{:?}", e)))?;
    drop(file);
    let re_p = regex::Regex::new(&conf.whitelist.clone().unwrap_or("--nothing-ur9nc23ur98c--".into()))?;
    let re_n = regex::Regex::new(&conf.blacklist.clone().unwrap_or("--nothing-ksm2u98rcm28--".into()))?;
    let channels = if let Some(fname) = conf.channels.as_ref() {
        if fname.ends_with(".txt") {
            Some(parse_channel_config_txt(fname, re_p, re_n).await?)
        } else if fname.ends_with(".yml") {
            let e = Error::with_msg_no_trace("unsupported channe config file");
            return Err(e);
        } else {
            let meta = tokio::fs::metadata(fname).await?;
            if meta.is_dir() {
                Some(parse_config_dir(&fname).await?)
            } else {
                let e = Error::with_msg_no_trace("unsupported channe config file");
                return Err(e);
            }
        }
    } else {
        None
    };
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

        // TODO parse the yml file at this path and compile a merged configuration
        e.path();
        todo!();
    }
    Ok(ret)
}

async fn parse_channel_config_txt(fname: &Path, re_p: Regex, re_n: Regex) -> Result<ChannelsConfig, Error> {
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
                },
            };
            conf.channels.push(item);
        }
    }
    info!("Parsed {} channels", conf.channels.len());
    Ok(conf)
}

#[derive(Debug, Deserialize)]
pub struct ChannelConfigParse {
    archiving_configuration: IngestConfigArchiving,
}

#[derive(Debug, Deserialize)]
pub struct IngestConfigArchiving {
    #[serde(default, with = "serde_replication_bool")]
    replication: bool,
    #[serde(default, with = "serde_option_channel_read_config")]
    short_term: Option<ChannelReadConfig>,
    #[serde(default, with = "serde_option_channel_read_config")]
    medium_term: Option<ChannelReadConfig>,
    #[serde(default, with = "serde_option_channel_read_config")]
    long_term: Option<ChannelReadConfig>,
}

mod serde_replication_bool {
    use serde::de;
    use serde::Deserializer;
    use std::fmt;

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
            write!(fmt, "a keyword `Enabled` or `None`, or not this field at all")
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
    use serde::de;
    use serde::Deserializer;
    use std::fmt;
    use std::time::Duration;

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
            write!(fmt, "keyword `Monitor`, an integer, or not this field at all")
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
            if v < 1 || v > 108000 {
                let e = E::custom(format!("unsupported value {v:?}, polling must be in range 1..108000"));
                return Err(e);
            }
            Ok(Some(ChannelReadConfig::Poll(Duration::from_secs(v as u64))))
        }

        fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            if v < 1 || v > 108000 {
                let e = E::custom(format!("unsupported value {v:?}, polling must be in range 1..108000"));
                return Err(e);
            }
            self.visit_u64(v as u64)
        }
    }
}

#[derive(Debug, Deserialize)]
pub enum ChannelReadConfig {
    Monitor,
    Poll(Duration),
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
"###;
    let x: BTreeMap<String, ChannelConfigParse> = serde_yaml::from_str(inp).unwrap();
}

pub struct ChannelsConfig {
    channels: Vec<ChannelConfig>,
}

impl ChannelsConfig {
    fn new() -> Self {
        Self { channels: Vec::new() }
    }
}

impl From<BTreeMap<String, ChannelConfigParse>> for ChannelsConfig {
    fn from(value: BTreeMap<String, ChannelConfigParse>) -> Self {
        let channels = value
            .into_iter()
            .map(|(k, v)| ChannelConfig {
                name: k,
                arch: v.archiving_configuration,
            })
            .collect();
        ChannelsConfig { channels }
    }
}

pub struct ChannelConfig {
    name: String,
    arch: IngestConfigArchiving,
}
