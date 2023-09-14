use err::Error;
use ingest_linux::net::local_hostname;
use netpod::log::*;
use netpod::Database;
use netpod::ScyllaConfig;
use serde::Deserialize;
use serde::Serialize;
use std::path::PathBuf;
use std::time::Duration;
use taskrun::tokio;
use tokio::fs::OpenOptions;
use tokio::io::AsyncReadExt;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CaIngestOpts {
    backend: String,
    channels: PathBuf,
    api_bind: Option<String>,
    search: Vec<String>,
    #[serde(default)]
    search_blacklist: Vec<String>,
    whitelist: Option<String>,
    blacklist: Option<String>,
    max_simul: Option<usize>,
    #[serde(with = "humantime_serde")]
    timeout: Option<Duration>,
    postgresql: Database,
    scylla: ScyllaConfig,
    array_truncate: Option<u64>,
    insert_worker_count: Option<usize>,
    insert_scylla_sessions: Option<usize>,
    insert_queue_max: Option<usize>,
    insert_item_queue_cap: Option<usize>,
    local_epics_hostname: Option<String>,
    store_workers_rate: Option<u64>,
    insert_frac: Option<u64>,
    use_rate_limit_queue: Option<bool>,
    #[serde(with = "humantime_serde")]
    ttl_index: Option<Duration>,
    #[serde(with = "humantime_serde")]
    ttl_d0: Option<Duration>,
    #[serde(with = "humantime_serde")]
    ttl_d1: Option<Duration>,
    #[serde(with = "humantime_serde")]
    ttl_binned: Option<Duration>,
    pub test_bsread_addr: Option<String>,
}

impl CaIngestOpts {
    pub fn backend(&self) -> &str {
        &self.backend
    }

    pub fn api_bind(&self) -> String {
        self.api_bind.clone().unwrap_or_else(|| "0.0.0.0:3011".into())
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
        self.insert_worker_count.unwrap_or(800)
    }

    pub fn insert_scylla_sessions(&self) -> usize {
        self.insert_scylla_sessions.unwrap_or(1)
    }

    pub fn insert_queue_max(&self) -> usize {
        self.insert_queue_max.unwrap_or(64)
    }

    pub fn array_truncate(&self) -> u64 {
        self.array_truncate.unwrap_or(512)
    }

    pub fn insert_item_queue_cap(&self) -> usize {
        self.insert_item_queue_cap.unwrap_or(80000)
    }

    pub fn local_epics_hostname(&self) -> String {
        self.local_epics_hostname.clone().unwrap_or_else(local_hostname)
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
ttl_d1: 10m 3s
ttl_binned: 70d
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
    assert_eq!(res.is_ok(), true);
    let conf = res.unwrap();
    assert_eq!(conf.channels, PathBuf::from("/some/path/file.txt"));
    assert_eq!(conf.api_bind, Some("0.0.0.0:3011".to_string()));
    assert_eq!(conf.search.get(0), Some(&"172.26.0.255".to_string()));
    assert_eq!(conf.scylla.hosts.get(1), Some(&"sf-nube-12:19042".to_string()));
    assert_eq!(conf.ttl_d1, Some(Duration::from_millis(1000 * (60 * 10 + 3) + 45)));
    assert_eq!(conf.ttl_binned, Some(Duration::from_secs(60 * 60 * 70)));
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

pub async fn parse_config(config: PathBuf) -> Result<(CaIngestOpts, Vec<String>), Error> {
    let mut file = OpenOptions::new().read(true).open(config).await?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).await?;
    let conf: CaIngestOpts = serde_yaml::from_slice(&buf).map_err(|e| Error::with_msg_no_trace(format!("{:?}", e)))?;
    drop(file);
    let re_p = regex::Regex::new(&conf.whitelist.clone().unwrap_or("--nothing-whitelisted--".into()))?;
    let re_n = regex::Regex::new(&conf.blacklist.clone().unwrap_or("--nothing-blacklisted--".into()))?;
    let mut file = OpenOptions::new().read(true).open(&conf.channels).await?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).await?;
    let lines = buf.split(|&x| x == 0x0a);
    let mut channels = Vec::new();
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
            channels.push(line.into());
        }
    }
    info!("Parsed {} channels", channels.len());
    Ok((conf, channels))
}
