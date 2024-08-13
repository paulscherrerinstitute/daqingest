#[cfg(feature = "bsread")]
use ingest_bsread::zmtp::ZmtpClientOpts;
use std::net::SocketAddr;

#[derive(Debug, clap::Parser)]
#[command(author, version, about)]
pub struct DaqIngestOpts {
    #[arg(long, action(clap::ArgAction::Count))]
    pub verbose: u8,
    #[clap(long)]
    pub label: Option<String>,
    #[command(subcommand)]
    pub subcmd: SubCmd,
    #[arg(long)]
    pub worker_threads: Option<usize>,
    #[arg(long)]
    pub blocking_threads: Option<usize>,
}

#[derive(Debug, clap::Parser)]
pub enum SubCmd {
    ListPkey,
    ListPulses,
    FetchEvents(FetchEvents),
    Db(Db),
    ScyllaSchemaCheck(CaConfig),
    ScyllaSchemaChange(CaConfig),
    #[command(subcommand)]
    ChannelAccess(ChannelAccess),
    #[cfg(feature = "bsread")]
    Bsread(Bsread),
    #[cfg(feature = "bsread")]
    BsreadDump(BsreadDump),
    Version,
    LogTest,
}

#[derive(Debug, clap::Parser)]
pub struct Bsread {
    #[arg(long)]
    pub backend: String,
    #[arg(long)]
    pub addr: SocketAddr,
    #[arg(long)]
    pub rcvbuf: Option<usize>,
    #[arg(long)]
    pub array_truncate: Option<usize>,
    #[arg(long)]
    pub do_pulse_id: bool,
    #[arg(long)]
    pub process_channel_count_limit: Option<usize>,
}

#[cfg(feature = "bsread")]
impl From<Bsread> for ZmtpClientOpts {
    fn from(k: Bsread) -> Self {
        Self {
            backend: k.backend,
            addr: k.addr,
            rcvbuf: k.rcvbuf,
            array_truncate: k.array_truncate,
            do_pulse_id: k.do_pulse_id,
            process_channel_count_limit: k.process_channel_count_limit,
        }
    }
}

#[derive(Debug, clap::Parser)]
pub struct FetchEvents {
    #[arg(long, num_args(1..))]
    pub scylla: Vec<String>,
    #[arg(long)]
    pub channel: String,
    #[arg(long)]
    pub backend: String,
}

#[derive(Debug, clap::Parser)]
pub struct BsreadDump {
    pub source: String,
}

#[derive(Debug, clap::Parser)]
pub enum ChannelAccess {
    CaIngest(CaConfig),
    CaSearch(CaSearch),
}

#[derive(Debug, clap::Parser)]
pub struct CaSearch {
    pub config: String,
}

#[derive(Debug, clap::Parser)]
pub struct CaConfig {
    pub config: String,
}

#[derive(Debug, clap::Parser)]
pub struct Db {
    #[arg(long)]
    pub scylla_host: String,
    #[arg(long)]
    pub scylla_keyspace: String,
    #[arg(long)]
    pub pg_host: String,
    #[arg(long, default_value = "5432")]
    pub pg_port: u16,
    #[arg(long)]
    pub pg_user: String,
    #[arg(long)]
    pub pg_pass: String,
    #[arg(long)]
    pub pg_name: String,
    #[command(subcommand)]
    pub sub: DbSub,
}

#[derive(Debug, clap::Parser)]
pub struct ScyllaDb {
    #[arg(long)]
    pub scylla_host: String,
    #[arg(long)]
    pub scylla_keyspace: String,
}

#[derive(Debug, clap::Parser)]
pub enum DbSub {
    Data(DbData),
}

#[derive(Debug, clap::Parser)]
pub struct DbData {
    #[arg(long)]
    pub backend: String,
    #[command(subcommand)]
    pub sub: DbDataSub,
}

#[derive(Debug, clap::Parser)]
pub enum DbDataSub {
    RemoveOlder(RemoveOlder),
    RemoveOlderAll(RemoveOlderAll),
    FindOlder(FindOlder),
}

#[derive(Debug, clap::Parser)]
pub struct RemoveOlder {
    #[arg(long)]
    pub date: String,
    #[arg(long)]
    pub channel_regex: String,
}

#[derive(Debug, clap::Parser)]
pub struct RemoveOlderAll {
    #[arg(long)]
    pub date: String,
}

#[derive(Debug, clap::Parser)]
pub struct FindOlder {
    #[arg(long)]
    pub date: String,
    #[arg(long)]
    pub table_name: String,
    #[arg(long)]
    pub slices: u32,
}
