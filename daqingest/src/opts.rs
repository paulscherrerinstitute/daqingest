use clap::ArgAction::Count;
use clap::Parser;
#[cfg(feature = "bsread")]
use ingest_bsread::zmtp::ZmtpClientOpts;
use std::net::SocketAddr;

#[derive(Debug, Parser)]
#[command(author, version, about)]
pub struct DaqIngestOpts {
    #[arg(long, action(Count))]
    pub verbose: u32,
    #[clap(long)]
    pub tag: Option<String>,
    #[command(subcommand)]
    pub subcmd: SubCmd,
    #[arg(long)]
    pub nworkers: Option<usize>,
}

#[derive(Debug, Parser)]
pub enum SubCmd {
    ListPkey,
    ListPulses,
    FetchEvents(FetchEvents),
    #[command(subcommand)]
    ChannelAccess(ChannelAccess),
    #[cfg(feature = "bsread")]
    Bsread(Bsread),
    #[cfg(feature = "bsread")]
    BsreadDump(BsreadDump),
    Version,
}

#[derive(Debug, Parser)]
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

#[derive(Debug, Parser)]
pub struct FetchEvents {
    #[arg(long, num_args(1..))]
    pub scylla: Vec<String>,
    #[arg(long)]
    pub channel: String,
    #[arg(long)]
    pub backend: String,
}

#[derive(Debug, Parser)]
pub struct BsreadDump {
    pub source: String,
}

#[derive(Debug, Parser)]
pub enum ChannelAccess {
    CaIngest(CaConfig),
    CaSearch(CaSearch),
}

#[derive(Debug, Parser)]
pub struct CaSearch {
    pub config: String,
}

#[derive(Debug, Parser)]
pub struct CaConfig {
    pub config: String,
}
