pub mod query;

use clap::ArgAction::Count;
use clap::Parser;
use netfetch::ca::ListenFromFileOpts;
use netfetch::zmtp::ZmtpClientOpts;

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
    Bsread(Bsread),
    ListPkey,
    ListPulses,
    FetchEvents(FetchEvents),
    BsreadDump(BsreadDump),
    #[command(subcommand)]
    ChannelAccess(ChannelAccess),
}

#[derive(Debug, Parser)]
pub struct Bsread {
    #[arg(long)]
    pub backend: String,
    #[arg(long)]
    pub scylla: Vec<String>,
    #[arg(long)]
    pub source: Vec<String>,
    #[arg(long)]
    pub rcvbuf: Option<usize>,
    #[arg(long)]
    pub array_truncate: Option<usize>,
    #[arg(long)]
    pub do_pulse_id: bool,
    #[arg(long)]
    pub skip_insert: bool,
    #[arg(long)]
    pub process_channel_count_limit: Option<usize>,
}

impl From<Bsread> for ZmtpClientOpts {
    fn from(k: Bsread) -> Self {
        Self {
            backend: k.backend,
            scylla: k.scylla,
            sources: k.source,
            rcvbuf: k.rcvbuf,
            array_truncate: k.array_truncate,
            do_pulse_id: k.do_pulse_id,
            process_channel_count_limit: k.process_channel_count_limit,
            skip_insert: k.skip_insert,
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

impl From<CaConfig> for ListenFromFileOpts {
    fn from(k: CaConfig) -> Self {
        Self {
            config: k.config.into(),
        }
    }
}
