pub mod query;

use clap::Parser;
use netfetch::ca::ListenFromFileOpts;
use netfetch::zmtp::ZmtpClientOpts;

#[derive(Debug, Parser)]
//#[clap(name = "daqingest", version)]
#[clap(version)]
pub struct DaqIngestOpts {
    #[clap(long, parse(from_occurrences))]
    pub verbose: u32,
    #[clap(long)]
    pub tag: Option<String>,
    #[clap(subcommand)]
    pub subcmd: SubCmd,
    #[clap(long)]
    pub nworkers: Option<usize>,
}

#[derive(Debug, Parser)]
pub enum SubCmd {
    Bsread(Bsread),
    ListPkey,
    ListPulses,
    FetchEvents(FetchEvents),
    BsreadDump(BsreadDump),
    #[clap(subcommand)]
    ChannelAccess(ChannelAccess),
}

#[derive(Debug, Parser)]
pub struct Bsread {
    #[clap(long)]
    pub scylla: Vec<String>,
    #[clap(long)]
    pub source: Vec<String>,
    #[clap(long)]
    pub rcvbuf: Option<usize>,
    #[clap(long)]
    pub array_truncate: Option<usize>,
    #[clap(long)]
    pub do_pulse_id: bool,
    #[clap(long)]
    pub skip_insert: bool,
    #[clap(long)]
    pub process_channel_count_limit: Option<usize>,
}

impl From<Bsread> for ZmtpClientOpts {
    fn from(k: Bsread) -> Self {
        Self {
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
    #[clap(long, min_values(1))]
    pub scylla: Vec<String>,
    #[clap(long)]
    pub channel: String,
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
