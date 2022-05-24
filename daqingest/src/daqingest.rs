pub mod query;

use clap::Parser;
use netfetch::ca::{CaConnectOpts, ListenFromFileOpts};
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
    CaChannel(CaChannel),
    CaConfig(CaConfig),
    CaSearch(CaConfig),
}

#[derive(Debug, Parser)]
pub struct CaChannel {
    #[clap(long)]
    pub channel: Vec<String>,
    #[clap(long)]
    pub addr_bind: String,
    #[clap(long)]
    pub addr_conn: String,
}

impl From<CaChannel> for CaConnectOpts {
    fn from(k: CaChannel) -> Self {
        Self {
            channels: k.channel,
            search: vec!["255.255.255.255".into()],
            addr_bind: k.addr_bind.parse().expect("can not parse address"),
            addr_conn: k.addr_conn.parse().expect("can not parse address"),
            max_simul: 113,
            timeout: 2000,
            abort_after_search: 0,
            pg_pass: "".into(),
            array_truncate: 512,
        }
    }
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
