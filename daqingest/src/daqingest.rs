pub mod query;

use clap::Parser;
use netfetch::zmtp::ZmtpClientOpts;

#[derive(Debug, Parser)]
//#[clap(name = "daqingest", version)]
//#[clap(version)]
pub struct DaqIngestOpts {
    #[clap(long, parse(from_occurrences))]
    pub verbose: u32,
    #[clap(subcommand)]
    pub subcmd: SubCmd,
}

#[derive(Debug, Parser)]
pub enum SubCmd {
    Bsread(Bsread),
    ListPkey,
    ListPulses,
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
}

impl From<Bsread> for ZmtpClientOpts {
    fn from(k: Bsread) -> Self {
        Self {
            scylla: k.scylla,
            sources: k.source,
            rcvbuf: k.rcvbuf,
            array_truncate: k.array_truncate,
            do_pulse_id: k.do_pulse_id,
        }
    }
}
