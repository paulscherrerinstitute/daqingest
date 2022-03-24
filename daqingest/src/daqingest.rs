use clap::Parser;

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
}

#[derive(Debug, Parser)]
pub struct Bsread {
    #[clap(long)]
    pub source: String,
    #[clap(long)]
    pub rcvbuf: Option<u32>,
}
