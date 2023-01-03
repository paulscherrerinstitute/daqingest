use clap::Parser;
use daqingest::opts::DaqIngestOpts;
use err::Error;
use log::*;
use netfetch::conf::parse_config;

pub fn main() -> Result<(), Error> {
    let opts = DaqIngestOpts::parse();
    // TODO offer again function to get runtime and configure tracing in one call
    let runtime = taskrun::get_runtime_opts(opts.nworkers.unwrap_or(12), 32);
    taskrun::tracing_init().unwrap();
    info!("daqingest version {}", clap::crate_version!());
    let res = runtime.block_on(async move {
        use daqingest::opts::ChannelAccess;
        use daqingest::opts::SubCmd;
        match opts.subcmd {
            SubCmd::Bsread(k) => netfetch::zmtp::zmtp_client(k.into()).await?,
            SubCmd::ListPkey => daqingest::query::list_pkey().await?,
            SubCmd::ListPulses => daqingest::query::list_pulses().await?,
            SubCmd::FetchEvents(k) => daqingest::query::fetch_events(k).await?,
            SubCmd::BsreadDump(k) => {
                let mut f = netfetch::zmtp::BsreadDumper::new(k.source);
                f.run().await?
            }
            SubCmd::ChannelAccess(k) => match k {
                ChannelAccess::CaSearch(k) => {
                    let (conf, channels) = parse_config(k.config.into()).await?;
                    netfetch::ca::search::ca_search(conf, &channels).await?
                }
                ChannelAccess::CaIngest(k) => {
                    let (conf, channels) = parse_config(k.config.into()).await?;
                    netfetch::ca::ca_connect(conf, &channels).await?
                }
                ChannelAccess::CaIngestNew(k) => {
                    let (conf, channels) = parse_config(k.config.into()).await?;
                    daqingest::daemon::run(conf, channels).await?
                }
            },
        }
        Ok(())
    });
    match res {
        Ok(k) => Ok(k),
        Err(e) => {
            error!("Catched: {:?}", e);
            Err(e)
        }
    }
}
