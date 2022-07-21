use clap::Parser;
use daqingest::{ChannelAccess, DaqIngestOpts, SubCmd};
use err::Error;
use log::*;

pub fn main() -> Result<(), Error> {
    let opts = DaqIngestOpts::parse();
    info!("daqingest version {}", clap::crate_version!());
    let runtime = taskrun::get_runtime_opts(opts.nworkers.unwrap_or(12), 32);
    let res = runtime.block_on(async move {
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
                    let opts = daqingest::CaConfig { config: k.config }.into();
                    netfetch::ca::search::ca_search(opts).await?
                }
                ChannelAccess::CaIngest(k) => netfetch::ca::ca_connect(k.into()).await?,
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
