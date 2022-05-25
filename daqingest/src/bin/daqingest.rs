use clap::Parser;
use daqingest::{ChannelAccess, DaqIngestOpts, SubCmd};
use err::Error;
use log::*;

pub fn main() -> Result<(), Error> {
    let opts = DaqIngestOpts::parse();
    log::info!("daqingest version {}", clap::crate_version!());
    let runtime = taskrun::get_runtime_opts(opts.nworkers.unwrap_or(12), 32);
    let res = runtime.block_on(async move {
        if false {
            return Err(Error::with_msg_no_trace(format!("unknown command")));
        } else {
        }
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
                ChannelAccess::CaSearch(k) => netfetch::ca::ca_search(k.into()).await?,
                ChannelAccess::CaConfig(k) => netfetch::ca::ca_connect(k.into()).await?,
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
