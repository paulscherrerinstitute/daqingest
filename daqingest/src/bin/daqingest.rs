use clap::Parser;
use daqingest::{ChannelAccess, DaqIngestOpts, SubCmd};
use err::Error;

pub fn main() -> Result<(), Error> {
    taskrun::run(async {
        if false {
            return Err(Error::with_msg_no_trace(format!("unknown command")));
        } else {
        }
        let opts = DaqIngestOpts::parse();
        log::info!("daqingest version {}", clap::crate_version!());
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
                ChannelAccess::CaChannel(_) => todo!(),
                ChannelAccess::CaSearch(k) => netfetch::ca::ca_search(k.into()).await?,
                ChannelAccess::CaConfig(k) => netfetch::ca::ca_connect(k.into()).await?,
            },
        }
        Ok(())
    })
}
