use clap::Parser;
use daqingest::{DaqIngestOpts, SubCmd};
use err::Error;

pub fn main() -> Result<(), Error> {
    taskrun::run(async {
        log::info!("daqingest  version {}", clap::crate_version!());
        if false {
            return Err(Error::with_msg_no_trace(format!("unknown command")));
        } else {
        }
        let opts = DaqIngestOpts::parse();
        log::info!("opts: {opts:?}");
        match opts.subcmd {
            SubCmd::Bsread(k) => netfetch::zmtp::zmtp_client(k.into()).await?,
            SubCmd::ListPkey => daqingest::query::list_pkey().await?,
            SubCmd::ListPulses => daqingest::query::list_pulses().await?,
            SubCmd::FetchEvents(k) => daqingest::query::fetch_events(k).await?,
            SubCmd::BsreadDump(k) => {
                let mut f = netfetch::zmtp::BsreadDumper::new(k.source);
                f.run().await?
            }
        }
        Ok(())
    })
}
