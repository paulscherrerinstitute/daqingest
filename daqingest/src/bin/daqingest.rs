use clap::Parser;
use daqingest::{DaqIngestOpts, SubCmd};
use err::Error;

pub fn main() -> Result<(), Error> {
    taskrun::run(async {
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
        }
        Ok(())
    })
}
