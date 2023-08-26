use clap::Parser;
use daqingest::opts::DaqIngestOpts;
use err::Error;
use log::*;
use netfetch::conf::parse_config;

pub fn main() -> Result<(), Error> {
    let opts = DaqIngestOpts::parse();
    // TODO offer again function to get runtime and configure tracing in one call
    let runtime = taskrun::get_runtime_opts(opts.nworkers.unwrap_or(12), 32);
    match taskrun::tracing_init() {
        Ok(()) => {}
        Err(()) => return Err(Error::with_msg_no_trace("tracing init failed")),
    }
    let res = runtime.block_on(async move {
        use daqingest::opts::ChannelAccess;
        use daqingest::opts::SubCmd;
        match opts.subcmd {
            SubCmd::Bsread(k) => netfetch::zmtp::zmtp_client(k.into())
                .await
                .map_err(|e| Error::from(e.to_string()))?,
            SubCmd::ListPkey => {
                // TODO must take scylla config from CLI
                let scylla_conf = err::todoval();
                scywr::tools::list_pkey(&scylla_conf).await?
            }
            SubCmd::ListPulses => {
                // TODO must take scylla config from CLI
                let scylla_conf = err::todoval();
                scywr::tools::list_pulses(&scylla_conf).await?
            }
            SubCmd::FetchEvents(k) => {
                // TODO must take scylla config from CLI
                let scylla_conf = err::todoval();
                scywr::tools::fetch_events(&k.backend, &k.channel, &scylla_conf).await?
            }
            SubCmd::BsreadDump(k) => {
                let mut f = netfetch::zmtp::dumper::BsreadDumper::new(k.source);
                f.run().await.map_err(|e| Error::from(e.to_string()))?
            }
            SubCmd::ChannelAccess(k) => match k {
                ChannelAccess::CaSearch(k) => {
                    info!("daqingest version {}", clap::crate_version!());
                    let (conf, channels) = parse_config(k.config.into()).await?;
                    netfetch::ca::search::ca_search(conf, &channels).await?
                }
                ChannelAccess::CaIngest(k) => {
                    info!("daqingest version {}", clap::crate_version!());
                    let (conf, channels) = parse_config(k.config.into()).await?;
                    daqingest::daemon::run(conf, channels).await?
                }
            },
            SubCmd::Version => {
                println!("{}", clap::crate_version!());
            }
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
