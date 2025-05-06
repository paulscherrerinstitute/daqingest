use clap::Parser;
use daqingest::opts::DaqIngestOpts;
use err::Error;
use log::*;
use netfetch::conf::CaIngestOpts;
use netfetch::conf::parse_config;
use netpod::Database;
use scywr::config::ScyllaIngestConfig;
use taskrun::TracingMode;

pub fn main() -> Result<(), Error> {
    let opts = DaqIngestOpts::parse();
    // TODO offer again function to get runtime and configure tracing in one call
    let runtime = taskrun::get_runtime_opts(opts.worker_threads.unwrap_or(8), opts.blocking_threads.unwrap_or(256));
    match taskrun::tracing_init(TracingMode::Production) {
        Ok(()) => {}
        Err(()) => return Err(Error::with_msg_no_trace("tracing init failed")),
    }
    let res = runtime.block_on(main_run(opts));
    match res {
        Ok(k) => Ok(k),
        Err(e) => {
            error!("catched: {:?}", e);
            Err(e)
        }
    }
}

async fn main_run(opts: DaqIngestOpts) -> Result<(), Error> {
    taskrun::tokio::spawn(main_run_inner(opts))
        .await
        .map_err(Error::from_string)?
}

async fn main_run_inner(opts: DaqIngestOpts) -> Result<(), Error> {
    let buildmark = "+0010";
    use daqingest::opts::ChannelAccess;
    use daqingest::opts::SubCmd;
    match opts.subcmd {
        SubCmd::ListPkey => {
            // TODO must take scylla config from CLI
            let scylla_conf = err::todoval();
            scywr::tools::list_pkey(&scylla_conf)
                .await
                .map_err(Error::from_string)?
        }
        SubCmd::ListPulses => {
            // TODO must take scylla config from CLI
            let scylla_conf = err::todoval();
            scywr::tools::list_pulses(&scylla_conf)
                .await
                .map_err(Error::from_string)?
        }
        SubCmd::FetchEvents(k) => {
            // TODO must take scylla config from CLI
            let scylla_conf = err::todoval();
            scywr::tools::fetch_events(&k.backend, &k.channel, &scylla_conf)
                .await
                .map_err(Error::from_string)?
        }
        SubCmd::Db(k) => {
            use daqingest::opts::DbSub;
            let pgconf = Database {
                host: k.pg_host,
                port: k.pg_port,
                user: k.pg_user,
                pass: k.pg_pass,
                name: k.pg_name,
            };
            let scyconf = ScyllaIngestConfig::new([k.scylla_host], k.scylla_keyspace);
            match k.sub {
                DbSub::Data(u) => {
                    use daqingest::opts::DbDataSub;
                    match u.sub {
                        DbDataSub::RemoveOlder(params) => {
                            info!("RemoveOlder  {:?}  {:?}", pgconf, scyconf);
                            daqingest::tools::remove_older(u.backend, params, &pgconf, &scyconf)
                                .await
                                .map_err(Error::from_string)?;
                        }
                        DbDataSub::RemoveOlderAll(params) => {
                            info!("RemoveOlderAll  {:?}  {:?}", params, scyconf);
                            daqingest::tools::remove_older_all(params, &scyconf)
                                .await
                                .map_err(Error::from_string)?;
                        }
                        DbDataSub::FindOlder(params) => {
                            info!("FindOlder  {:?}  {:?}", pgconf, scyconf);
                            daqingest::tools::find_older_msp(u.backend, params, &pgconf, &scyconf)
                                .await
                                .map_err(Error::from_string)?;
                        }
                    }
                }
            }
        }
        SubCmd::Scylla(k) => match k.sub {
            daqingest::opts::ScyllaSubcmd::Schema(j) => match j.sub {
                daqingest::opts::ScyllaSchemaSubcmd::Check(h) => {
                    info!("daqingest version {} {}", clap::crate_version!(), buildmark);
                    let (opts, _) = parse_config(h.config.into()).await?;
                    scylla_schema_check(opts, false).await?;
                }
                daqingest::opts::ScyllaSchemaSubcmd::Change(h) => {
                    info!("daqingest version {} {}", clap::crate_version!(), buildmark);
                    let (opts, _) = parse_config(h.config.into()).await?;
                    scylla_schema_check(opts, true).await?;
                }
            },
        },
        SubCmd::ChannelAccess(k) => match k {
            ChannelAccess::CaIngest(k) => {
                info!("daqingest version {} {}", clap::crate_version!(), buildmark);
                let (conf, channels_config) = parse_config(k.config.into()).await?;
                daqingest::daemon::run(conf, channels_config).await?
            }
            ChannelAccess::CaSearch(_k) => {
                // info!("daqingest version {}", clap::crate_version!());
                // let (conf, channels) = parse_config(k.config.into()).await?;
                // netfetch::ca::search::ca_search(conf, &channels).await?
            }
        },
        #[cfg(feature = "bsread")]
        SubCmd::Bsread(k) => ingest_bsread::zmtp::zmtp_client(k.into())
            .await
            .map_err(|e| Error::from(e.to_string()))?,
        #[cfg(feature = "bsread")]
        SubCmd::BsreadDump(k) => {
            let mut f = ingest_bsread::zmtp::dumper::BsreadDumper::new(k.source);
            f.run().await.map_err(|e| Error::from(e.to_string()))?
        }
        SubCmd::Version => {
            println!("{}", clap::crate_version!());
        }
        SubCmd::LogTest => {
            info!("log-test");
            warn!("log-test");
            error!("log-test");
            debug!("log-test");
            trace!("log-test");
            netfetch::log_test();
            let _spg = tracing::span!(tracing::Level::INFO, "log_span_debug");
            _spg.in_scope(|| {
                netfetch::log_test();
            })
        }
        SubCmd::Ca(subcmd) => {
            use daqingest::opts::CaSubcmds::*;
            match subcmd.subcmds {
                Find(cmd) => {
                    daqingest::tools::catools::find(cmd, subcmd.broadcast.unwrap_or(String::new()))
                        .await
                        .map_err(|e| Error::from_string(e))?;
                }
                Get(_cmd) => todo!(),
            }
        }
    }
    Ok(())
}

async fn scylla_schema_check(opts: CaIngestOpts, do_change: bool) -> Result<(), Error> {
    let opstr = if do_change { "change" } else { "check" };
    info!("start scylla schema {}", opstr);
    info!("{:?}", opts.scylla_config_st());
    info!("{:?}", opts.scylla_config_mt());
    info!("{:?}", opts.scylla_config_lt());
    info!("{:?}", opts.scylla_config_st_rf1());
    scywr::schema::migrate_scylla_data_schema_all_rt(
        [
            &opts.scylla_config_st(),
            &opts.scylla_config_mt(),
            &opts.scylla_config_lt(),
            &opts.scylla_config_st_rf1(),
        ],
        do_change,
    )
    .await
    .map_err(Error::from_string)?;
    info!("stop scylla schema {}", opstr);
    Ok(())
}
