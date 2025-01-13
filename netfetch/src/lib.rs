pub mod ca;
pub mod conf;
pub mod daemon_common;
pub mod errconv;
pub mod linuxhelper;
pub mod metrics;
pub mod netbuf;
pub mod polltimer;
pub mod ratelimit;
pub mod rt;
#[cfg(test)]
pub mod test;
pub mod throttletrace;

use log::*;

pub fn log_test() {
    info!("log-test");
    warn!("log-test");
    error!("log-test");
    debug!("log-test");
    trace!("log-test");
}

pub fn dbg_chn(chn: &str) -> bool {
    let chns = ["SINEG01:QE-B1-OP"];
    chns.contains(&chn)
}
