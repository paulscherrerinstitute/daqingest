pub mod series;

pub use series::ChannelStatusSeriesId;
pub use series::SeriesId;

use log::*;

pub fn log_test() {
    info!("log-test");
    warn!("log-test");
    error!("log-test");
    debug!("log-test");
    trace!("log-test");
}
