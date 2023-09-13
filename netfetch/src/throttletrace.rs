use core::fmt;
use log::*;
use std::time::Duration;
use std::time::Instant;

pub struct ThrottleTrace {
    ivl: Duration,
    next: Instant,
    count: u64,
}

impl ThrottleTrace {
    pub fn new(ivl: Duration) -> Self {
        Self {
            ivl,
            next: Instant::now(),
            count: 0,
        }
    }

    pub fn trigger(&mut self, msg: &str) {
        self.count += 1;
        let tsnow = Instant::now();
        if self.next <= tsnow {
            self.next = tsnow + self.ivl;
            debug!("{} (count {})", msg, self.count);
        }
    }

    pub fn trigger_fmt(&mut self, msg: &str, params: &[&dyn fmt::Debug]) {
        self.count += 1;
        let tsnow = Instant::now();
        if self.next <= tsnow {
            self.next = tsnow + self.ivl;
            if params.len() == 1 {
                debug!("{}  {:?}  (count {})", msg, params[0], self.count);
            } else if params.len() == 2 {
                debug!("{}  {:?}  {:?}  (count {})", msg, params[0], params[1], self.count);
            } else {
                debug!("{}  (count {})", msg, self.count);
            }
        }
    }
}
