use std::time::Duration;
use std::time::Instant;

pub struct RateLimit {
    last: Instant,
    dtmin: Duration,
}

impl RateLimit {
    pub fn new(dtmin: Duration) -> Self {
        let last = Instant::now().checked_sub(2 * dtmin).unwrap();
        Self { last, dtmin }
    }

    pub fn trigger(&mut self, tsnow: Instant) -> bool {
        if self.last + self.dtmin <= tsnow {
            self.last = tsnow;
            true
        } else {
            false
        }
    }
}
