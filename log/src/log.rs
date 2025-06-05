#![allow(unused_imports)]

pub use branch_debug as debug;
pub use branch_error as error;
pub use branch_info as info;
pub use branch_trace as trace;
pub use branch_warn as warn;

use std::sync::LazyLock;

#[allow(unused)]
#[inline(always)]
pub fn is_log_direct() -> bool {
    static ONCE: LazyLock<bool> =
        LazyLock::new(|| std::env::var("LOG_DIRECT").map_or(false, |x| x.parse().unwrap_or(false)));
    *ONCE
}

pub mod log_tracing {
    pub use tracing::debug;
    pub use tracing::error;
    pub use tracing::info;
    pub use tracing::trace;
    pub use tracing::warn;
    // pub use tracing::{self, event, span, Level};
}

pub mod log_direct {
    #[allow(unused)]
    #[macro_export]
    macro_rules! direct_trace {
        ($fmt:expr) => {
            eprintln!("TRACE {}", format_args!($fmt));
        };
        ($fmt:expr, $($arg:tt)*) => {
            eprintln!("TRACE {}", format_args!($fmt, $($arg)*));
        };
    }
    #[allow(unused)]
    #[macro_export]
    macro_rules! direct_debug {
        ($fmt:expr) => {
            // eprintln!(concat!("DEBUG ", $fmt));
            // eprintln!("{}", format_args!(concat!("DEBUG ", $fmt)));
            eprintln!("DEBUG {}", format_args!($fmt));
        };
        ($fmt:expr, $($arg:expr),*) => {
            // eprintln!(concat!("DEBUG ", $fmt), $($arg),*);
            // eprintln!("{}", format_args!(concat!("DEBUG ", $fmt), $($arg),*));
            eprintln!("DEBUG {}", format_args!($fmt, $($arg),*));
        };
    }
    #[allow(unused)]
    #[macro_export]
    macro_rules! direct_info {
        ($fmt:expr) => {
            eprintln!("INFO  {}", format_args!($fmt));
        };
        ($fmt:expr, $($arg:expr),*) => {
            eprintln!("INFO  {}", format_args!($fmt, $($arg),*));
        };
    }
    #[allow(unused)]
    #[macro_export]
    macro_rules! direct_warn {
        ($fmt:expr) => {
            eprintln!("WARN  {}", format_args!($fmt));
        };
        ($fmt:expr, $($arg:expr),*) => {
            eprintln!("WARN  {}", format_args!($fmt, $($arg),*));
        };
    }
    #[allow(unused)]
    #[macro_export]
    macro_rules! direct_error {
        ($fmt:expr) => {
            eprintln!("ERROR {}", format_args!($fmt));
        };
        ($fmt:expr, $($arg:expr),*) => {
            eprintln!("ERROR {}", format_args!($fmt, $($arg),*));
        };
    }
    pub use crate::direct_debug as debug;
    pub use crate::direct_error as error;
    pub use crate::direct_info as info;
    pub use crate::direct_trace as trace;
    pub use crate::direct_warn as warn;
}

#[allow(unused)]
#[macro_export]
macro_rules! log_v2_trace {
    // ($fmt:expr) => {
    //     let h = format_args!();
    //     eprintln!(concat!("TRACE V2 ", $fmt));
    // };
    ($fmt:expr, $($arg:expr),*) => {
        // let fmt2 = concat!("", $fmt);
        // let fmt2 = concat!("TRACE V2 ", $fmt, $($arg),*);
        // let h = format_args!($fmt, $($arg),*);
        // eprintln!("h: {:?}", h);
    };
}

pub mod log_macros_branch {
    #[allow(unused)]
    #[macro_export]
    macro_rules! branch_trace {
        ($($arg:tt)*) => {
            if $crate::is_log_direct() {
                $crate::log_direct::trace!($($arg)*);
            } else {
                $crate::log_tracing::trace!($($arg)*);
            }
        };
    }
    #[allow(unused)]
    #[macro_export]
    macro_rules! branch_debug {
        ($($arg:tt)*) => {
            if $crate::is_log_direct() {
                $crate::log_direct::debug!($($arg)*);
            } else {
                $crate::log_tracing::debug!($($arg)*);
            }
        };
    }
    #[allow(unused)]
    #[macro_export]
    macro_rules! branch_info {
        ($($arg:tt)*) => {
            if $crate::is_log_direct() {
                $crate::log_direct::info!($($arg)*);
            } else {
                $crate::log_tracing::info!($($arg)*);
            }
        };
    }
    #[allow(unused)]
    #[macro_export]
    macro_rules! branch_warn {
        ($($arg:tt)*) => {
            if $crate::is_log_direct() {
                $crate::log_direct::warn!($($arg)*);
            } else {
                $crate::log_tracing::warn!($($arg)*);
            }
        };
    }
    #[allow(unused)]
    #[macro_export]
    macro_rules! branch_error {
        ($($arg:tt)*) => {
            if $crate::is_log_direct() {
                $crate::log_direct::error!($($arg)*);
            } else {
                $crate::log_tracing::error!($($arg)*);
            }
        };
    }
    pub use branch_debug as debug;
    pub use branch_error as error;
    pub use branch_info as info;
    pub use branch_trace as trace;
    pub use branch_warn as warn;
    pub use tracing::{self, Level, event, span};
}
