#![allow(unused_imports)]
// pub use tracing::debug;
// pub use tracing::error;
// pub use tracing::info;
pub use tracing::trace;
pub use tracing::warn;

pub use direct_debug as debug;
pub use direct_error as error;
pub use direct_info as info;

pub mod log_macros_direct {
    #[allow(unused)]
    #[macro_export]
    macro_rules! direct_trace {
        ($fmt:expr) => {
            eprintln!(concat!("TRACE ", $fmt));
        };
        ($fmt:expr, $($arg:expr),*) => {
            eprintln!(concat!("TRACE ", $fmt), $($arg),*);
        };
    }
    #[allow(unused)]
    #[macro_export]
    macro_rules! direct_debug {
        ($fmt:expr) => {
            eprintln!(concat!("DEBUG ", $fmt));
        };
        ($fmt:expr, $($arg:expr),*) => {
            eprintln!(concat!("DEBUG ", $fmt), $($arg),*);
        };
    }
    #[allow(unused)]
    #[macro_export]
    macro_rules! direct_info {
        ($fmt:expr) => {
            eprintln!(concat!("INFO  ", $fmt));
        };
        ($fmt:expr, $($arg:expr),*) => {
            eprintln!(concat!("INFO  ", $fmt), $($arg),*);
        };
    }
    #[allow(unused)]
    #[macro_export]
    macro_rules! direct_warn {
        ($fmt:expr) => {
            eprintln!(concat!("WARN  ", $fmt));
        };
        ($fmt:expr, $($arg:expr),*) => {
            eprintln!(concat!("WARN  ", $fmt), $($arg),*);
        };
    }
    #[allow(unused)]
    #[macro_export]
    macro_rules! direct_error {
        ($fmt:expr) => {
            eprintln!(concat!("ERROR ", $fmt));
        };
        ($fmt:expr, $($arg:expr),*) => {
            eprintln!(concat!("ERROR ", $fmt), $($arg),*);
        };
    }
}
