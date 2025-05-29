#![allow(unused_imports)]
// pub use tracing::debug;
// pub use tracing::error;
// pub use tracing::info;
// pub use tracing::trace;
// pub use tracing::warn;

pub use direct_debug as debug;
pub use direct_error as error;
pub use direct_info as info;
pub use direct_trace as trace;
pub use direct_warn as warn;

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
            // eprintln!(concat!("DEBUG ", $fmt));
            // eprintln!("DEBUG {}", format_args!($fmt));
            eprintln!("{}", format_args!(concat!("DEBUG ", $fmt)));
        };
        ($fmt:expr, $($arg:expr),*) => {
            // eprintln!(concat!("DEBUG ", $fmt), $($arg),*);
            // eprintln!("DEBUG {}", format_args!($fmt, $($arg),*));
            eprintln!("{}", format_args!(concat!("DEBUG ", $fmt), $($arg),*));
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
