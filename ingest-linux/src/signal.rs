use log::*;
use std::ffi::CStr;
use std::mem::MaybeUninit;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("{self}")]
pub enum Error {
    SignalHandlerSet,
    SignalHandlerUnset,
}

pub fn set_signal_handler(
    signum: libc::c_int,
    cb: fn(libc::c_int, *const libc::siginfo_t, *const libc::c_void) -> (),
) -> Result<(), Error> {
    //let cb: fn(libc::c_int, *const libc::siginfo_t, *const libc::c_void) -> () = handler_sigaction;
    // Safe because it creates a valid value:
    let mask: libc::sigset_t = unsafe { MaybeUninit::zeroed().assume_init() };
    let sa_sigaction: libc::sighandler_t = cb as *const libc::c_void as _;
    let act = libc::sigaction {
        sa_sigaction,
        sa_mask: mask,
        sa_flags: 0,
        sa_restorer: None,
    };
    let (ec, msg) = unsafe {
        let ec = libc::sigaction(signum, &act, std::ptr::null_mut());
        let errno = *libc::__errno_location();
        (ec, CStr::from_ptr(libc::strerror(errno)))
    };
    if ec != 0 {
        // Not valid to print here, but we will panic anyways after that.
        eprintln!("error: {:?}", msg);
        return Err(Error::SignalHandlerSet);
    }
    Ok(())
}

pub fn unset_signal_handler(signum: libc::c_int) -> Result<(), Error> {
    // Safe because it creates a valid value:
    let mask: libc::sigset_t = unsafe { MaybeUninit::zeroed().assume_init() };
    let act = libc::sigaction {
        sa_sigaction: libc::SIG_DFL,
        sa_mask: mask,
        sa_flags: 0,
        sa_restorer: None,
    };
    let (ec, msg) = unsafe {
        let ec = libc::sigaction(signum, &act, std::ptr::null_mut());
        let errno = *libc::__errno_location();
        (ec, CStr::from_ptr(libc::strerror(errno)))
    };
    if ec != 0 {
        // Not valid to print here, but we will panic anyways after that.
        eprintln!("error: {:?}", msg);
        return Err(Error::SignalHandlerUnset);
    }
    Ok(())
}
