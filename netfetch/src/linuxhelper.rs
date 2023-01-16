use err::Error;
use log::*;
use std::ffi::CStr;
use std::mem::MaybeUninit;
use tokio::net::TcpStream;

pub fn local_hostname() -> String {
    let mut buf = vec![0u8; 128];
    let hostname = unsafe {
        let ec = libc::gethostname(buf.as_mut_ptr() as _, buf.len() - 2);
        if ec != 0 {
            panic!();
        }
        let hostname = CStr::from_ptr(&buf[0] as *const _ as _);
        hostname.to_str().unwrap()
    };
    hostname.into()
}

#[test]
fn test_get_local_hostname() {
    assert_ne!(local_hostname().len(), 0);
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
        panic!();
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
        panic!();
    }
    Ok(())
}

pub fn set_rcv_sock_opts(conn: &mut TcpStream, rcvbuf: u32) -> Result<(), Error> {
    use std::mem::size_of;
    use std::os::unix::prelude::AsRawFd;
    let fd = conn.as_raw_fd();
    unsafe {
        type N = libc::c_int;
        let n: N = rcvbuf as _;
        let ec = libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            &n as *const N as _,
            size_of::<N>() as _,
        );
        if ec != 0 {
            error!("ec {ec}");
            if ec != 0 {
                return Err(Error::with_msg_no_trace(format!("can not set socket option")));
            }
        }
    }
    unsafe {
        type N = libc::c_int;
        let mut n: N = -1;
        let mut l = size_of::<N>() as libc::socklen_t;
        let ec = libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            &mut n as *mut N as _,
            &mut l as _,
        );
        if ec != 0 {
            let errno = *libc::__errno_location();
            let es = CStr::from_ptr(libc::strerror(errno));
            error!("can not query socket option  ec {ec}  errno {errno}  es {es:?}");
        } else {
            if (n as u32) < rcvbuf * 5 / 6 {
                warn!("SO_RCVBUF {n}  smaller than requested {rcvbuf}");
            } else {
                info!("SO_RCVBUF {n}");
            }
        }
    }
    Ok(())
}
