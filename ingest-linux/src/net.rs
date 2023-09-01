use std::ffi::CStr;
use taskrun::tokio;
use thiserror::Error;
use tokio::net::TcpStream;

#[derive(Debug, Error)]
#[error("{self}")]
pub enum Error {
    SocketOptionSet,
    SocketOptionGet,
}

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
            // error!("ec {ec}");
            return Err(Error::SocketOptionSet);
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
            let _es = CStr::from_ptr(libc::strerror(errno));
            // error!("can not query socket option  ec {ec}  errno {errno}  es {es:?}");
            Err(Error::SocketOptionGet)
        } else {
            if (n as u32) < rcvbuf * 5 / 6 {
                // warn!("SO_RCVBUF {n}  smaller than requested {rcvbuf}");
            } else {
                // info!("SO_RCVBUF {n}");
            }
            Ok(())
        }
    }
}
