use err::Error;
use tokio::io::ReadBuf;

pub struct NetBuf {
    buf: Vec<u8>,
    wp: usize,
    rp: usize,
}

macro_rules! check_invariants {
    ($self:expr) => {
        //$self.check_invariants()
    };
}

impl NetBuf {
    pub fn new(cap: usize) -> Self {
        Self {
            buf: vec![0; cap],
            wp: 0,
            rp: 0,
        }
    }

    pub fn state(&self) -> (usize, usize) {
        (self.rp, self.wp)
    }

    pub fn len(&self) -> usize {
        check_invariants!(self);
        self.wp - self.rp
    }

    #[inline(always)]
    pub fn cap(&self) -> usize {
        check_invariants!(self);
        self.buf.len()
    }

    pub fn wcap(&self) -> usize {
        check_invariants!(self);
        self.buf.len() - self.wp
    }

    pub fn data(&self) -> &[u8] {
        check_invariants!(self);
        &self.buf[self.rp..self.wp]
    }

    pub fn adv(&mut self, x: usize) -> Result<(), Error> {
        check_invariants!(self);
        if self.len() < x {
            return Err(Error::with_msg_no_trace("not enough bytes"));
        } else {
            self.rp += x;
            Ok(())
        }
    }

    pub fn wadv(&mut self, x: usize) -> Result<(), Error> {
        check_invariants!(self);
        if self.wcap() < x {
            return Err(Error::with_msg_no_trace("not enough space"));
        } else {
            self.wp += x;
            Ok(())
        }
    }

    pub fn read_u8(&mut self) -> Result<u8, Error> {
        check_invariants!(self);
        type T = u8;
        const TS: usize = std::mem::size_of::<T>();
        if self.len() < TS {
            return Err(Error::with_msg_no_trace("not enough bytes"));
        } else {
            let val = self.buf[self.rp];
            self.rp += TS;
            Ok(val)
        }
    }

    pub fn read_u16_be(&mut self) -> Result<u16, Error> {
        check_invariants!(self);
        type T = u16;
        const TS: usize = std::mem::size_of::<T>();
        if self.len() < TS {
            return Err(Error::with_msg_no_trace("not enough bytes"));
        } else {
            let val = T::from_be_bytes(self.buf[self.rp..self.rp + TS].try_into()?);
            self.rp += TS;
            Ok(val)
        }
    }

    pub fn read_u32_be(&mut self) -> Result<u32, Error> {
        check_invariants!(self);
        type T = u32;
        const TS: usize = std::mem::size_of::<T>();
        if self.len() < TS {
            return Err(Error::with_msg_no_trace("not enough bytes"));
        } else {
            let val = T::from_be_bytes(self.buf[self.rp..self.rp + TS].try_into()?);
            self.rp += TS;
            Ok(val)
        }
    }

    pub fn read_u64(&mut self) -> Result<u64, Error> {
        check_invariants!(self);
        type T = u64;
        const TS: usize = std::mem::size_of::<T>();
        if self.len() < TS {
            return Err(Error::with_msg_no_trace("not enough bytes"));
        } else {
            let val = T::from_be_bytes(self.buf[self.rp..self.rp + TS].try_into()?);
            self.rp += TS;
            Ok(val)
        }
    }

    pub fn read_bytes(&mut self, n: usize) -> Result<&[u8], Error> {
        check_invariants!(self);
        if self.len() < n {
            return Err(Error::with_msg_no_trace("not enough bytes"));
        } else {
            let val = self.buf[self.rp..self.rp + n].as_ref();
            self.rp += n;
            Ok(val)
        }
    }

    pub fn read_buf_for_fill(&mut self, need_min: usize) -> ReadBuf {
        check_invariants!(self);
        self.rewind_if_needed(need_min);
        let read_buf = ReadBuf::new(&mut self.buf[self.wp..]);
        read_buf
    }

    pub fn write_buf(&mut self, n: usize) -> Result<&mut [u8], Error> {
        check_invariants!(self);
        self.rewind_if_needed(n);
        if self.wcap() < n {
            Err(Error::with_msg_no_trace("write_buf not enough space"))
        } else {
            let ret = &mut self.buf[self.wp..self.wp + n];
            self.wp += n;
            Ok(ret)
        }
    }

    #[inline(always)]
    pub fn rewind_if_needed(&mut self, need_min: usize) {
        check_invariants!(self);
        if self.rp != 0 && self.rp == self.wp {
            self.rp = 0;
            self.wp = 0;
        } else if self.cap() - self.rp < need_min {
            self.buf.copy_within(self.rp..self.wp, 0);
            self.wp -= self.rp;
            self.rp = 0;
        }
    }

    pub fn put_slice(&mut self, buf: &[u8]) -> Result<(), Error> {
        check_invariants!(self);
        self.rewind_if_needed(buf.len());
        if self.wcap() < buf.len() {
            return Err(Error::with_msg_no_trace("not enough space"));
        } else {
            self.buf[self.wp..self.wp + buf.len()].copy_from_slice(buf);
            self.wp += buf.len();
            Ok(())
        }
    }

    pub fn put_u8(&mut self, v: u8) -> Result<(), Error> {
        check_invariants!(self);
        type T = u8;
        const TS: usize = std::mem::size_of::<T>();
        self.rewind_if_needed(TS);
        if self.wcap() < TS {
            return Err(Error::with_msg_no_trace("not enough space"));
        } else {
            self.buf[self.wp..self.wp + TS].copy_from_slice(&v.to_be_bytes());
            self.wp += TS;
            Ok(())
        }
    }

    pub fn put_u64(&mut self, v: u64) -> Result<(), Error> {
        check_invariants!(self);
        type T = u64;
        const TS: usize = std::mem::size_of::<T>();
        self.rewind_if_needed(TS);
        if self.wcap() < TS {
            return Err(Error::with_msg_no_trace("not enough space"));
        } else {
            self.buf[self.wp..self.wp + TS].copy_from_slice(&v.to_be_bytes());
            self.wp += TS;
            Ok(())
        }
    }

    #[allow(unused)]
    fn check_invariants(&self) {
        if self.wp > self.buf.len() {
            eprintln!("ERROR  netbuf  wp {}  rp {}", self.wp, self.rp);
            std::process::exit(87);
        }
        if self.rp > self.wp {
            eprintln!("ERROR  netbuf  wp {}  rp {}", self.wp, self.rp);
            std::process::exit(87);
        }
    }
}
