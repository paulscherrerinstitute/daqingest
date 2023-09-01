use std::fmt;

#[derive(Debug)]
pub enum Error {
    NotEnoughBytes,
    NotEnoughSpace(usize, usize, usize),
    TryFromSliceError,
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{self:?}")
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

impl From<std::array::TryFromSliceError> for Error {
    fn from(_: std::array::TryFromSliceError) -> Self {
        Self::TryFromSliceError
    }
}

pub struct SlideBuf {
    buf: Vec<u8>,
    wp: usize,
    rp: usize,
}

macro_rules! check_invariants {
    ($self:expr) => {
        //$self.check_invariants()
    };
}

impl SlideBuf {
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

    pub fn data_mut(&mut self) -> &mut [u8] {
        check_invariants!(self);
        &mut self.buf[self.rp..self.wp]
    }

    pub fn reset(&mut self) {
        self.rp = 0;
        self.wp = 0;
    }

    pub fn adv(&mut self, x: usize) -> Result<(), Error> {
        check_invariants!(self);
        if self.len() < x {
            return Err(Error::NotEnoughBytes);
        } else {
            self.rp += x;
            Ok(())
        }
    }

    pub fn wadv(&mut self, x: usize) -> Result<(), Error> {
        check_invariants!(self);
        if self.wcap() < x {
            self.rewind();
        }
        if self.wcap() < x {
            return Err(Error::NotEnoughSpace(self.cap(), self.wcap(), x));
        } else {
            self.wp += x;
            Ok(())
        }
    }

    pub fn rp(&self) -> usize {
        self.rp
    }

    pub fn set_rp(&mut self, rp: usize) -> Result<(), Error> {
        check_invariants!(self);
        if rp > self.wp {
            Err(Error::NotEnoughBytes)
        } else {
            self.rp = rp;
            Ok(())
        }
    }

    pub fn rewind_rp(&mut self, n: usize) -> Result<(), Error> {
        check_invariants!(self);
        if self.rp < n {
            Err(Error::NotEnoughBytes)
        } else {
            self.rp -= n;
            Ok(())
        }
    }

    pub fn read_u8(&mut self) -> Result<u8, Error> {
        check_invariants!(self);
        type T = u8;
        const TS: usize = std::mem::size_of::<T>();
        if self.len() < TS {
            return Err(Error::NotEnoughBytes);
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
            return Err(Error::NotEnoughBytes);
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
            return Err(Error::NotEnoughBytes);
        } else {
            let val = T::from_be_bytes(self.buf[self.rp..self.rp + TS].try_into()?);
            self.rp += TS;
            Ok(val)
        }
    }

    pub fn read_u64_be(&mut self) -> Result<u64, Error> {
        check_invariants!(self);
        type T = u64;
        const TS: usize = std::mem::size_of::<T>();
        if self.len() < TS {
            return Err(Error::NotEnoughBytes);
        } else {
            let val = T::from_be_bytes(self.buf[self.rp..self.rp + TS].try_into()?);
            self.rp += TS;
            Ok(val)
        }
    }

    pub fn read_i32_be(&mut self) -> Result<i32, Error> {
        check_invariants!(self);
        type T = i32;
        const TS: usize = std::mem::size_of::<T>();
        if self.len() < TS {
            return Err(Error::NotEnoughBytes);
        } else {
            let val = T::from_be_bytes(self.buf[self.rp..self.rp + TS].try_into()?);
            self.rp += TS;
            Ok(val)
        }
    }

    pub fn read_i64_be(&mut self) -> Result<i64, Error> {
        check_invariants!(self);
        type T = i64;
        const TS: usize = std::mem::size_of::<T>();
        if self.len() < TS {
            return Err(Error::NotEnoughBytes);
        } else {
            let val = T::from_be_bytes(self.buf[self.rp..self.rp + TS].try_into()?);
            self.rp += TS;
            Ok(val)
        }
    }

    pub fn read_f32_be(&mut self) -> Result<f32, Error> {
        check_invariants!(self);
        type T = f32;
        const TS: usize = std::mem::size_of::<T>();
        if self.len() < TS {
            return Err(Error::NotEnoughBytes);
        } else {
            let val = T::from_be_bytes(self.buf[self.rp..self.rp + TS].try_into()?);
            self.rp += TS;
            Ok(val)
        }
    }

    pub fn read_f64_be(&mut self) -> Result<f64, Error> {
        check_invariants!(self);
        type T = f64;
        const TS: usize = std::mem::size_of::<T>();
        if self.len() < TS {
            return Err(Error::NotEnoughBytes);
        } else {
            let val = T::from_be_bytes(self.buf[self.rp..self.rp + TS].try_into()?);
            self.rp += TS;
            Ok(val)
        }
    }

    pub fn read_bytes(&mut self, n: usize) -> Result<&[u8], Error> {
        check_invariants!(self);
        if self.len() < n {
            return Err(Error::NotEnoughBytes);
        } else {
            let val = self.buf[self.rp..self.rp + n].as_ref();
            self.rp += n;
            Ok(val)
        }
    }

    /*pub fn read_buf_for_fill(&mut self, need_min: usize) -> ReadBuf {
        check_invariants!(self);
        self.rewind_if_needed(need_min);
        let read_buf = ReadBuf::new(&mut self.buf[self.wp..]);
        read_buf
    }*/

    // TODO issue is that this return exactly the size that was asked for,
    // but most of time, we want to first get some scratch space, and later
    // advance the write pointer.
    pub fn ___write_buf___(&mut self, n: usize) -> Result<&mut [u8], Error> {
        check_invariants!(self);
        self.rewind_if_needed(n);
        if self.wcap() < n {
            self.rewind();
        }
        if self.wcap() < n {
            Err(Error::NotEnoughSpace(self.cap(), self.wcap(), n))
        } else {
            let ret = &mut self.buf[self.wp..self.wp + n];
            self.wp += n;
            Ok(ret)
        }
    }

    #[inline(always)]
    pub fn rewind(&mut self) {
        self.buf.copy_within(self.rp..self.wp, 0);
        self.wp -= self.rp;
        self.rp = 0;
    }

    #[inline(always)]
    pub fn rewind_if_needed(&mut self, need_min: usize) {
        check_invariants!(self);
        if self.rp != 0 && self.rp == self.wp {
            self.rp = 0;
            self.wp = 0;
        } else if self.cap() < self.rp + need_min {
            self.rewind();
        }
    }

    pub fn available_writable_area(&mut self, need_min: usize) -> Result<&mut [u8], Error> {
        check_invariants!(self);
        self.rewind_if_needed(need_min);
        if self.wcap() < need_min {
            self.rewind();
        }
        if self.wcap() < need_min {
            Err(Error::NotEnoughSpace(self.cap(), self.wcap(), need_min))
        } else {
            let ret = &mut self.buf[self.wp..];
            Ok(ret)
        }
    }

    pub fn put_slice(&mut self, buf: &[u8]) -> Result<(), Error> {
        check_invariants!(self);
        self.rewind_if_needed(buf.len());
        if self.wcap() < buf.len() {
            self.rewind();
        }
        if self.wcap() < buf.len() {
            return Err(Error::NotEnoughSpace(self.cap(), self.wcap(), buf.len()));
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
            self.rewind();
        }
        if self.wcap() < TS {
            return Err(Error::NotEnoughSpace(self.cap(), self.wcap(), TS));
        } else {
            self.buf[self.wp..self.wp + TS].copy_from_slice(&v.to_be_bytes());
            self.wp += TS;
            Ok(())
        }
    }

    pub fn put_u16_be(&mut self, v: u16) -> Result<(), Error> {
        check_invariants!(self);
        type T = u16;
        const TS: usize = std::mem::size_of::<T>();
        self.rewind_if_needed(TS);
        if self.wcap() < TS {
            self.rewind();
        }
        if self.wcap() < TS {
            return Err(Error::NotEnoughSpace(self.cap(), self.wcap(), TS));
        } else {
            self.buf[self.wp..self.wp + TS].copy_from_slice(&v.to_be_bytes());
            self.wp += TS;
            Ok(())
        }
    }

    pub fn put_u32_be(&mut self, v: u32) -> Result<(), Error> {
        check_invariants!(self);
        type T = u32;
        const TS: usize = std::mem::size_of::<T>();
        self.rewind_if_needed(TS);
        if self.wcap() < TS {
            self.rewind();
        }
        if self.wcap() < TS {
            return Err(Error::NotEnoughSpace(self.cap(), self.wcap(), TS));
        } else {
            self.buf[self.wp..self.wp + TS].copy_from_slice(&v.to_be_bytes());
            self.wp += TS;
            Ok(())
        }
    }

    pub fn put_u64_be(&mut self, v: u64) -> Result<(), Error> {
        check_invariants!(self);
        type T = u64;
        const TS: usize = std::mem::size_of::<T>();
        self.rewind_if_needed(TS);
        if self.wcap() < TS {
            self.rewind();
        }
        if self.wcap() < TS {
            return Err(Error::NotEnoughSpace(self.cap(), self.wcap(), TS));
        } else {
            self.buf[self.wp..self.wp + TS].copy_from_slice(&v.to_be_bytes());
            self.wp += TS;
            Ok(())
        }
    }

    pub fn put_f32_be(&mut self, v: f32) -> Result<(), Error> {
        check_invariants!(self);
        type T = f32;
        const TS: usize = std::mem::size_of::<T>();
        self.rewind_if_needed(TS);
        if self.wcap() < TS {
            self.rewind();
        }
        if self.wcap() < TS {
            return Err(Error::NotEnoughSpace(self.cap(), self.wcap(), TS));
        } else {
            self.buf[self.wp..self.wp + TS].copy_from_slice(&v.to_be_bytes());
            self.wp += TS;
            Ok(())
        }
    }

    pub fn put_f64_be(&mut self, v: f64) -> Result<(), Error> {
        check_invariants!(self);
        type T = f64;
        const TS: usize = std::mem::size_of::<T>();
        self.rewind_if_needed(TS);
        if self.wcap() < TS {
            self.rewind();
        }
        if self.wcap() < TS {
            return Err(Error::NotEnoughSpace(self.cap(), self.wcap(), TS));
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

impl fmt::Debug for SlideBuf {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("SlideBuf")
            .field("cap", &self.cap())
            .field("wp", &self.wp)
            .field("rp", &self.rp)
            .finish()
    }
}
