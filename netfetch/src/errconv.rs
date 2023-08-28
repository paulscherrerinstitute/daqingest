use async_channel::RecvError;
use async_channel::SendError;
use err::Error;

pub trait ErrConv<T> {
    fn err_conv(self) -> Result<T, Error>;
}

impl<T, H> ErrConv<T> for Result<T, SendError<H>> {
    fn err_conv(self) -> Result<T, Error> {
        match self {
            Ok(k) => Ok(k),
            Err(e) => Err(Error::with_msg_no_trace(format!("{e:?}"))),
        }
    }
}

impl<T> ErrConv<T> for Result<T, RecvError> {
    fn err_conv(self) -> Result<T, Error> {
        match self {
            Ok(k) => Ok(k),
            Err(e) => Err(Error::with_msg_no_trace(format!("{e:?}"))),
        }
    }
}
