use async_channel::Send;
use async_channel::Sender;
use err::thiserror;
use futures_util::Future;
use futures_util::FutureExt;
use pin_project::pin_project;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::ptr::NonNull;
use std::task::Context;
use std::task::Poll;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error<T> {
    NoSendInProgress,
    Closed(T),
}

#[pin_project]
pub struct SenderPolling<T>
where
    T: 'static,
{
    sender: Option<Box<Sender<T>>>,
    sender_ptr: NonNull<Sender<T>>,
    fut: Option<Send<'static, T>>,
    _pin: PhantomPinned,
}

unsafe impl<T> core::marker::Send for SenderPolling<T> where T: core::marker::Send {}

impl<T> SenderPolling<T> {
    pub fn new(sender: Sender<T>) -> Self {
        let mut ret = Self {
            sender: Some(Box::new(sender)),
            sender_ptr: NonNull::dangling(),
            fut: None,
            _pin: PhantomPinned,
        };
        ret.sender_ptr = NonNull::from(ret.sender.as_ref().unwrap().as_ref());
        ret
    }

    pub fn is_idle(&self) -> bool {
        self.sender.is_some() && self.fut.is_none()
    }

    pub fn is_sending(&self) -> bool {
        self.fut.is_some()
    }

    pub fn send_pin(self: Pin<&mut Self>, item: T) {
        unsafe { Pin::get_unchecked_mut(self) }.send(item)
    }

    pub fn send(&mut self, item: T) {
        if self.sender.is_none() {
            // panic!("send on dropped sender");
            // TODO
            return;
        }
        let sender = unsafe { self.sender_ptr.as_mut() };
        let s = sender.send(item);
        self.fut = Some(s);
    }

    pub fn close(&self) {
        if let Some(tx) = self.sender.as_ref() {
            tx.close();
        }
    }

    pub fn drop(&mut self) {
        self.sender = None;
        self.fut = None;
    }
}

impl<T> Future for SenderPolling<T> {
    type Output = Result<(), Error<T>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        let this = self.project();
        match this.fut {
            Some(fut) => match fut.poll_unpin(cx) {
                Ready(Ok(())) => {
                    *this.fut = None;
                    Ready(Ok(()))
                }
                Ready(Err(e)) => {
                    *this.fut = None;
                    Ready(Err(Error::Closed(e.0)))
                }
                Pending => Pending,
            },
            None => Ready(Err(Error::NoSendInProgress)),
        }
    }
}
