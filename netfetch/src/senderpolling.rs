use async_channel::Send;
use async_channel::SendError;
use async_channel::Sender;
use err::thiserror;
use futures_util::Future;
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
    #[pin]
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

    pub fn has_sender(&self) -> bool {
        self.sender.is_some()
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

    pub fn drop(self: Pin<&mut Self>) {
        unsafe {
            let this = self.get_unchecked_mut();
            this.fut = None;
            this.sender = None;
        }
    }

    pub fn len(&self) -> Option<usize> {
        self.sender.as_ref().map(|x| x.len())
    }

    pub async fn send_async_pin(self: Pin<&mut Self>, item: T) -> Result<(), SendError<T>> {
        unsafe { Pin::get_unchecked_mut(self) }.send_async(item).await
    }

    pub async fn send_async(&mut self, item: T) -> Result<(), SendError<T>> {
        if self.is_sending() {
            let fut = self.fut.take().unwrap();
            if let Err(e) = fut.await {
                return Err(e);
            }
        }
        self.sender.as_ref().unwrap().send(item).await
    }

    unsafe fn reset_fut(futopt: Pin<&mut Option<Send<'_, T>>>) {
        let y = futopt.get_unchecked_mut();
        let z = y.as_mut().unwrap_unchecked();
        std::ptr::drop_in_place(z);
        std::ptr::write(y, None);
    }

    #[allow(unused)]
    unsafe fn reset_fut_old(futopt: Pin<&mut Option<Send<'_, T>>>) {
        *futopt.get_unchecked_mut() = None;
    }
}

impl<T> Future for SenderPolling<T>
where
    T: Unpin,
{
    type Output = Result<(), Error<T>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        let mut this = self.project();
        match this.fut.as_mut().as_pin_mut() {
            Some(mut fut) => match fut.as_mut().poll(cx) {
                Ready(Ok(())) => {
                    unsafe {
                        Self::reset_fut(this.fut);
                    }
                    Ready(Ok(()))
                }
                Ready(Err(e)) => {
                    unsafe {
                        Self::reset_fut(this.fut);
                    }
                    Ready(Err(Error::Closed(e.0)))
                }
                Pending => Pending,
            },
            None => Ready(Err(Error::NoSendInProgress)),
        }
    }
}

impl<T> Clone for SenderPolling<T> {
    fn clone(&self) -> Self {
        let sender = self.sender.as_ref().unwrap().as_ref().clone();
        SenderPolling::new(sender)
    }
}

impl<T> From<Sender<T>> for SenderPolling<T> {
    fn from(value: Sender<T>) -> Self {
        SenderPolling::new(value)
    }
}
