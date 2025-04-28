use super::connset::CaConnSetEvent;
use super::findioc::FindIocRes;
use async_channel::Receiver;
use dbpg::seriesbychannel::ChannelInfoResult;
use err::Error;
use futures_util::Stream;
use pin_project::pin_project;
use std::collections::VecDeque;
use std::pin::Pin;
use std::pin::pin;
use std::task::Context;
use std::task::Poll;

#[pin_project]
pub struct InputMerge {
    #[pin]
    inp1: Option<Receiver<CaConnSetEvent>>,
    #[pin]
    inp2: Option<Receiver<VecDeque<FindIocRes>>>,
    #[pin]
    inp3: Option<Receiver<Result<ChannelInfoResult, Error>>>,
}

impl InputMerge {
    pub fn new(
        inp1: Receiver<CaConnSetEvent>,
        inp2: Receiver<VecDeque<FindIocRes>>,
        inp3: Receiver<Result<ChannelInfoResult, Error>>,
    ) -> Self {
        Self {
            inp1: Some(inp1),
            inp2: Some(inp2),
            inp3: Some(inp3),
        }
    }

    pub fn close(&mut self) {
        if let Some(x) = self.inp1.as_ref() {
            x.close();
        }
    }
}

fn todoval<T>() -> T {
    todo!()
}

impl Stream for InputMerge {
    type Item = CaConnSetEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let ret = {
            let mut selfp = self.as_mut().project();
            if let Some(inp) = selfp.inp3.as_mut().as_pin_mut() {
                match inp.poll_next(cx) {
                    Ready(Some(_)) => Some(CaConnSetEvent::ConnSetCmd(todoval())),
                    Ready(None) => {
                        unsafe {
                            // TODO what guarantees that I can drop the content here like this?
                            self.as_mut().get_unchecked_mut().inp3 = None;
                        }
                        None
                    }
                    Pending => None,
                }
            } else {
                None
            }
        };
        let ret = if let Some(x) = ret {
            Some(x)
        } else {
            let mut selfp = self.as_mut().project();
            if let Some(inp) = selfp.inp2.as_mut().as_pin_mut() {
                match inp.poll_next(cx) {
                    Ready(Some(_)) => Some(CaConnSetEvent::ConnSetCmd(todoval())),
                    Ready(None) => {
                        unsafe {
                            // TODO what guarantees that I can drop the content here like this?
                            self.as_mut().get_unchecked_mut().inp2 = None;
                        }
                        None
                    }
                    Pending => None,
                }
            } else {
                None
            }
        };
        if let Some(x) = ret {
            Ready(Some(x))
        } else {
            let mut selfp = self.as_mut().project();
            if let Some(inp) = selfp.inp1.as_mut().as_pin_mut() {
                match inp.poll_next(cx) {
                    Ready(Some(x)) => Ready(Some(x)),
                    Ready(None) => {
                        unsafe {
                            // TODO what guarantees that I can drop the content here like this?
                            self.as_mut().get_unchecked_mut().inp1 = None;
                        }
                        Ready(None)
                    }
                    Pending => Pending,
                }
            } else {
                Ready(None)
            }
        }
    }
}
