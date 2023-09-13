use super::connset::CaConnSetEvent;
use super::findioc::FindIocRes;
use crate::ca::connset::ConnSetCmd;
use async_channel::Receiver;
use dbpg::seriesbychannel::ChannelInfoResult;
use err::Error;
use futures_util::StreamExt;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

pub struct InputMerge {
    inp1: Option<Receiver<CaConnSetEvent>>,
    inp2: Option<Receiver<VecDeque<FindIocRes>>>,
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

impl futures_util::Stream for InputMerge {
    type Item = CaConnSetEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let ret = {
            if let Some(inp) = &mut self.inp3 {
                match inp.poll_next_unpin(cx) {
                    Ready(Some(x)) => Some(CaConnSetEvent::ConnSetCmd(todo!())),
                    Ready(None) => {
                        self.inp2 = None;
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
            if let Some(inp) = &mut self.inp2 {
                match inp.poll_next_unpin(cx) {
                    Ready(Some(x)) => Some(CaConnSetEvent::ConnSetCmd(todo!())),
                    Ready(None) => {
                        self.inp2 = None;
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
            if let Some(inp) = &mut self.inp1 {
                match inp.poll_next_unpin(cx) {
                    Ready(Some(x)) => Ready(Some(x)),
                    Ready(None) => {
                        self.inp1 = None;
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
