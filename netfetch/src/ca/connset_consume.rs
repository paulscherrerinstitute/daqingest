use crate::ca::conn::CaConnEvent;
use async_channel::Receiver;
use async_channel::Sender;
use err::thiserror;
use log::*;
use std::net::SocketAddr;
use taskrun::spawn;
use taskrun::tokio::task::JoinHandle;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {}

pub struct ConnSetConsumeInp(Receiver<CaConnEvent>);

pub struct ConnSetConsume {
    inp: Receiver<(SocketAddr, CaConnEvent)>,
}

impl ConnSetConsume {
    pub fn new(inp: Receiver<(SocketAddr, CaConnEvent)>) -> (JoinHandle<Result<(), Error>>,) {
        let ret = Self { inp };
        let jh = spawn(ret.run());
        (jh,)
    }

    fn handle_event(&mut self, addr: SocketAddr, ev: CaConnEvent) {}

    async fn run(mut self) -> Result<(), Error> {
        loop {
            match self.inp.recv().await {
                Ok((addr, item)) => self.handle_event(addr, item),
                Err(e) => {
                    error!("{e}");
                    break;
                }
            }
        }
        Ok(())
    }
}
