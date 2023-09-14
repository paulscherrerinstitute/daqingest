use crate::conn::PgClient;
use async_channel::Receiver;
use async_channel::RecvError;
use async_channel::SendError;
use async_channel::Sender;
use err::thiserror;
use err::ThisError;
use log::*;
use netpod::Database;

#[derive(Debug, ThisError)]
pub enum Error {
    Postgres(#[from] tokio_postgres::Error),
    EndOfPool,
    ChannelRecv(#[from] RecvError),
    ChannelSend,
}

impl From<crate::err::Error> for Error {
    fn from(value: crate::err::Error) -> Self {
        type G = crate::err::Error;
        match value {
            G::Postgres(e) => Error::Postgres(e),
        }
    }
}

impl<T> From<SendError<T>> for Error {
    fn from(_: SendError<T>) -> Self {
        Self::ChannelSend
    }
}

struct PgClientInner {
    pgc: PgClient,
    handout_count: u64,
}

pub struct PgClientPooled {
    pgc: Option<PgClientInner>,
    tx: Sender<PgClientInner>,
}

impl Drop for PgClientPooled {
    fn drop(&mut self) {
        // async: the channel capacity is chosen large enough so
        // all pooled connections have space.
        match self.tx.try_send(self.pgc.take().unwrap()) {
            Ok(()) => {}
            Err(e) => {
                use async_channel::TrySendError;
                match e {
                    TrySendError::Full(_) => {
                        error!("can not return pooled database handle, pool is blocked");
                    }
                    TrySendError::Closed(_) => {
                        warn!("can not return pooled database handle, pool is closed")
                    }
                }
            }
        }
    }
}

impl PgClientPooled {
    pub fn pgc(&self) -> &PgClient {
        &self.pgc.as_ref().unwrap().pgc
    }
}

pub struct PgPool {
    tx: Sender<PgClientInner>,
    rx: Receiver<PgClientInner>,
    #[allow(unused)]
    handout_count: u64,
}

impl PgPool {
    pub async fn new(cap: usize, dbconf: &Database) -> Result<Self, Error> {
        let (tx, rx) = async_channel::bounded(2 + cap);
        for _ in 0..cap {
            let (pgc, jh) = crate::conn::make_pg_client(dbconf).await?;
            let pgc = PgClientInner { pgc, handout_count: 0 };
            tx.send(pgc).await?;
        }
        let ret = PgPool {
            tx,
            rx,
            handout_count: 0,
        };
        Ok(ret)
    }

    pub async fn conn(&self) -> Result<PgClientPooled, Error> {
        let mut pgc = self.rx.recv().await?;
        pgc.handout_count += 1;
        let ret = PgClientPooled {
            pgc: Some(pgc),
            tx: self.tx.clone(),
        };
        is_it_send(&ret);
        is_it_sync(&ret);
        let mut ret = ret;
        is_it_mut_sync(&mut ret);
        Ok(ret)
    }
}

fn is_it_send<T: Send>(_: &T) {}

fn is_it_sync<T: Sync>(_: &T) {}

fn is_it_mut_sync<T: Sync>(_: &mut T) {}
