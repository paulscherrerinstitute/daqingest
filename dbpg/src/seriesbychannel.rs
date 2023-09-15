use async_channel::Receiver;
use async_channel::Sender;
use core::fmt;
use err::thiserror;
use err::ThisError;
use futures_util::Future;
use futures_util::StreamExt;
use log::*;
use md5::Digest;
use netpod::Database;
use series::series::Existence;
use series::SeriesId;
use stats::SeriesByChannelStats;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use taskrun::tokio;
use tokio::task::JoinHandle;
use tokio_postgres::Client as PgClient;
use tokio_postgres::Statement as PgStatement;

#[allow(unused)]
macro_rules! trace2 {
    ($($arg:tt)*) => {
        if false {
            trace!($($arg)*);
        }
    };
}

#[allow(unused)]
macro_rules! trace3 {
    ($($arg:tt)*) => {
        if false {
            trace!($($arg)*);
        }
    };
}

#[derive(Debug, ThisError)]
pub enum Error {
    Postgres(#[from] tokio_postgres::Error),
    CreateSeriesFail,
    SeriesMissing,
    ChannelError,
}

impl From<crate::err::Error> for Error {
    fn from(value: crate::err::Error) -> Self {
        use crate::err::Error as A;
        match value {
            A::Postgres(x) => Self::Postgres(x),
        }
    }
}

pub type BoxedSend = Pin<Box<dyn Future<Output = Result<(), ()>> + Send>>;

pub trait CanSendChannelInfoResult: Sync {
    fn make_send(&self, item: Result<ChannelInfoResult, Error>) -> BoxedSend;
}

pub struct ChannelInfoQuery {
    pub backend: String,
    pub channel: String,
    pub scalar_type: i32,
    pub shape_dims: Vec<i32>,
    pub tx: Pin<Box<dyn CanSendChannelInfoResult + Send>>,
}

impl fmt::Debug for ChannelInfoQuery {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("ChannelInfoQuery")
            .field("backend", &self.backend)
            .field("channel", &self.channel)
            .field("scalar_type", &self.scalar_type)
            .field("shape_dims", &self.shape_dims)
            .finish()
    }
}

struct ChannelInfoResult2 {
    backend: String,
    channel: String,
    series: Existence<SeriesId>,
    tx: Pin<Box<dyn CanSendChannelInfoResult + Send>>,
}

#[derive(Debug)]
pub struct ChannelInfoResult {
    pub backend: String,
    pub channel: String,
    pub series: Existence<SeriesId>,
}

struct Worker {
    pg: PgClient,
    qu_select: PgStatement,
    qu_insert: PgStatement,
    batch_rx: Receiver<Vec<ChannelInfoQuery>>,
    stats: Arc<SeriesByChannelStats>,
}

impl Worker {
    async fn new(
        db: &Database,
        batch_rx: Receiver<Vec<ChannelInfoQuery>>,
        stats: Arc<SeriesByChannelStats>,
    ) -> Result<Self, Error> {
        let (pg, jh) = crate::conn::make_pg_client(db).await?;
        let sql = concat!(
            "with q1 as (select * from unnest($1::text[], $2::text[], $3::int[], $4::text[], $5::int[])",
            " as inp (backend, channel, scalar_type, shape_dims, rid))",
            " select t.series, q1.rid, t.channel from series_by_channel t",
            " join q1 on t.facility = q1.backend and t.channel = q1.channel",
            " and t.scalar_type = q1.scalar_type and t.shape_dims = q1.shape_dims::int[]",
            " and t.agg_kind = 0",
            " order by q1.rid",
        );
        let qu_select = pg.prepare(sql).await?;
        let sql = concat!(
            "with q1 as (select * from unnest($1::text[], $2::text[], $3::int[], $4::text[], $5::bigint[])",
            " as inp (backend, channel, scalar_type, shape_dims, series))",
            " insert into series_by_channel (series, facility, channel, scalar_type, shape_dims, agg_kind)",
            " select series, backend, channel, scalar_type, shape_dims::int[], 0 from q1",
            " on conflict do nothing"
        );
        let qu_insert = pg.prepare(sql).await?;
        let ret = Self {
            pg,
            qu_select,
            qu_insert,
            batch_rx,
            stats,
        };
        Ok(ret)
    }

    async fn select(
        &self,
        batch: Vec<ChannelInfoQuery>,
    ) -> Result<(Vec<ChannelInfoResult2>, Vec<ChannelInfoQuery>), Error> {
        let mut backend = Vec::new();
        let mut channel = Vec::new();
        let mut scalar_type = Vec::new();
        let mut shape_dims = Vec::new();
        let mut shape_dims_str = Vec::new();
        let mut rid = Vec::new();
        let mut tx = Vec::new();
        for (i, e) in batch.into_iter().enumerate() {
            backend.push(e.backend);
            channel.push(e.channel);
            scalar_type.push(e.scalar_type);
            let mut dims = String::with_capacity(32);
            dims.push('{');
            for (i, &v) in e.shape_dims.iter().enumerate() {
                if i > 0 {
                    dims.push(',');
                }
                use std::fmt::Write;
                write!(dims, "{}", v).unwrap();
            }
            dims.push('}');
            shape_dims_str.push(dims);
            shape_dims.push(e.shape_dims);
            rid.push(i as i32);
            tx.push((i as u32, e.tx));
        }
        let rows = self
            .pg
            .query(
                &self.qu_select,
                &[&backend, &channel, &scalar_type, &shape_dims_str, &rid],
            )
            .await?;
        let mut result = Vec::new();
        let mut missing = Vec::new();
        let mut it1 = rows.into_iter();
        let mut e1 = it1.next();
        for (qrid, tx) in tx {
            let i = qrid as usize;
            let found = if let Some(row) = &e1 {
                let rid: i32 = row.get(1);
                if rid as u32 == qrid {
                    let series: i64 = row.get(0);
                    let ch2: String = row.get(2);
                    let series = SeriesId::new(series as _);
                    let res = ChannelInfoResult2 {
                        // TODO take from database query. Needs test.
                        backend: backend[0].clone(),
                        channel: ch2,
                        series: Existence::Existing(series),
                        tx,
                    };
                    result.push(res);
                    e1 = it1.next();
                    None
                } else {
                    Some(tx)
                }
            } else {
                Some(tx)
            };
            if let Some(tx) = found {
                let k = ChannelInfoQuery {
                    backend: backend[i].clone(),
                    channel: channel[i].clone(),
                    scalar_type: scalar_type[i].clone(),
                    shape_dims: shape_dims[i].clone(),
                    tx,
                };
                missing.push(k);
            }
        }
        Ok((result, missing))
    }

    async fn insert_missing(&self, batch: &Vec<ChannelInfoQuery>) -> Result<(), Error> {
        let tsbeg = Instant::now();
        let mut backends = Vec::new();
        let mut channels = Vec::new();
        let mut scalar_types = Vec::new();
        let mut shape_dimss = Vec::new();
        let mut shape_dims_strs = Vec::new();
        let mut hashers = Vec::new();
        for e in batch.into_iter() {
            {
                let mut h = md5::Md5::new();
                h.update(e.backend.as_bytes());
                h.update(e.channel.as_bytes());
                h.update(format!("{:?}", e.scalar_type).as_bytes());
                h.update(format!("{:?}", e.shape_dims).as_bytes());
                hashers.push(h);
            }
            backends.push(&e.backend);
            channels.push(&e.channel);
            scalar_types.push(e.scalar_type);
            let mut dims = String::with_capacity(32);
            dims.push('{');
            for (i, &v) in e.shape_dims.iter().enumerate() {
                if i > 0 {
                    dims.push(',');
                }
                use std::fmt::Write;
                write!(dims, "{}", v).unwrap();
            }
            dims.push('}');
            shape_dims_strs.push(dims);
            shape_dimss.push(&e.shape_dims);
        }
        let mut i1 = 0;
        loop {
            i1 += 1;
            if i1 >= 200 {
                return Err(Error::CreateSeriesFail);
            }
            let mut seriess = Vec::with_capacity(hashers.len());
            let mut all_good = true;
            for h in &mut hashers {
                let mut good = false;
                for _ in 0..400 {
                    h.update(tsbeg.elapsed().subsec_nanos().to_ne_bytes());
                    let f = h.clone().finalize();
                    let series = u64::from_le_bytes(f.as_slice()[0..8].try_into().unwrap());
                    if series >= 100000000000000000 && series <= i64::MAX as u64 {
                        seriess.push(series as i64);
                        good = true;
                        break;
                    }
                }
                if !good {
                    all_good = false;
                    break;
                }
            }
            if !all_good {
                continue;
            }
            self.pg
                .execute(
                    &self.qu_insert,
                    &[&backends, &channels, &scalar_types, &shape_dims_strs, &seriess],
                )
                .await?;
            break;
        }
        Ok(())
    }

    async fn work(&mut self) -> Result<(), Error> {
        while let Some(batch) = self.batch_rx.next().await {
            self.stats.recv_batch().inc();
            self.stats.recv_items().add(batch.len() as _);
            for x in &batch {
                trace3!(
                    "search for {}  {}  {:?}  {:?}",
                    x.backend,
                    x.channel,
                    x.scalar_type,
                    x.shape_dims
                );
            }
            let (res1, missing) = self.select(batch).await?;
            let res3 = if missing.len() > 0 {
                trace2!("missing {}", missing.len());
                for x in &missing {
                    trace2!("insert missing {x:?}");
                }
                let missing_count = missing.len();
                self.insert_missing(&missing).await?;
                let (res2, missing2) = self.select(missing).await?;
                if missing2.len() > 0 {
                    for x in &missing2 {
                        warn!("series ids still missing after insert  {}", x.channel);
                    }
                    Err(Error::SeriesMissing)
                } else {
                    trace2!("select missing after insert  {} of {}", missing_count, res2.len());
                    Ok((res1, res2))
                }
            } else {
                Ok((res1, Vec::new()))
            }?;
            for r in res3.0.into_iter().chain(res3.1.into_iter()) {
                let item = ChannelInfoResult {
                    backend: r.backend,
                    channel: r.channel,
                    series: r.series,
                };
                // trace3!("try to send result for  {:?}", item);
                let fut = r.tx.make_send(Ok(item));
                match fut.await {
                    Ok(()) => {}
                    Err(_e) => {
                        //warn!("can not deliver result");
                        // return Err(Error::ChannelError);
                        self.stats.res_tx_fail.inc();
                    }
                }
            }
        }
        debug!("Worker done");
        Ok(())
    }
}

pub async fn start_lookup_workers(
    worker_count: usize,
    db: &Database,
    stats: Arc<SeriesByChannelStats>,
) -> Result<
    (
        Sender<ChannelInfoQuery>,
        Vec<JoinHandle<Result<(), Error>>>,
        JoinHandle<()>,
    ),
    Error,
> {
    let inp_cap = 128;
    let batch_out_cap = 4;
    let timeout = Duration::from_millis(400);
    let (query_tx, query_rx) = async_channel::bounded(inp_cap);
    let (batch_rx, bjh) = batchtools::batcher::batch(inp_cap, timeout, batch_out_cap, query_rx);
    let mut jhs = Vec::new();
    for _ in 0..worker_count {
        let mut worker = Worker::new(db, batch_rx.clone(), stats.clone()).await?;
        let jh = tokio::task::spawn(async move { worker.work().await });
        jhs.push(jh);
    }
    Ok((query_tx, jhs, bjh))
}
