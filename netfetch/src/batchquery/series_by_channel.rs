use crate::batcher;
use crate::dbpg::make_pg_client;
use crate::series::Existence;
use crate::series::SeriesId;
use async_channel::Receiver;
use async_channel::Sender;
use err::Error;
use futures_util::StreamExt;
use netpod::log::*;
use netpod::Database;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_postgres::Client as PgClient;
use tokio_postgres::Statement as PgStatement;

pub struct ChannelInfoQuery {
    pub backend: String,
    pub channel: String,
    pub scalar_type: i32,
    pub shape_dims: Vec<i32>,
    pub tx: Sender<Result<Existence<SeriesId>, Error>>,
}

impl ChannelInfoQuery {
    pub fn dummy(&self) -> Self {
        Self {
            backend: String::new(),
            channel: String::new(),
            scalar_type: 4242,
            shape_dims: Vec::new(),
            tx: self.tx.clone(),
        }
    }
}

struct ChannelInfoResult {
    series: Vec<Existence<SeriesId>>,
    tx: Vec<Sender<Result<Existence<SeriesId>, Error>>>,
}

struct PgRes {
    pgc: PgClient,
    st: PgStatement,
}

async fn prepare_pgcs(sql: &str, pgcn: usize, db: &Database) -> Result<(Sender<PgRes>, Receiver<PgRes>), Error> {
    let (pgc_tx, pgc_rx) = async_channel::bounded(pgcn);
    for _ in 0..pgcn {
        let pgc = make_pg_client(&db).await?;
        let st = pgc.prepare(sql).await.map_err(|e| Error::from(e.to_string()))?;
        let k = PgRes { pgc, st };
        match pgc_tx.send(k).await {
            Ok(_) => {}
            Err(e) => {
                error!("can not enqueue pgc {e}");
            }
        }
    }
    Ok((pgc_tx, pgc_rx))
}

async fn fetch_data(batch: Vec<ChannelInfoQuery>, pgres: PgRes) -> Result<(ChannelInfoResult, PgRes), Error> {
    let mut backend = Vec::new();
    let mut channel = Vec::new();
    let mut scalar_type = Vec::new();
    let mut shape_dims: Vec<String> = Vec::new();
    let mut rid = Vec::new();
    let mut tx = Vec::new();
    for (i, e) in batch.into_iter().enumerate() {
        backend.push(e.backend);
        channel.push(e.channel);
        scalar_type.push(e.scalar_type);
        let mut dims = String::with_capacity(16);
        dims.push('{');
        for (i, v) in e.shape_dims.into_iter().enumerate() {
            if i > 0 {
                dims.push(',');
            }
            use std::fmt::Write;
            write!(dims, "{}", v).unwrap();
        }
        dims.push('}');
        shape_dims.push(dims);
        rid.push(i as i32);
        tx.push((i as u32, e.tx));
    }
    match pgres
        .pgc
        .query(&pgres.st, &[&backend, &channel, &scalar_type, &shape_dims, &rid])
        .await
        .map_err(|e| {
            error!("{e}");
            Error::from(e.to_string())
        }) {
        Ok(rows) => {
            let mut series_ids = Vec::new();
            let mut txs = Vec::new();
            let mut it1 = rows.into_iter();
            let mut e1 = it1.next();
            for (qrid, tx) in tx {
                if let Some(row) = &e1 {
                    let rid: i32 = row.get(1);
                    if rid as u32 == qrid {
                        let series: i64 = row.get(0);
                        let series = SeriesId::new(series as _);
                        series_ids.push(Existence::Existing(series));
                        txs.push(tx);
                    }
                    e1 = it1.next();
                }
            }
            let result = ChannelInfoResult {
                series: series_ids,
                tx: txs,
            };
            Ok((result, pgres))
        }
        Err(e) => {
            error!("error in pg query {e}");
            tokio::time::sleep(Duration::from_millis(2000)).await;
            Err(e)
        }
    }
}

async fn run_queries(
    npg: usize,
    batch_rx: Receiver<Vec<ChannelInfoQuery>>,
    pgc_rx: Receiver<PgRes>,
    pgc_tx: Sender<PgRes>,
) -> Result<(), Error> {
    let mut stream = batch_rx
        .map(|batch| {
            let pgc_rx = pgc_rx.clone();
            let pgc_tx = pgc_tx.clone();
            async move {
                if let Ok(pgres) = pgc_rx.recv().await {
                    let (res, pgres) = fetch_data(batch, pgres).await?;
                    if let Err(_) = pgc_tx.send(pgres).await {
                        error!("can not hand back pgres");
                        Err(Error::with_msg_no_trace("can not hand back pgres"))
                    } else {
                        Ok(res)
                    }
                } else {
                    error!("can not get pgc");
                    Err(Error::with_msg_no_trace("no more pgres"))
                }
            }
        })
        .buffer_unordered(npg);
    while let Some(item) = stream.next().await {
        match item {
            Ok(res) => {
                for (sid, tx) in res.series.into_iter().zip(res.tx) {
                    match tx.send(Ok(sid)).await {
                        Ok(_) => {}
                        Err(_) => {
                            // TODO count cases, but no log. Client may no longer be interested in this result.
                        }
                    }
                }
            }
            Err(e) => {
                error!("{e}");
                tokio::time::sleep(Duration::from_millis(1000)).await;
            }
        }
    }
    Ok(())
}

pub async fn start_task(db: &Database) -> Result<Sender<ChannelInfoQuery>, Error> {
    let sql = concat!(
        "with q1 as (select * from unnest($1::text[], $2::text[], $3::int[], $4::text[], $5::int[])",
        " as inp (backend, channel, scalar_type, shape_dims, rid))",
        " select t.series, q1.rid from series_by_channel t",
        " join q1 on t.facility = q1.backend and t.channel = q1.channel",
        " and t.scalar_type = q1.scalar_type and t.shape_dims = q1.shape_dims::int[]",
        " and t.agg_kind = 0",
        " order by q1.rid",
    );
    let inp_cap = 128;
    let batch_out_cap = 4;
    let pgcn = 4;
    let timeout = Duration::from_millis(200);
    let (pgc_tx, pgc_rx) = prepare_pgcs(sql, pgcn, db).await?;
    let (query_tx, query_rx) = async_channel::bounded(inp_cap);
    let (batch_rx, _batch_jh) = batcher::batch(inp_cap, timeout, batch_out_cap, query_rx);
    let _queries_jh: JoinHandle<_> = tokio::task::spawn(run_queries(pgcn, batch_rx, pgc_rx, pgc_tx));
    Ok(query_tx)
}
