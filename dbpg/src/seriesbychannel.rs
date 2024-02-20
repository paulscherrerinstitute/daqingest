use async_channel::Receiver;
use async_channel::Sender;
use chrono::DateTime;
use chrono::Utc;
use core::fmt;
use err::thiserror;
use err::ThisError;
use futures_util::Future;
use futures_util::TryFutureExt;
use log::*;
use md5::Digest;
use netpod::Database;
use netpod::ScalarType;
use netpod::SeriesKind;
use netpod::Shape;
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
    #[error("DbConsistencySeries({0})")]
    DbConsistencySeries(String),
    ScalarType,
    Shape,
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

impl CanSendChannelInfoResult for async_channel::Sender<Result<ChannelInfoResult, Error>> {
    fn make_send(&self, item: Result<ChannelInfoResult, Error>) -> BoxedSend {
        let tx = self.clone();
        let fut = async move { tx.send(item).map_err(|_| ()).await };
        Box::pin(fut)
    }
}

pub struct ChannelInfoQuery {
    pub backend: String,
    pub channel: String,
    pub kind: SeriesKind,
    pub scalar_type: ScalarType,
    pub shape: Shape,
    pub tx: Pin<Box<dyn CanSendChannelInfoResult + Send>>,
}

impl fmt::Debug for ChannelInfoQuery {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("ChannelInfoQuery")
            .field("backend", &self.backend)
            .field("channel", &self.channel)
            .field("kind", &self.kind)
            .field("scalar_type", &self.scalar_type)
            .field("shape", &self.shape)
            .finish()
    }
}

#[derive(Debug)]
pub struct ChannelInfoResult {
    pub backend: String,
    pub channel: String,
    pub series: RegisteredSeries,
}

struct FoundResult {
    job: ChannelInfoQuery,
    status: MatchingSeries,
}

#[derive(Debug)]
enum MatchingSeries {
    DoesntExist,
    UsedBefore(SeriesId),
    Latest(SeriesId),
}

#[derive(Debug, Clone, PartialEq)]
pub enum RegisteredSeries {
    Created(SeriesId),
    Updated(SeriesId),
    Current(SeriesId),
}

impl RegisteredSeries {
    pub fn to_series(&self) -> SeriesId {
        match self {
            RegisteredSeries::Created(sid) => sid.clone(),
            RegisteredSeries::Updated(sid) => sid.clone(),
            RegisteredSeries::Current(sid) => sid.clone(),
        }
    }
}

struct Worker {
    pg: PgClient,
    qu_select: PgStatement,
    qu_insert: PgStatement,
    batch_rx: Receiver<Vec<ChannelInfoQuery>>,
    stats: Arc<SeriesByChannelStats>,
    pg_client_jh: JoinHandle<Result<(), crate::err::Error>>,
}

impl Worker {
    async fn new(
        db: &Database,
        batch_rx: Receiver<Vec<ChannelInfoQuery>>,
        stats: Arc<SeriesByChannelStats>,
    ) -> Result<Self, Error> {
        use tokio_postgres::types::Type;
        let (pg, pg_client_jh) = crate::conn::make_pg_client(db).await?;
        let sql = concat!(
            "with q1 as (",
            " select * from unnest($1, $2, $3, $4)",
            " as inp (rid, backend, channel, kind)",
            ")",
            " select q1.rid, t.series, t.scalar_type, t.shape_dims, t.tscs, t.kind from q1",
            " join series_by_channel t on t.facility = q1.backend and t.channel = q1.channel",
            " and t.kind = q1.kind and t.agg_kind = 0",
            " order by q1.rid",
        );
        let qu_select = pg
            .prepare_typed(
                sql,
                &[Type::INT4_ARRAY, Type::TEXT_ARRAY, Type::TEXT_ARRAY, Type::INT2_ARRAY],
            )
            .await?;

        let sql = concat!(
            "with q1 as (",
            " select * from unnest($1, $2, $3, $4, $5, $6)",
            " as inp (backend, channel, scalar_type, shape_dims, series, kind)",
            ")",
            " insert into series_by_channel (series, facility, channel, kind, scalar_type, shape_dims, agg_kind)",
            " select series, backend, channel, kind, scalar_type,",
            " array(select e::int from jsonb_array_elements(shape_dims) as e) as shape_dims,",
            " 0 from q1",
            " on conflict do nothing"
        );
        let qu_insert = pg
            .prepare_typed(
                sql,
                &[
                    Type::TEXT_ARRAY,
                    Type::TEXT_ARRAY,
                    Type::INT4_ARRAY,
                    Type::JSONB_ARRAY,
                    Type::INT8_ARRAY,
                    Type::INT2_ARRAY,
                ],
            )
            .await?;
        let ret = Self {
            pg,
            qu_select,
            qu_insert,
            batch_rx,
            stats,
            pg_client_jh,
        };
        Ok(ret)
    }

    async fn work<FR: HashSalter>(&mut self) -> Result<(), Error> {
        let batch_rx = self.batch_rx.clone();
        while let Ok(batch) = batch_rx.recv().await {
            self.stats.recv_batch().inc();
            self.stats.recv_items().add(batch.len() as _);
            for x in &batch {
                trace3!(
                    "search for {}  {}  {:?}  {:?}",
                    x.backend,
                    x.channel,
                    x.scalar_type,
                    x.shape
                );
            }
            self.pg.execute("begin", &[]).await?;
            match self.handle_batch::<FR>(batch).await {
                Ok(()) => {
                    let ts1 = Instant::now();
                    match self.pg.execute("commit", &[]).await {
                        Ok(n) => {
                            let dt = ts1.elapsed();
                            if dt > Duration::from_millis(40) {
                                debug!("commit {} {:.0} ms", n, dt.as_secs_f32());
                            }
                        }
                        Err(e) => {
                            warn!("commit error {e}");
                            self.pg.execute("rollback", &[]).await?;
                            tokio::time::sleep(Duration::from_millis(1000)).await;
                        }
                    };
                }
                Err(e) => {
                    error!("transaction error {e}");
                    self.pg.execute("rollback", &[]).await?;
                }
            };
        }
        trace!("Worker2 done");
        Ok(())
    }

    async fn handle_batch<FR: HashSalter>(&mut self, batch: Vec<ChannelInfoQuery>) -> Result<(), Error> {
        let batch_len = batch.len();
        let res1 = self.select(batch).await?;
        let mut latest = Vec::new();
        let mut used_before = Vec::new();
        let mut doesnt_exist = Vec::new();
        for x in res1 {
            match x.status {
                MatchingSeries::DoesntExist => doesnt_exist.push(x),
                MatchingSeries::UsedBefore(sid) => used_before.push((x.job, RegisteredSeries::Updated(sid))),
                MatchingSeries::Latest(sid) => latest.push((x.job, RegisteredSeries::Current(sid))),
            }
        }
        if doesnt_exist.len() != 0 {
            let batch2 = doesnt_exist.into_iter().map(|x| x.job).collect();
            self.insert_missing::<FR>(&batch2).await?;
            let res2 = self.select(batch2).await?;
            for x in res2 {
                match x.status {
                    MatchingSeries::DoesntExist => {
                        error!("series ids still missing after insert  {:?}", x.job);
                        return Err(Error::SeriesMissing);
                    }
                    MatchingSeries::UsedBefore(sid) => {
                        // Could theoretically happen when we ask in the same batch
                        // for the same channel name but of different type/shape.
                        warn!("series id used before after insert  {:?}", x.job);
                        // return Err(Error::SeriesMissing);
                        latest.push((x.job, RegisteredSeries::Created(sid)));
                    }
                    MatchingSeries::Latest(sid) => {
                        latest.push((x.job, RegisteredSeries::Created(sid)));
                    }
                }
            }
        };
        if used_before.len() != 0 {
            // TODO update the used before, then select again
            let sid: Vec<_> = used_before.iter().map(|x| x.1.to_series().id() as i64).collect();
            self.update_used_before::<FR>(sid).await?;
            for e in used_before {
                latest.push(e);
            }
        };
        if latest.len() != batch_len {
            error!("can not register all series");
            return Err(Error::SeriesMissing);
        }
        for e in latest {
            let item = ChannelInfoResult {
                backend: e.0.backend,
                channel: e.0.channel,
                series: e.1.clone(),
            };
            let item = Ok(item);
            match e.0.tx.make_send(item).await {
                Ok(()) => {}
                Err(_) => {
                    self.stats.res_tx_fail.inc();
                }
            };
        }
        Ok(())
    }

    async fn select(&self, batch: Vec<ChannelInfoQuery>) -> Result<Vec<FoundResult>, Error> {
        let (rids, backends, channels, kinds, jobs) = batch
            .into_iter()
            .enumerate()
            .map(|(i, e)| {
                let rid = i as i32;
                let backend = e.backend.clone();
                let channel = e.channel.clone();
                let kind = e.kind.to_db_i16();
                let job = e;
                (rid, backend, channel, kind, job)
            })
            .fold(
                (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new()),
                |mut a, v| {
                    a.0.push(v.0);
                    a.1.push(v.1);
                    a.2.push(v.2);
                    a.3.push(v.3);
                    a.4.push(v.4);
                    a
                },
            );
        // debug!("select worker start batch of {}  {:?}", channels.len(), channels);
        let rows = self
            .pg
            .query(&self.qu_select, &[&rids, &backends, &channels, &kinds])
            .await?;
        let mut row_it = rows.into_iter();
        let mut row_opt = row_it.next();
        let mut acc = Vec::new();
        let mut result = Vec::new();
        for (&rid, job) in rids.iter().zip(jobs.into_iter()) {
            loop {
                break if let Some(row) = &row_opt {
                    if row.get::<_, i32>(0) == rid {
                        let series = SeriesId::new(row.get::<_, i64>(1) as _);
                        let scalar_type = ScalarType::from_scylla_i32(row.get(2)).map_err(|_| Error::ScalarType)?;
                        let shape_dims = Shape::from_scylla_shape_dims(row.get::<_, Vec<i32>>(3).as_slice())
                            .map_err(|_| Error::Shape)?;
                        let tscs: Vec<DateTime<Utc>> = row.get(4);
                        let kind: i16 = row.get(5);
                        let kind = SeriesKind::from_db_i16(kind).map_err(|_| Error::ScalarType)?;
                        if job.channel == "TEST:MEDIUM:WAVE-01024:F32:000000"
                            || series == SeriesId::new(1605348259462543621)
                        {
                            debug!(
                                "select worker found in database  {:?}  {:?}  {:?}  {:?}  {:?}  {:?}",
                                rid, series, scalar_type, shape_dims, tscs, kind
                            );
                        }
                        acc.push((rid, series, scalar_type, shape_dims, tscs));
                        row_opt = row_it.next();
                        continue;
                    }
                };
            }
            // debug!("check for {job:?}");
            // TODO  call decide with empty accumulator: will result in DoesntExist.
            let v = std::mem::replace(&mut acc, Vec::new());
            let dec = Self::decide_matching_via_db(job.scalar_type.clone(), job.shape.clone(), v)?;
            // debug!("decision  {dec:?}");
            result.push(FoundResult { job, status: dec });
        }
        Ok(result)
    }

    fn decide_matching_via_db(
        scalar_type: ScalarType,
        shape: Shape,
        acc: Vec<(i32, SeriesId, ScalarType, Shape, Vec<DateTime<Utc>>)>,
    ) -> Result<MatchingSeries, Error> {
        let a2 = acc.iter().map(|x| &x.4).collect();
        Self::assert_order(a2)?;
        let unfolded = Self::unfold_series_rows(acc)?;
        Self::assert_varying_types(&unfolded)?;
        if let Some(last) = unfolded.last() {
            if last.1 == scalar_type && last.2 == shape {
                Ok(MatchingSeries::Latest(last.0.clone()))
            } else {
                for e in unfolded.into_iter().rev() {
                    if e.1 == scalar_type && e.2 == shape {
                        return Ok(MatchingSeries::UsedBefore(e.0.clone()));
                    }
                }
                Ok(MatchingSeries::DoesntExist)
            }
        } else {
            Ok(MatchingSeries::DoesntExist)
        }
    }

    fn unfold_series_rows(
        acc: Vec<(i32, SeriesId, ScalarType, Shape, Vec<DateTime<Utc>>)>,
    ) -> Result<Vec<(SeriesId, ScalarType, Shape, DateTime<Utc>)>, Error> {
        let mut ret = Vec::new();
        for g in acc.iter() {
            for h in g.4.iter() {
                ret.push((g.1.clone(), g.2.clone(), g.3.clone(), *h));
            }
        }
        ret.sort_by(|a, b| a.cmp(b));
        Ok(ret)
    }

    fn assert_order(v: Vec<&Vec<DateTime<Utc>>>) -> Result<(), Error> {
        for x in &v {
            if x.len() > 1 {
                let mut z = x[0];
                for &y in x[1..].iter() {
                    if y <= z {
                        let e = Error::DbConsistencySeries(format!("tscs not in order"));
                        return Err(e);
                    } else {
                        z = y;
                    }
                }
            }
        }
        Ok(())
    }

    fn assert_varying_types(v: &Vec<(SeriesId, ScalarType, Shape, DateTime<Utc>)>) -> Result<(), Error> {
        if v.len() > 1 {
            let mut z_0 = &v[0].0;
            let mut z_1 = &v[0].1;
            let mut z_2 = &v[0].2;
            for r in v[1..].iter() {
                if r.1 == *z_1 && r.2 == *z_2 {
                    let e = Error::DbConsistencySeries(format!("no change between entries  {:?}  {:?}", r.0, z_0));
                    return Err(e);
                }
                z_0 = &r.0;
                z_1 = &r.1;
                z_2 = &r.2;
            }
            Ok(())
        } else {
            Ok(())
        }
    }

    async fn insert_missing<FR: HashSalter>(&self, batch: &Vec<ChannelInfoQuery>) -> Result<(), Error> {
        // debug!("insert_missing  len {}", batch.len());
        let (backends, channels, kinds, scalar_types, shapes, mut hashers) = batch
            .iter()
            .map(|job| {
                let backend = &job.backend;
                let channel = &job.channel;
                let kind = job.kind.to_db_i16();
                let scalar_type = &job.scalar_type;
                let shape = &job.shape;
                let hasher = {
                    let mut h = md5::Md5::new();
                    h.update(job.backend.as_bytes());
                    h.update(job.channel.as_bytes());
                    h.update(format!("{:?}", scalar_type).as_bytes());
                    h.update(format!("{:?}", shape).as_bytes());
                    h
                };
                let x = (backend, channel, kind, scalar_type.to_scylla_i32(), shape, hasher);
                if channel == "TEST:MEDIUM:WAVE-01024:F32:000000" {
                    debug!("INSERT {x:?}");
                }
                x
            })
            .fold(
                (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new()),
                |mut a, x| {
                    a.0.push(x.0);
                    a.1.push(x.1);
                    a.2.push(x.2);
                    a.3.push(x.3);
                    a.4.push(x.4);
                    a.5.push(x.5);
                    a
                },
            );
        let mut total_insert_count = 0;
        let mut i1: u16 = 0;
        loop {
            i1 += 1;
            if i1 >= 200 {
                warn!("create series id outer abort");
                return Err(Error::CreateSeriesFail);
            }
            let mut series_ids = Vec::with_capacity(hashers.len());
            for h in &mut hashers {
                let mut i2: u16 = 0;
                loop {
                    break {
                        i2 += 1;
                        if i2 >= 800 {
                            warn!("create series id inner abort");
                            return Err(Error::CreateSeriesFail);
                        }
                        FR::hupd(&mut |buf| h.update(buf), i1, i2);
                        let f = h.clone().finalize();
                        let series = u64::from_le_bytes(f.as_slice()[0..8].try_into().unwrap());
                        if series >= 1000000000000000000 && series <= i64::MAX as u64 {
                            series_ids.push(series as i64);
                        } else {
                            continue;
                        }
                    };
                }
            }
            let shape_dims_jss: Vec<_> = shapes.iter().map(|x| x.to_json_value()).collect();
            let cc = self
                .pg
                .execute(
                    &self.qu_insert,
                    &[
                        &backends,
                        &channels,
                        &scalar_types,
                        &shape_dims_jss,
                        &series_ids,
                        &kinds,
                    ],
                )
                .await
                .unwrap();
            // debug!("cc {}  channels {}", cc, channels.len());
            total_insert_count += cc;
            if total_insert_count == channels.len() as u64 {
                break;
            }
        }
        Ok(())
    }

    async fn update_used_before<FR: HashSalter>(&self, sid: Vec<i64>) -> Result<(), Error> {
        debug!("update_used_before {sid:?}");
        if sid.contains(&1605348259462543621) {
            debug!("UPDATE TSC FOR 1605348259462543621");
        }
        let sql = concat!(
            "update series_by_channel set",
            " tscs = tscs || now()::timestamptz",
            " where series = any ($1)",
        );
        let qu = self
            .pg
            .prepare_typed(sql, &[tokio_postgres::types::Type::INT8_ARRAY])
            .await?;
        let n = self.pg.execute(&qu, &[&sid]).await?;
        trace!("update_used_before  n {n}");
        Ok(())
    }
}

pub trait HashSalter {
    fn hupd(hupd: &mut dyn FnMut(&[u8]), i1: u16, i2: u16);
}

pub async fn start_lookup_workers<FR: HashSalter>(
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
    let inp_cap = 64;
    let batch_out_cap = 4;
    let timeout = Duration::from_millis(100);
    let (query_tx, query_rx) = async_channel::bounded(inp_cap);
    let (batch_rx, bjh) = batchtools::batcher::batch(inp_cap, timeout, batch_out_cap, query_rx);
    let mut jhs = Vec::new();
    for _ in 0..worker_count {
        let mut worker = Worker::new(db, batch_rx.clone(), stats.clone()).await?;
        let jh = tokio::task::spawn(async move { worker.work::<FR>().await });
        jhs.push(jh);
    }
    Ok((query_tx, jhs, bjh))
}

struct SalterTest;

impl HashSalter for SalterTest {
    fn hupd(hupd: &mut dyn FnMut(&[u8]), i1: u16, i2: u16) {
        hupd(&i1.to_le_bytes());
        hupd(&i2.to_le_bytes());
    }
}

pub struct SalterRandom;

impl HashSalter for SalterRandom {
    fn hupd(hupd: &mut dyn FnMut(&[u8]), i1: u16, i2: u16) {
        let tsnow = Instant::now();
        let b = unsafe { &*(&tsnow as *const Instant as *const [u8; core::mem::size_of::<Instant>()]) };
        hupd(b)
    }
}

async fn psql_play(db: &Database) -> Result<(), Error> {
    use tokio_postgres::types::ToSql;
    use tokio_postgres::types::Type;
    let (pg, pg_client_jh) = crate::conn::make_pg_client(db).await?;
    if false {
        let sql = concat!("select pg_typeof($1)");
        let qu = pg.prepare_typed(sql, &[Type::INT4_ARRAY]).await?;
        // let qu = pg.prepare(sql).await?;
        let p1 = 4f64;
        let rows = pg.query(&qu, &[&p1]).await;
        debug!("{rows:?}");
        let p1 = &[12i32, 13, 14][..];
        let rows = pg.query(&qu, &[&p1]).await;
        debug!("{rows:?}");
        let p1 = vec![12i32, 13, 14];
        let rows = pg.query(&qu, &[&p1]).await;
        debug!("{rows:?}");
        let p1 = vec![12i64, 13, 14];
        let rows = pg.query(&qu, &[&p1]).await;
        debug!("{rows:?}");
        let p1 = vec![String::from("a"), String::from("b")];
        let rows = pg.query(&qu, &[&p1]).await;
        debug!("{rows:?}");
        let p1 = vec![vec![4i64, 8], vec![10, 12]];
        let rows = pg.query(&qu, &[&p1]).await;
        debug!("{rows:?}");
    }
    if false {
        let sql = concat!(
            "with q1 as (",
            "select * from unnest($1) as inp (col1)",
            ")",
            " select * from q1",
        );
        let qu = pg.prepare_typed(sql, &[Type::INT4_ARRAY]).await?;
        let p1 = vec![1_i32, 2, 3, 4];
        let rows = pg.query(&qu, &[&p1]).await?;
        for row in rows {
            debug!("{:?}  {:?}", row, row.columns());
            let ca: i32 = row.get(0);
            let cb: Vec<i32> = row.get(0);
            debug!("{:?}  {:?}", ca, cb);
        }
    }
    if true {
        let sql = concat!(
            "with q1 as (",
            "select col1 from unnest($1) as inp (col1)",
            ")",
            " select col1::int4[] from q1",
        );
        let qu = pg.prepare_typed(sql, &[Type::JSONB_ARRAY]).await?;
        // JsVal
        // let p1 = vec![JsVal::Array(vec![3_i32, 4, 5, 6])];
        let p1 = vec![
            serde_json::to_value(&vec![1_i32, 2, 3, 4]).unwrap(),
            serde_json::to_value(&vec![5_i32, 6, 7, 8]).unwrap(),
        ];
        // let p1 = serde_json::to_value(vec![vec![1_i32, 2, 3, 4], vec![5_i32, 6, 7, 8]]).unwrap();
        let rows = pg.query(&qu, &[&p1]).await?;
        for row in rows {
            debug!("{:?}  {:?}", row, row.columns());
            let ca = row.try_get::<_, i32>(0);
            let cb = row.try_get::<_, Vec<i32>>(0);
            debug!("{:?}  {:?}", ca, cb);
        }
    }
    Ok(())
}

#[test]
fn test_series_by_channel_01() {
    let fut = async {
        use crate as dbpg;
        let backend = "bck-test-00";
        let channel = "chn-test-00";
        let channel_01 = "chn-test-01";
        let channel_02 = "chn-test-02";
        let series_by_channel_stats = Arc::new(SeriesByChannelStats::new());
        let pgconf = test_db_conf();
        if false {
            psql_play(&pgconf).await?;
            return Ok(());
        }
        let (pg, _pg_client_jh) = crate::conn::make_pg_client(&pgconf).await?;
        crate::schema::schema_check(&pg).await.unwrap();
        {
            pg.execute("delete from series_by_channel where facility = $1", &[&backend])
                .await?;

            // Block the id
            let id1: i64 = 5609172854884670524;
            let sql = concat!(
                "insert into series_by_channel (series, facility, channel, kind, scalar_type, shape_dims, agg_kind)",
                " values ($1, $2, $3, 2, $4, array[]::int4[], 0)"
            );
            pg.execute(sql, &[&id1, &backend, &"test-block-00", &1i32]).await?;

            if true {
                // Create a used-before case
                let id: i64 = 4802468414253815536;
                let sql = concat!(
                    "insert into series_by_channel",
                    " (series, facility, channel, kind, scalar_type, shape_dims, agg_kind, tscs)",
                    " values ($1, $2, $3, 2, 5, array[64]::int4[], 0, array['2000-10-10T08:00:00Z'::timestamptz])"
                );
                pg.execute(sql, &[&id, &backend, &channel_02]).await?;
                let id: i64 = 6409375609862757444;
                let sql = concat!(
                    "insert into series_by_channel",
                    " (series, facility, channel, kind, scalar_type, shape_dims, agg_kind, tscs)",
                    " values ($1, $2, $3, 2, 8, array[64]::int4[], 0, array['2001-10-10T08:00:00Z'::timestamptz])"
                );
                pg.execute(sql, &[&id, &backend, &channel_02]).await?;
            }
        }
        // TODO keep join handles and await later
        let (channel_info_query_tx, jhs, jh) =
            dbpg::seriesbychannel::start_lookup_workers::<SalterTest>(1, &pgconf, series_by_channel_stats.clone())
                .await?;

        let mut rxs = Vec::new();
        let rx = {
            let (tx, rx) = async_channel::bounded(1);
            let item = dbpg::seriesbychannel::ChannelInfoQuery {
                backend: backend.into(),
                channel: channel.into(),
                kind: SeriesKind::ChannelData,
                scalar_type: netpod::ScalarType::U16,
                shape: Shape::Wave(64),
                tx: Box::pin(tx),
            };
            channel_info_query_tx.send(item).await.unwrap();
            rx
        };
        rxs.push(rx);

        let rx = {
            let (tx, rx) = async_channel::bounded(1);
            let item = dbpg::seriesbychannel::ChannelInfoQuery {
                backend: backend.into(),
                channel: channel_01.into(),
                kind: SeriesKind::ChannelData,
                scalar_type: netpod::ScalarType::U16,
                shape: Shape::Wave(64),
                tx: Box::pin(tx),
            };
            channel_info_query_tx.send(item).await.unwrap();
            rx
        };
        rxs.push(rx);

        let rx = {
            let (tx, rx) = async_channel::bounded(1);
            let item = dbpg::seriesbychannel::ChannelInfoQuery {
                backend: backend.into(),
                channel: channel_02.into(),
                kind: SeriesKind::ChannelData,
                scalar_type: netpod::ScalarType::U16,
                shape: Shape::Wave(64),
                tx: Box::pin(tx),
            };
            channel_info_query_tx.send(item).await.unwrap();
            rx
        };
        rxs.push(rx);

        let mut series_ids = Vec::new();
        for rx in rxs {
            let res = rx.recv().await.unwrap();
            debug!("received A: {res:?}");
            series_ids.push(res.unwrap().series);
        }
        {
            let exp = vec![
                RegisteredSeries::Created(SeriesId::new(5191776216635917420)),
                RegisteredSeries::Created(SeriesId::new(5025677968516078602)),
                RegisteredSeries::Updated(SeriesId::new(4802468414253815536)),
            ];
            assert_eq!(series_ids, exp);
        }

        let (tx, rx) = async_channel::bounded(1);
        let item = dbpg::seriesbychannel::ChannelInfoQuery {
            backend: backend.into(),
            channel: channel.into(),
            kind: SeriesKind::ChannelData,
            scalar_type: netpod::ScalarType::U16,
            shape: Shape::Wave(64),
            tx: Box::pin(tx),
        };
        channel_info_query_tx.send(item).await.unwrap();
        let res = rx.recv().await.unwrap();
        debug!("received C: {res:?}");

        {
            let rows = pg
                .query(
                    concat!(
                        "select series, channel, kind, scalar_type, shape_dims",
                        " from series_by_channel where facility = $1",
                        " and channel not like 'test-block%'",
                        " order by channel, scalar_type, shape_dims",
                    ),
                    &[&backend],
                )
                .await?;
            let mut all = Vec::new();
            for row in rows {
                let series: i64 = row.get(0);
                let channel: String = row.get(1);
                let scalar_type: i32 = row.get(3);
                let shape_dims: Vec<i32> = row.get(4);
                all.push((series, channel, scalar_type, shape_dims));
            }
            let exp: Vec<(i64, String, i32, Vec<i32>)> = vec![
                (5191776216635917420, "chn-test-00".into(), 5, vec![64]),
                (5025677968516078602, "chn-test-01".into(), 5, vec![64]),
                (4802468414253815536, "chn-test-02".into(), 5, vec![64]),
                (6409375609862757444, "chn-test-02".into(), 8, vec![64]),
            ];
            assert_eq!(all, exp);
            // TODO assert that the correct chn-test-02 got updated
        }
        Ok::<_, Error>(())
    };
    taskrun::run(fut).unwrap();
}

#[cfg(test)]
fn test_db_conf() -> Database {
    if false {
        Database {
            host: "127.0.0.1".into(),
            port: 5432,
            user: "daqbuffer".into(),
            pass: "daqbuffer".into(),
            name: "daqbuffer".into(),
        }
    } else {
        Database {
            host: "sf-scylladb-amd32-01.psi.ch".into(),
            port: 5173,
            user: "test_daqbuffer".into(),
            pass: "test_daqbuffer".into(),
            name: "test_daqbuffer".into(),
        }
    }
}

#[cfg(test)]
async fn test_db_conn() -> Result<PgClient, Error> {
    let db = test_db_conf();
    let (pg, pg_client_jh) = crate::conn::make_pg_client(&db).await?;
    Ok(pg)
}
