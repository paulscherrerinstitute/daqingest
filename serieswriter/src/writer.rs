use crate::timebin::ConnTimeBin;
use async_channel::Receiver;
use async_channel::Sender;
use dbpg::seriesbychannel::ChannelInfoQuery;
use err::thiserror;
use err::ThisError;
use futures_util::future;
use futures_util::StreamExt;
use log::*;
use netpod::timeunits::HOUR;
use netpod::timeunits::SEC;
use netpod::ScalarType;
use netpod::SeriesKind;
use netpod::Shape;
use netpod::TsNano;
use scywr::iteminsertqueue::DataValue;
use scywr::iteminsertqueue::InsertItem;
use scywr::iteminsertqueue::QueryItem;
use series::ChannelStatusSeriesId;
use series::SeriesId;
use stats::SeriesWriterEstablishStats;
use std::collections::VecDeque;
use std::sync::atomic;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

#[derive(Debug, ThisError)]
pub enum Error {
    DbPgSid(#[from] dbpg::seriesid::Error),
    ChannelSendError,
    ChannelRecvError,
    SeriesLookupError,
    Db(#[from] dbpg::err::Error),
    DbSchema(#[from] dbpg::schema::Error),
    Scy(#[from] scywr::session::Error),
    ScySchema(#[from] scywr::schema::Error),
    Series(#[from] dbpg::seriesbychannel::Error),
    Timebin(#[from] crate::timebin::Error),
}

impl<T> From<async_channel::SendError<T>> for Error {
    fn from(value: async_channel::SendError<T>) -> Self {
        Error::ChannelSendError
    }
}
impl From<async_channel::RecvError> for Error {
    fn from(value: async_channel::RecvError) -> Self {
        Error::ChannelRecvError
    }
}

#[derive(Debug)]
pub struct SeriesWriter {
    cssid: ChannelStatusSeriesId,
    sid: SeriesId,
    scalar_type: ScalarType,
    shape: Shape,
    ts_msp_last: Option<TsNano>,
    inserted_in_current_msp: u32,
    bytes_in_current_msp: u32,
    msp_max_entries: u32,
    msp_max_bytes: u32,
    // TODO this should be in an Option:
    ts_msp_grid_last: u32,
    binner: ConnTimeBin,
    written_last: Option<DataValue>,
}

impl SeriesWriter {
    pub async fn establish(
        worker_tx: Sender<ChannelInfoQuery>,
        backend: String,
        channel: String,
        scalar_type: ScalarType,
        shape: Shape,
        stnow: SystemTime,
    ) -> Result<Self, Error> {
        let (tx, rx) = async_channel::bounded(1);
        let item = ChannelInfoQuery {
            backend: backend.clone(),
            channel: channel.clone(),
            kind: SeriesKind::ChannelStatus,
            scalar_type: ScalarType::ChannelStatus,
            shape: Shape::Scalar,
            tx: Box::pin(tx),
        };
        worker_tx.send(item).await?;
        let res = rx.recv().await?.map_err(|_| Error::SeriesLookupError)?;
        let cssid = ChannelStatusSeriesId::new(res.series.to_series().id());
        Self::establish_with_cssid(worker_tx, cssid, backend, channel, scalar_type, shape, stnow).await
    }

    pub async fn establish_with_cssid(
        worker_tx: Sender<ChannelInfoQuery>,
        cssid: ChannelStatusSeriesId,
        backend: String,
        channel: String,
        scalar_type: ScalarType,
        shape: Shape,
        tsnow: SystemTime,
    ) -> Result<Self, Error> {
        let (tx, rx) = async_channel::bounded(1);
        let item = ChannelInfoQuery {
            backend,
            channel,
            kind: SeriesKind::ChannelData,
            scalar_type: scalar_type.clone(),
            shape: shape.clone(),
            tx: Box::pin(tx),
        };
        worker_tx.send(item).await?;
        let res = rx.recv().await?.map_err(|_| Error::SeriesLookupError)?;
        let sid = res.series.to_series();
        let mut binner = ConnTimeBin::empty(sid.clone(), TsNano::from_ns(SEC * 10));
        binner.setup_for(&scalar_type, &shape, tsnow)?;
        let res = Self {
            cssid,
            sid,
            scalar_type,
            shape,
            ts_msp_last: None,
            inserted_in_current_msp: 0,
            bytes_in_current_msp: 0,
            msp_max_entries: 64000,
            msp_max_bytes: 1024 * 1024 * 20,
            ts_msp_grid_last: 0,
            binner,
            written_last: None,
        };
        Ok(res)
    }

    pub fn sid(&self) -> SeriesId {
        self.sid.clone()
    }

    pub fn scalar_type(&self) -> &ScalarType {
        &self.scalar_type
    }

    pub fn shape(&self) -> &Shape {
        &self.shape
    }

    pub fn write(
        &mut self,
        ts: TsNano,
        ts_local: TsNano,
        val: DataValue,
        item_qu: &mut VecDeque<QueryItem>,
    ) -> Result<(), Error> {
        // TODO compute the binned data here as well and flush completed bins if needed.
        self.binner.push(ts.clone(), &val)?;

        // TODO decide on better msp/lsp: random offset!
        // As long as one writer is active, the msp is arbitrary.

        // Maximum resolution of the ts msp:
        let msp_res_max = SEC * 2;

        let (ts_msp, ts_msp_changed) = match self.ts_msp_last {
            Some(ts_msp_last) => {
                if self.inserted_in_current_msp >= self.msp_max_entries
                    || self.bytes_in_current_msp >= self.msp_max_bytes
                    || ts_msp_last.add_ns(HOUR) <= ts
                {
                    let ts_msp = ts.div(msp_res_max).mul(msp_res_max);
                    if ts_msp == ts_msp_last {
                        (ts_msp, false)
                    } else {
                        self.ts_msp_last = Some(ts_msp);
                        self.inserted_in_current_msp = 1;
                        self.bytes_in_current_msp = val.byte_size();
                        (ts_msp, true)
                    }
                } else {
                    self.inserted_in_current_msp += 1;
                    self.bytes_in_current_msp += val.byte_size();
                    (ts_msp_last, false)
                }
            }
            None => {
                let ts_msp = ts.div(msp_res_max).mul(msp_res_max);
                self.ts_msp_last = Some(ts_msp);
                self.inserted_in_current_msp = 1;
                self.bytes_in_current_msp = val.byte_size();
                (ts_msp, true)
            }
        };
        let ts_lsp = ts.delta(ts_msp);
        let item = InsertItem {
            series: self.sid.clone(),
            ts_msp: ts_msp.to_ts_ms(),
            ts_lsp: ts_lsp,
            msp_bump: ts_msp_changed,
            pulse: 0,
            scalar_type: self.scalar_type.clone(),
            shape: self.shape.clone(),
            val,
            ts_local: ts_local.to_ts_ms(),
        };
        item_qu.push_back(QueryItem::Insert(item));
        Ok(())
    }

    pub fn tick(&mut self, iiq: &mut VecDeque<QueryItem>) -> Result<(), Error> {
        self.binner.tick(iiq)?;
        Ok(())
    }
}

pub struct JobId(pub u64);

pub struct EstablishWriterWorker {
    worker_tx: Sender<ChannelInfoQuery>,
    jobrx: Receiver<EstablishWorkerJob>,
    stats: Arc<SeriesWriterEstablishStats>,
}

impl EstablishWriterWorker {
    fn new(
        worker_tx: Sender<ChannelInfoQuery>,
        jobrx: Receiver<EstablishWorkerJob>,
        stats: Arc<SeriesWriterEstablishStats>,
    ) -> Self {
        Self {
            worker_tx,
            jobrx,
            stats,
        }
    }

    async fn work(self) {
        let cnt = Arc::new(AtomicU64::new(0));
        taskrun::spawn({
            let cnt = cnt.clone();
            async move {
                if true {
                    return Ok::<_, Error>(());
                }
                loop {
                    taskrun::tokio::time::sleep(Duration::from_millis(10000)).await;
                    debug!("EstablishWriterWorker  cnt {}", cnt.load(atomic::Ordering::SeqCst));
                }
                Ok::<_, Error>(())
            }
        });
        self.jobrx
            .map(move |item| {
                let wtx = self.worker_tx.clone();
                let cnt = cnt.clone();
                let stats = self.stats.clone();
                async move {
                    let res = SeriesWriter::establish(
                        wtx.clone(),
                        item.backend,
                        item.channel,
                        item.scalar_type,
                        item.shape,
                        item.tsnow,
                    )
                    .await;
                    cnt.fetch_add(1, atomic::Ordering::SeqCst);
                    if item.restx.send((item.job_id, res)).await.is_err() {
                        stats.result_send_fail().inc();
                        trace!("can not send writer establish result");
                    }
                }
            })
            .buffer_unordered(512)
            .for_each(|_| future::ready(()))
            .await;
    }
}

pub struct EstablishWorkerJob {
    job_id: JobId,
    backend: String,
    channel: String,
    scalar_type: ScalarType,
    shape: Shape,
    restx: Sender<(JobId, Result<SeriesWriter, Error>)>,
    tsnow: SystemTime,
}

impl EstablishWorkerJob {
    pub fn new(
        job_id: JobId,
        backend: String,
        channel: String,
        scalar_type: ScalarType,
        shape: Shape,
        restx: Sender<(JobId, Result<SeriesWriter, Error>)>,
        tsnow: SystemTime,
    ) -> Self {
        Self {
            job_id,
            backend,
            channel,
            scalar_type,
            shape,
            restx,
            tsnow,
        }
    }
}

pub fn start_writer_establish_worker(
    worker_tx: Sender<ChannelInfoQuery>,
    stats: Arc<SeriesWriterEstablishStats>,
) -> Result<(Sender<EstablishWorkerJob>,), Error> {
    let (tx, rx) = async_channel::bounded(256);
    let worker = EstablishWriterWorker::new(worker_tx, rx, stats);
    taskrun::spawn(worker.work());
    Ok((tx,))
}

#[test]
fn write_00() {
    use netpod::Database;
    use scywr::config::ScyllaIngestConfig;
    use stats::SeriesByChannelStats;
    use std::sync::Arc;
    let fut = async {
        let dbconf = &Database {
            name: "daqbuffer".into(),
            host: "localhost".into(),
            port: 5432,
            user: "daqbuffer".into(),
            pass: "daqbuffer".into(),
        };
        let scyconf = &ScyllaIngestConfig::new(["127.0.0.1:19042"], "daqingest_test_00_rf3", "daqingest_test_00_rf1");
        let (pgc, pg_jh) = dbpg::conn::make_pg_client(dbconf).await?;
        dbpg::schema::schema_check(&pgc).await?;
        scywr::schema::migrate_scylla_data_schema(scyconf, netpod::ttl::RetentionTime::Short).await?;
        let scy = scywr::session::create_session(scyconf).await?;
        let stats = SeriesByChannelStats::new();
        let stats = Arc::new(stats);
        let (tx, jhs, jh) =
            dbpg::seriesbychannel::start_lookup_workers::<dbpg::seriesbychannel::SalterRandom>(1, dbconf, stats)
                .await?;
        let backend = "bck-test-00";
        let channel = "chn-test-00";
        let scalar_type = ScalarType::I16;
        let shape = Shape::Scalar;
        let tsnow = SystemTime::now();
        let mut writer = SeriesWriter::establish(tx, backend.into(), channel.into(), scalar_type, shape, tsnow).await?;
        eprintln!("{writer:?}");
        let mut item_queue = VecDeque::new();
        let item_qu = &mut item_queue;
        for i in 0..10 {
            let ts = TsNano::from_ns(HOUR * 24 + SEC * i);
            let ts_local = ts.clone();
            let val = DataValue::Scalar(scywr::iteminsertqueue::ScalarValue::I16(i as _));
            writer.write(ts, ts_local, val, item_qu)?;
        }
        eprintln!("{item_queue:?}");
        Ok::<_, Error>(())
    };
    taskrun::run(fut).unwrap();
}
