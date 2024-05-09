use crate::rtwriter::MinQuiets;
use crate::rtwriter::RtWriter;
use crate::writer::SeriesWriter;
use async_channel::Receiver;
use async_channel::Sender;
use dbpg::seriesbychannel::ChannelInfoQuery;
use err::thiserror;
use err::ThisError;
use futures_util::future;
use futures_util::StreamExt;
use netpod::log::*;
use netpod::timeunits::HOUR;
use netpod::timeunits::SEC;
use netpod::ScalarType;
use netpod::Shape;
use netpod::TsNano;
use scywr::insertqueues::InsertDeques;
use scywr::iteminsertqueue::DataValue;
use series::ChannelStatusSeriesId;
use series::SeriesId;
use stats::SeriesWriterEstablishStats;
use std::sync::atomic;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

#[derive(Debug, ThisError)]
pub enum Error {
    Postgres(#[from] dbpg::err::Error),
    PostgresSchema(#[from] dbpg::schema::Error),
    ScyllaSession(#[from] scywr::session::Error),
    ScyllaSchema(#[from] scywr::schema::Error),
    SeriesWriter(#[from] crate::writer::Error),
    SeriesByChannel(#[from] dbpg::seriesbychannel::Error),
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
                    let res = RtWriter::new(
                        wtx.clone(),
                        item.cssid,
                        item.backend,
                        item.channel,
                        item.scalar_type,
                        item.shape,
                        item.min_quiets,
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
    cssid: ChannelStatusSeriesId,
    scalar_type: ScalarType,
    shape: Shape,
    min_quiets: MinQuiets,
    restx: Sender<(JobId, Result<RtWriter, crate::rtwriter::Error>)>,
    tsnow: SystemTime,
}

impl EstablishWorkerJob {
    pub fn new(
        job_id: JobId,
        backend: String,
        channel: String,
        cssid: ChannelStatusSeriesId,
        scalar_type: ScalarType,
        shape: Shape,
        min_quiets: MinQuiets,
        restx: Sender<(JobId, Result<RtWriter, crate::rtwriter::Error>)>,
        tsnow: SystemTime,
    ) -> Self {
        Self {
            job_id,
            backend,
            channel,
            cssid,
            scalar_type,
            shape,
            min_quiets,
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
        let mut iqdqs = InsertDeques::new();
        for i in 0..10 {
            let ts = TsNano::from_ns(HOUR * 24 + SEC * i);
            let ts_local = ts.clone();
            let val = DataValue::Scalar(scywr::iteminsertqueue::ScalarValue::I16(i as _));
            writer.write(ts, ts_local, val, &mut iqdqs.st_rf3_rx)?;
        }
        Ok::<_, Error>(())
    };
    taskrun::run(fut).unwrap();
}
