use async_channel::Sender;
use dbpg::seriesbychannel::ChannelInfoQuery;
use err::thiserror;
use err::ThisError;
use log::*;
use netpod::timeunits::HOUR;
use netpod::timeunits::SEC;
use netpod::Database;
use netpod::ScalarType;
use netpod::ScyllaConfig;
use netpod::Shape;
use netpod::TsNano;
use netpod::TS_MSP_GRID_SPACING;
use netpod::TS_MSP_GRID_UNIT;
use scywr::iteminsertqueue::DataValue;
use scywr::iteminsertqueue::InsertItem;
use scywr::iteminsertqueue::QueryItem;
use series::series::CHANNEL_STATUS_DUMMY_SCALAR_TYPE;
use series::ChannelStatusSeriesId;
use series::SeriesId;
use stats::SeriesByChannelStats;
use std::collections::VecDeque;
use std::sync::Arc;

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
    ts_msp_last: TsNano,
    inserted_in_current_msp: u32,
    msp_max_entries: u32,
    ts_msp_grid_last: u32,
}

impl SeriesWriter {
    // TODO this requires a database
    pub async fn establish(
        worker_tx: Sender<ChannelInfoQuery>,
        backend: String,
        channel: String,
        scalar_type: ScalarType,
        shape: Shape,
    ) -> Result<Self, Error> {
        let (tx, rx) = async_channel::bounded(1);
        let item = ChannelInfoQuery {
            backend: backend.clone(),
            channel: channel.clone(),
            scalar_type: CHANNEL_STATUS_DUMMY_SCALAR_TYPE,
            shape_dims: shape.to_scylla_vec(),
            tx: Box::pin(tx),
        };
        worker_tx.send(item).await?;
        let res = rx.recv().await?.map_err(|_| Error::SeriesLookupError)?;
        let cssid = ChannelStatusSeriesId::new(res.series.into_inner().id());
        let (tx, rx) = async_channel::bounded(1);
        let item = ChannelInfoQuery {
            backend,
            channel,
            scalar_type: scalar_type.to_scylla_i32(),
            shape_dims: shape.to_scylla_vec(),
            tx: Box::pin(tx),
        };
        worker_tx.send(item).await?;
        let res = rx.recv().await?.map_err(|_| Error::SeriesLookupError)?;
        let sid = res.series.into_inner();
        let res = Self {
            cssid,
            sid,
            scalar_type,
            shape,

            // TODO
            ts_msp_last: todo!(),

            inserted_in_current_msp: 0,
            msp_max_entries: 64000,
            ts_msp_grid_last: 0,
        };
        Ok(res)
    }

    pub fn write(&mut self, ts: TsNano, ts_local: TsNano, val: DataValue, item_qu: &mut VecDeque<QueryItem>) {
        // TODO check for compatibility of the given data..

        // TODO compute the binned data here as well and flush completed bins if needed.

        // TODO decide on better msp/lsp: random offset!
        // As long as one writer is active, the msp is arbitrary.
        let (ts_msp, ts_msp_changed) = if self.inserted_in_current_msp >= self.msp_max_entries
            || TsNano::from_ns(self.ts_msp_last.ns() + HOUR) <= ts
        {
            let div = SEC * 10;
            let ts_msp = TsNano::from_ns(ts.ns() / div * div);
            if ts_msp == self.ts_msp_last {
                (ts_msp, false)
            } else {
                self.ts_msp_last = ts_msp.clone();
                self.inserted_in_current_msp = 1;
                (ts_msp, true)
            }
        } else {
            self.inserted_in_current_msp += 1;
            (self.ts_msp_last.clone(), false)
        };
        let ts_lsp = TsNano::from_ns(ts.ns() - ts_msp.ns());
        let ts_msp_grid = (ts.ns() / TS_MSP_GRID_UNIT / TS_MSP_GRID_SPACING * TS_MSP_GRID_SPACING) as u32;
        let ts_msp_grid = if self.ts_msp_grid_last != ts_msp_grid {
            self.ts_msp_grid_last = ts_msp_grid;
            Some(ts_msp_grid)
        } else {
            None
        };
        let item = InsertItem {
            series: self.sid.clone(),
            ts_msp: ts_msp.ns(),
            ts_lsp: ts_lsp.ns(),
            msp_bump: ts_msp_changed,
            pulse: 0,
            scalar_type: self.scalar_type.clone(),
            shape: self.shape.clone(),
            val,
            ts_msp_grid,
            ts_local: ts_local.ns(),
        };
        item_qu.push_back(QueryItem::Insert(item));
    }
}

#[test]
fn write_00() {
    let fut = async {
        let dbconf = &Database {
            name: "daqbuffer".into(),
            host: "localhost".into(),
            port: 5432,
            user: "daqbuffer".into(),
            pass: "daqbuffer".into(),
        };
        let scyconf = &ScyllaConfig {
            hosts: vec!["127.0.0.1:19042".into()],
            keyspace: "daqingest_test_00".into(),
        };
        let (pgc, pg_jh) = dbpg::conn::make_pg_client(dbconf).await?;
        dbpg::schema::schema_check(&pgc).await?;
        scywr::schema::migrate_scylla_data_schema(scyconf).await?;
        let scy = scywr::session::create_session(scyconf).await?;
        let stats = SeriesByChannelStats::new();
        let stats = Arc::new(stats);
        let (tx, jhs, jh) = dbpg::seriesbychannel::start_lookup_workers(1, dbconf, stats).await?;
        let backend = "bck-test-00";
        let channel = "chn-test-00";
        let scalar_type = ScalarType::U16;
        let shape = Shape::Scalar;
        let writer = SeriesWriter::establish(tx, backend.into(), channel.into(), scalar_type, shape).await?;
        eprintln!("{writer:?}");
        Ok::<_, Error>(())
    };
    taskrun::run(fut).unwrap();
}
