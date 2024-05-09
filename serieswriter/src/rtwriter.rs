use crate::writer::SeriesWriter;
use async_channel::Sender;
use dbpg::seriesbychannel::ChannelInfoQuery;
use err::thiserror;
use err::ThisError;
use netpod::ScalarType;
use netpod::SeriesKind;
use netpod::Shape;
use netpod::TsNano;
use scywr::insertqueues::InsertDeques;
use scywr::iteminsertqueue::DataValue;
use series::ChannelStatusSeriesId;
use series::SeriesId;
use std::time::Duration;
use std::time::SystemTime;

#[derive(Debug, ThisError)]
pub enum Error {
    SeriesLookupError,
    SeriesWriter(#[from] crate::writer::Error),
}

#[derive(Debug)]
pub struct MinQuiets {
    pub st: Duration,
    pub mt: Duration,
    pub lt: Duration,
}

#[derive(Debug)]
pub struct RtWriter {
    sid: SeriesId,
    scalar_type: ScalarType,
    shape: Shape,
    state_st: State,
    state_mt: State,
    state_lt: State,
    min_quiets: MinQuiets,
}

impl RtWriter {
    pub async fn new(
        channel_info_tx: Sender<ChannelInfoQuery>,
        cssid: ChannelStatusSeriesId,
        backend: String,
        channel: String,
        scalar_type: ScalarType,
        shape: Shape,
        min_quiets: MinQuiets,
        stnow: SystemTime,
    ) -> Result<Self, Error> {
        let sid = {
            let (tx, rx) = async_channel::bounded(1);
            let item = ChannelInfoQuery {
                backend,
                channel,
                kind: SeriesKind::ChannelData,
                scalar_type: scalar_type.clone(),
                shape: shape.clone(),
                tx: Box::pin(tx),
            };
            channel_info_tx.send(item).await.map_err(|_| Error::SeriesLookupError)?;
            let res = rx
                .recv()
                .await
                .map_err(|_| Error::SeriesLookupError)?
                .map_err(|_| Error::SeriesLookupError)?;
            res.series.to_series()
        };
        let state_st = {
            let writer =
                SeriesWriter::establish_with_cssid_sid(cssid, sid, scalar_type.clone(), shape.clone(), stnow).await?;
            State { writer, last_ins: None }
        };
        let state_mt = {
            let writer =
                SeriesWriter::establish_with_cssid_sid(cssid, sid, scalar_type.clone(), shape.clone(), stnow).await?;
            State { writer, last_ins: None }
        };
        let state_lt = {
            let writer =
                SeriesWriter::establish_with_cssid_sid(cssid, sid, scalar_type.clone(), shape.clone(), stnow).await?;
            State { writer, last_ins: None }
        };
        let ret = Self {
            sid,
            scalar_type,
            shape,
            state_st,
            state_mt,
            state_lt,
            min_quiets,
        };
        Ok(ret)
    }

    pub fn sid(&self) -> SeriesId {
        self.sid.clone()
    }

    pub fn scalar_type(&self) -> ScalarType {
        self.scalar_type.clone()
    }

    pub fn shape(&self) -> Shape {
        self.shape.clone()
    }

    pub fn write(
        &mut self,
        ts: TsNano,
        ts_local: TsNano,
        val: DataValue,
        iqdqs: &mut InsertDeques,
    ) -> Result<(), Error> {
        // Decide whether we want to write.
        {
            let min_quiet = self.min_quiets.st;
            let deque = &mut iqdqs.st_rf3_rx;
            if self.state_st.last_ins.as_ref().map_or(true, |x| {
                if x.0 >= ts_local {
                    // bad clock, ignore.
                    // TODO count in stats.
                    false
                } else if ts_local.ms() - x.0.ms() < 1000 * min_quiet.as_secs() {
                    false
                } else {
                    val != x.1
                }
            }) {
                self.state_st.last_ins = Some((ts, val.clone()));
                self.state_st.writer.write(ts, ts_local, val.clone(), deque)?;
            }
        }
        {
            let min_quiet = self.min_quiets.mt;
            let deque = &mut iqdqs.mt_rf3_rx;
            if self.state_mt.last_ins.as_ref().map_or(true, |x| {
                if x.0 >= ts_local {
                    // bad clock, ignore.
                    // TODO count in stats.
                    false
                } else if ts_local.ms() - x.0.ms() < 1000 * min_quiet.as_secs() {
                    false
                } else {
                    val != x.1
                }
            }) {
                self.state_mt.last_ins = Some((ts, val.clone()));
                self.state_mt.writer.write(ts, ts_local, val.clone(), deque)?;
            }
        }
        {
            let min_quiet = self.min_quiets.lt;
            let deque = &mut iqdqs.lt_rf3_rx;
            if self.state_lt.last_ins.as_ref().map_or(true, |x| {
                if x.0 >= ts_local {
                    // bad clock, ignore.
                    // TODO count in stats.
                    false
                } else if ts_local.ms() - x.0.ms() < 1000 * min_quiet.as_secs() {
                    false
                } else {
                    val != x.1
                }
            }) {
                self.state_lt.last_ins = Some((ts, val.clone()));
                self.state_lt.writer.write(ts, ts_local, val.clone(), deque)?;
            }
        }
        Ok(())
    }

    pub fn tick(&mut self, iqdqs: &mut InsertDeques) -> Result<(), Error> {
        self.state_st.writer.tick(&mut iqdqs.st_rf3_rx)?;
        self.state_mt.writer.tick(&mut iqdqs.mt_rf3_rx)?;
        self.state_lt.writer.tick(&mut iqdqs.lt_rf3_rx)?;
        Ok(())
    }
}

#[derive(Debug)]
struct State {
    writer: SeriesWriter,
    last_ins: Option<(TsNano, DataValue)>,
}
