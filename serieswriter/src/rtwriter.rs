use crate::writer::SeriesWriter;
use async_channel::Sender;
use dbpg::seriesbychannel::ChannelInfoQuery;
use err::thiserror;
use err::ThisError;
use netpod::log::*;
use netpod::DtNano;
use netpod::ScalarType;
use netpod::SeriesKind;
use netpod::Shape;
use netpod::TsNano;
use scywr::insertqueues::InsertDeques;
use scywr::iteminsertqueue::DataValue;
use scywr::iteminsertqueue::QueryItem;
use series::ChannelStatusSeriesId;
use series::SeriesId;
use std::collections::VecDeque;
use std::time::Duration;
use std::time::SystemTime;

#[allow(unused)]
macro_rules! trace_rt_decision {
    ($($arg:tt)*) => {
        if false {
            trace!($($arg)*);
        }
    };
}

#[derive(Debug, ThisError)]
#[cstm(name = "SerieswriterRtwriter")]
pub enum Error {
    SeriesLookupError,
    SeriesWriter(#[from] crate::writer::Error),
}

#[derive(Debug, Clone)]
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
        ts_ioc: TsNano,
        ts_local: TsNano,
        val: DataValue,
        iqdqs: &mut InsertDeques,
    ) -> Result<((bool, bool, bool),), Error> {
        let sid = self.sid;
        if sid.id() == 6050300124140774549 {
            info!("write {:?}", val);
        }
        let (did_write_st,) = Self::write_inner(
            "ST",
            self.min_quiets.st,
            &mut self.state_st,
            &mut iqdqs.st_rf3_rx,
            ts_ioc,
            ts_local,
            val.clone(),
            sid,
        )?;
        let (did_write_mt,) = Self::write_inner(
            "MT",
            self.min_quiets.mt,
            &mut self.state_mt,
            &mut iqdqs.mt_rf3_rx,
            ts_ioc,
            ts_local,
            val.clone(),
            sid,
        )?;
        let (did_write_lt,) = Self::write_inner(
            "LT",
            self.min_quiets.lt,
            &mut self.state_lt,
            &mut iqdqs.lt_rf3_rx,
            ts_ioc,
            ts_local,
            val.clone(),
            sid,
        )?;
        Ok(((did_write_st, did_write_mt, did_write_lt),))
    }

    fn write_inner(
        rt: &str,
        min_quiet: Duration,
        state: &mut State,
        deque: &mut VecDeque<QueryItem>,
        ts_ioc: TsNano,
        ts_local: TsNano,
        val: DataValue,
        sid: SeriesId,
    ) -> Result<(bool,), Error> {
        // Decide whether we want to write.
        // Use the IOC time for the decision whether to write.
        // But use the ingest local time as the primary index.
        let do_write = if let Some(last) = &state.last_ins {
            if ts_ioc == last.ts_ioc {
                trace_rt_decision!("{rt}  {sid}  ignore, because same IOC time  {ts_ioc:?}  {ts_local:?}");
                false
            } else if ts_local < last.ts_local {
                trace_rt_decision!("{rt}  {sid}  ignore, because ts_local  rewind  {ts_ioc:?}  {ts_local:?}");
                false
            } else if ts_local.ms() - last.ts_local.ms() < 1000 * min_quiet.as_secs() {
                trace_rt_decision!("{rt}  {sid}  ignore, because not min quiet");
                false
            } else if ts_local.delta(last.ts_local) < DtNano::from_ms(5) {
                trace_rt_decision!("{rt}  {sid}  ignore, because store rate cap");
                false
            } else if val == last.val {
                trace_rt_decision!("{rt}  {sid}  ignore, because value did not change");
                false
            } else {
                trace_rt_decision!("{rt}  {sid}  accept");
                true
            }
        } else {
            true
        };
        if do_write {
            state.last_ins = Some(LastIns {
                ts_local,
                ts_ioc,
                val: val.clone(),
            });
            state.writer.write(ts_ioc, ts_local, val.clone(), deque)?;
        }
        Ok((do_write,))
    }

    pub fn tick(&mut self, iqdqs: &mut InsertDeques) -> Result<(), Error> {
        self.state_st.writer.tick(&mut iqdqs.st_rf3_rx)?;
        self.state_mt.writer.tick(&mut iqdqs.mt_rf3_rx)?;
        self.state_lt.writer.tick(&mut iqdqs.lt_rf3_rx)?;
        Ok(())
    }
}

#[derive(Debug)]
struct LastIns {
    ts_local: TsNano,
    ts_ioc: TsNano,
    val: DataValue,
}

#[derive(Debug)]
struct State {
    writer: SeriesWriter,
    last_ins: Option<LastIns>,
}
