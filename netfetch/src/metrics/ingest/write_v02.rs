use crate::metrics::RoutesResources;
use axum::Json;
use axum::extract::FromRequest;
use axum::extract::Query;
use axum::http::HeaderMap;
use bytes::Bytes;
use core::fmt;
use dbpg::seriesbychannel::ChannelInfoQuery;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use items_2::binning::container_events::ContainerEvents;
use items_2::binning::container_events::EventValueType;
use netpod::APP_CBOR_FRAMED;
use netpod::DaqbufChannelConfig;
use netpod::EnumVariant;
use netpod::ScalarType;
use netpod::SeriesKind;
use netpod::Shape;
use netpod::TsNano;
use netpod::log;
use netpod::ttl::RetentionTime;
use scywr::insertqueues::InsertDeques;
use scywr::iteminsertqueue::ArrayValue;
use scywr::iteminsertqueue::DataValue;
use scywr::iteminsertqueue::QueryItem;
use scywr::iteminsertqueue::ScalarValue;
use serde::Deserialize;
use serde::Serialize;
use series::ChannelStatusSeriesId;
use series::SeriesId;
use serieswriter::binwriter::BinWriter;
use serieswriter::binwriter::DiscardFirstOutput;
use serieswriter::binwriter::WriteCntZero;
use serieswriter::msptool::MspSplit;
use serieswriter::rtwriter::MinQuiets;
use serieswriter::writer::EmittableType;
use serieswriter::writer::SeriesWriter;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use streams::framed_bytes::FramedBytesStream;
use taskrun::tokio::time::timeout;

macro_rules! error { ($($arg:expr),*) => ( if true { log::error!($($arg),*) } ); }

macro_rules! info { ($($arg:expr),*) => ( if true { log::info!($($arg),*) } ); }

macro_rules! debug_setup { ($($arg:expr),*) => ( if true { log::debug!($($arg),*) } ); }

macro_rules! trace_input { ($($arg:expr),*) => ( if true { log::trace!($($arg),*) } ); }

macro_rules! trace_queues { ($($arg:expr),*) => ( if true { log::trace!($($arg),*) } ); }

autoerr::create_error_v1!(
    name(Error, "MetricsIngestV02Write"),
    enum variants {
        UnsupportedContentType,
        Logic,
        SeriesWriter(#[from] serieswriter::writer::Error),
        MissingChannelName,
        MissingScalarType,
        MissingShape,
        MissingSeriesId,
        SendError,
        Decode,
        FramedBytes(#[from] streams::framed_bytes::Error),
        InsertQueues(#[from] scywr::insertqueues::Error),
        Serde(#[from] serde_json::Error),
        Parse(String),
        NotSupported,
        WorkerQueueMissing,
        ConfigLookup,
        Postgres(#[from] dbpg::err::Error),
        TaskJoin(#[from] taskrun::tokio::task::JoinError),
        ConfBySeries(#[from] dbpg::confbyseries::Error),
    },
);

type ValueSeriesWriter = SeriesWriter<WritableType>;

#[derive(Debug, Serialize)]
struct WritableTypeState {
    series: SeriesId,
    msp_split_data: MspSplit,
}

impl WritableTypeState {
    fn new(series: SeriesId) -> Self {
        Self {
            series,
            msp_split_data: MspSplit::new(10000, 1024 * 256),
        }
    }
}

#[derive(Debug, Clone)]
struct WritableType(TsNano, DataValue);

impl EmittableType for WritableType {
    type State = WritableTypeState;

    fn ts(&self) -> TsNano {
        self.0.clone()
    }

    fn has_change(&self, k: &Self) -> bool {
        true
    }

    fn byte_size(&self) -> u32 {
        8 + self.1.byte_size()
    }

    fn into_query_item(
        self,
        ts_net: Instant,
        tsev: TsNano,
        state: &mut <Self as EmittableType>::State,
    ) -> serieswriter::writer::EmitRes {
        let (ts_msp, ts_lsp, ts_msp_chg) = state.msp_split_data.split(self.0.clone(), self.byte_size());
        let item = QueryItem::Insert(scywr::iteminsertqueue::InsertItem {
            series: state.series.clone(),
            ts_msp: ts_msp.to_ts_ms(),
            ts_lsp,
            val: self.1.clone(),
            ts_net,
        });
        let mut items = smallvec::SmallVec::new();
        items.push(item);
        if ts_msp_chg {
            items.push(QueryItem::Msp(scywr::iteminsertqueue::MspItem::new(
                state.series.clone(),
                ts_msp.to_ts_ms(),
                ts_net,
            )));
        }
        serieswriter::writer::EmitRes {
            items,
            bytes: self.byte_size(),
            status: 0,
        }
    }
}

struct EvPushParams<'a> {
    frame: &'a Bytes,
    writer: &'a mut ValueSeriesWriter,
    binwriter: &'a mut Option<BinWriter>,
    iqdqs: &'a mut InsertDeques,
    chname: &'a str,
    rt: RetentionTime,
    scalar_type: ScalarType,
    shape: Shape,
}

fn evpush_dim0<T, F1>(mut params: EvPushParams, f1: F1) -> Result<(), Error>
where
    T: EventValueType,
    F1: Fn(<T as EventValueType>::IterTy1<'_>) -> DataValue,
{
    let evs: ContainerEvents<T> = ciborium::de::from_reader(Cursor::new(params.frame))
        .map_err(|e| {
            error!("cbor decode error {e}");
        })
        .map_err(|_| Error::Decode)?;
    // trace_input!("see events {:?}", evs);
    let stnow = SystemTime::now();
    let tsev = TsNano::from_system_time(stnow);
    let tsnow = Instant::now();
    let mut emit_state = WritableTypeState::new(params.writer.sid());
    if evs.len() != 0 {
        if params.binwriter.is_none() {
            for (i, (ts, val)) in evs.iter_zip().enumerate() {
                let min_quiets = MinQuiets::http_ingest_default();
                let is_polled = false;
                let emit_znt_zero_default = WriteCntZero::Disable;
                let do_discard_front = DiscardFirstOutput::Enable;
                let cssid = ChannelStatusSeriesId::new(0);
                let sid = params.writer.sid();
                let wr2 = BinWriter::new(
                    ts,
                    min_quiets,
                    is_polled,
                    emit_znt_zero_default,
                    do_discard_front,
                    cssid,
                    sid,
                    params.scalar_type.clone(),
                    params.shape.clone(),
                    params.chname.into(),
                )
                .unwrap();
                *params.binwriter = Some(wr2);
                break;
            }
        }
        let binwriter = params.binwriter.as_mut().unwrap();
        for (i, (ts, val)) in evs.iter_zip().enumerate() {
            let val = val.clone();
            let val = f1(val);
            let val_f32 = val.f32_for_binning();
            binwriter.ingest(ts, val_f32, params.iqdqs).unwrap();
        }
        let deque = params.iqdqs.deque(params.rt.clone());
        for (i, (ts, val)) in evs.iter_zip().enumerate() {
            let val = val.clone();
            trace_input!("ev  {:6}  {:20}  {:20?}", i, ts, val);
            let val = f1(val);
            params
                .writer
                .write(WritableType(ts, val), &mut emit_state, tsnow, tsev, deque)?;
        }
    } else {
    }
    Ok(())
}

fn evpush_dim0_enum(mut params: EvPushParams) -> Result<(), Error> {
    let evs: ContainerEvents<EnumVariant> = ciborium::de::from_reader(Cursor::new(params.frame))
        .map_err(|e| {
            error!("cbor decode error {e}");
        })
        .map_err(|_| Error::Decode)?;
    // trace_input!("see events {:?}", evs);
    let stnow = SystemTime::now();
    let tsev = TsNano::from_system_time(stnow);
    let tsnow = Instant::now();
    let mut emit_state = WritableTypeState::new(params.writer.sid());
    let deque = params.iqdqs.deque(params.rt.clone());
    for (i, (ts, val)) in evs.iter_zip().enumerate() {
        let val = val.clone();
        trace_input!("ev  {:6}  {:20}  {:20?}", i, ts, val);
        let val = DataValue::Scalar(ScalarValue::Enum(val.ix as i16, val.name.into()));
        params
            .writer
            .write(WritableType(ts, val), &mut emit_state, tsnow, tsev, deque)?;
    }
    Ok(())
}

fn evpush_dim1<T, F1>(mut params: EvPushParams, f1: F1) -> Result<(), Error>
where
    Vec<T>: EventValueType,
    F1: Fn(<Vec<T> as EventValueType>::IterTy1<'_>) -> DataValue,
{
    let evs: ContainerEvents<Vec<T>> = ciborium::de::from_reader(Cursor::new(params.frame))
        .map_err(|e| {
            error!("cbor decode error {e}");
        })
        .map_err(|_| Error::Decode)?;
    trace_input!("see events {:?}", evs);
    error!("TODO require timestamp in input format");
    let stnow = SystemTime::now();
    let tsev = TsNano::from_system_time(stnow);
    let tsnow = Instant::now();
    let mut emit_state = WritableTypeState::new(params.writer.sid());
    let deque = params.iqdqs.deque(params.rt.clone());
    for (i, (ts, val)) in evs.iter_zip().enumerate() {
        let val = val.clone();
        trace_input!("ev  {:6}  {:20}  {:20?}", i, ts, val);
        let val = f1(val);
        params
            .writer
            .write(WritableType(ts, val), &mut emit_state, tsnow, tsev, deque)?;
    }
    Ok(())
}

fn frame_write(
    frame: &Bytes,
    chname: &str,
    rt: RetentionTime,
    scalar_type: ScalarType,
    shape: Shape,
    writer: &mut SeriesWriter<WritableType>,
    binwriter: &mut Option<BinWriter>,
    iqdqs: &mut InsertDeques,
) -> Result<(), Error> {
    let params = EvPushParams {
        frame,
        writer,
        binwriter,
        iqdqs,
        chname,
        rt,
        scalar_type: scalar_type.clone(),
        shape: shape.clone(),
    };
    match &shape {
        Shape::Scalar => match &scalar_type {
            ScalarType::U8 => {
                evpush_dim0::<u8, _>(params, |x| DataValue::Scalar(ScalarValue::U8(x as _)))?;
            }
            ScalarType::U16 => {
                evpush_dim0::<u16, _>(params, |x| DataValue::Scalar(ScalarValue::U16(x as _)))?;
            }
            ScalarType::U32 => {
                evpush_dim0::<u32, _>(params, |x| DataValue::Scalar(ScalarValue::U32(x as _)))?;
            }
            ScalarType::U64 => {
                evpush_dim0::<u64, _>(params, |x| DataValue::Scalar(ScalarValue::U64(x as _)))?;
            }
            ScalarType::I8 => {
                evpush_dim0::<i8, _>(params, |x| DataValue::Scalar(ScalarValue::I8(x)))?;
            }
            ScalarType::I16 => {
                evpush_dim0::<i16, _>(params, |x| DataValue::Scalar(ScalarValue::I16(x)))?;
            }
            ScalarType::I32 => {
                evpush_dim0::<i32, _>(params, |x| DataValue::Scalar(ScalarValue::I32(x)))?;
            }
            ScalarType::I64 => {
                evpush_dim0::<i64, _>(params, |x| DataValue::Scalar(ScalarValue::I64(x)))?;
            }
            ScalarType::F32 => {
                evpush_dim0::<f32, _>(params, |x| DataValue::Scalar(ScalarValue::F32(x)))?;
            }
            ScalarType::F64 => {
                evpush_dim0::<f64, _>(params, |x| DataValue::Scalar(ScalarValue::F64(x)))?;
            }
            ScalarType::BOOL => {
                evpush_dim0::<bool, _>(params, |x| DataValue::Scalar(ScalarValue::Bool(x)))?;
            }
            ScalarType::STRING => {
                evpush_dim0::<String, _>(params, |x| DataValue::Scalar(ScalarValue::String(x.into())))?;
            }
            ScalarType::Enum => {
                evpush_dim0_enum(params)?;
            }
        },
        Shape::Wave(_) => match &scalar_type {
            ScalarType::U8 => {
                evpush_dim1::<u8, _>(params, |x| DataValue::Array(ArrayValue::U8(x)))?;
            }
            ScalarType::U16 => {
                evpush_dim1::<u16, _>(params, |x| DataValue::Array(ArrayValue::U16(x)))?;
            }
            ScalarType::U32 => {
                evpush_dim1::<u32, _>(params, |x| DataValue::Array(ArrayValue::U32(x)))?;
            }
            ScalarType::U64 => {
                evpush_dim1::<u64, _>(params, |x| DataValue::Array(ArrayValue::U64(x)))?;
            }
            ScalarType::I8 => {
                evpush_dim1::<i8, _>(params, |x| DataValue::Array(ArrayValue::I8(x)))?;
            }
            ScalarType::I16 => {
                evpush_dim1::<i16, _>(params, |x| DataValue::Array(ArrayValue::I16(x)))?;
            }
            ScalarType::I32 => {
                evpush_dim1::<i32, _>(params, |x| DataValue::Array(ArrayValue::I32(x)))?;
            }
            ScalarType::I64 => {
                evpush_dim1::<i64, _>(params, |x| DataValue::Array(ArrayValue::I64(x)))?;
            }
            ScalarType::F32 => {
                evpush_dim1::<f32, _>(params, |x| DataValue::Array(ArrayValue::F32(x)))?;
            }
            ScalarType::F64 => {
                evpush_dim1::<f64, _>(params, |x| DataValue::Array(ArrayValue::F64(x)))?;
            }
            ScalarType::BOOL => return Err(Error::NotSupported),
            ScalarType::STRING => return Err(Error::NotSupported),
            ScalarType::Enum => return Err(Error::NotSupported),
        },
        Shape::Image(_, _) => return Err(Error::NotSupported),
    }
    Ok(())
}

async fn write_with_fresh_msps_inner(
    headers: HeaderMap,
    params: HashMap<String, String>,
    body: axum::body::Body,
    rres: Arc<RoutesResources>,
) -> Result<Json<serde_json::Value>, Error> {
    if let Some(ct) = headers.get("content-type") {
        if let Ok(s) = ct.to_str() {
            if s == APP_CBOR_FRAMED {
            } else {
                return Err(Error::UnsupportedContentType);
            }
        } else {
            return Err(Error::UnsupportedContentType);
        }
    } else {
        return Err(Error::UnsupportedContentType);
    };
    debug_setup!("params {:?}", params);
    let stnow = SystemTime::now();
    let worker_tx = rres.worker_tx.clone();
    let backend = rres.backend.clone();
    let channel = params.get("channelName").ok_or(Error::MissingChannelName)?.to_string();
    let s = params.get("scalarType").ok_or(Error::MissingScalarType)?;
    let scalar_type = ScalarType::from_variant_str(&s).map_err(|e| Error::Parse(e.to_string()))?;
    let shape: Shape = serde_json::from_str(params.get("shape").map_or("[]", |x| x.as_str()))?;
    let rt: RetentionTime = params
        .get("retentionTime")
        .and_then(|x| x.parse().ok())
        .unwrap_or(RetentionTime::Short);
    debug_setup!(
        "establishing series writer for {:?} {:?} {:?} {:?}",
        channel,
        scalar_type,
        shape,
        rt
    );
    let (tx, rx) = async_channel::bounded(8);
    let qu = ChannelInfoQuery {
        backend,
        channel: channel.clone(),
        kind: SeriesKind::ChannelData,
        scalar_type: scalar_type.clone(),
        shape: shape.clone(),
        tx: Box::pin(tx),
    };
    rres.worker_tx.send(qu).await.map_err(|_| Error::WorkerQueueMissing)?;
    let chinfo = rx
        .recv()
        .await
        .map_err(|_| Error::ConfigLookup)?
        .map_err(|_| Error::ConfigLookup)?;
    let mut writer = SeriesWriter::new(chinfo.series.to_series())?;
    let mut binwriter = None;
    debug_setup!("series writer established");
    let mut iqdqs = InsertDeques::new();
    let mut iqtx = rres.iqtx.clone();
    let mut frames = FramedBytesStream::new(
        body.into_data_stream()
            .map_err(|_| streams::framed_bytes::Error::DataInput),
    );
    loop {
        let x = timeout(Duration::from_millis(2000), frames.try_next()).await;
        let x = match x {
            Ok(x) => x,
            Err(_) => {
                tick_writers(&mut writer, &mut iqdqs, rt.clone())?;
                continue;
            }
        };
        let frame = match x? {
            Some(x) => x,
            None => {
                trace_input!("input stream done");
                break;
            }
        };
        trace_input!("got frame len {}", frame.len());
        frame_write(
            &frame,
            &channel,
            rt.clone(),
            scalar_type.clone(),
            shape.clone(),
            &mut writer,
            &mut binwriter,
            &mut iqdqs,
        )?;
        trace_queues!("frame send_all begin  {}  {}", iqdqs.summary(), iqtx.summary());
        iqtx.send_all(&mut iqdqs).await?;
        trace_queues!("frame send_all done  {}  {}", iqdqs.summary(), iqtx.summary());
        tick_writers(&mut writer, &mut iqdqs, rt.clone())?;
        trace_queues!("frame tick_writers done  {}  {}", iqdqs.summary(), iqtx.summary());
        if let Some(binwriter) = binwriter.as_mut() {
            binwriter.tick(&mut iqdqs).unwrap();
        }
    }
    trace_queues!("after send_all begin  {}  {}", iqdqs.summary(), iqtx.summary());
    iqtx.send_all(&mut iqdqs).await?;
    trace_queues!("after send_all done  {}  {}", iqdqs.summary(), iqtx.summary());
    finish_writers(&mut writer, &mut iqdqs, rt.clone())?;
    trace_queues!("after finish_writers done  {}  {}", iqdqs.summary(), iqtx.summary());
    if let Some(binwriter) = binwriter.as_mut() {
        binwriter.tick(&mut iqdqs).unwrap();
    }
    let ret = Json(serde_json::json!({
        "status": "ok",
        "chinfo": chinfo,
        "series_id": chinfo.series.to_series().id(),
    }));
    Ok(ret)
}

pub async fn write_with_fresh_msps(
    (headers, Query(params), body): (HeaderMap, Query<HashMap<String, String>>, axum::body::Body),
    rres: Arc<RoutesResources>,
) -> Json<serde_json::Value> {
    match write_with_fresh_msps_inner(headers, params, body, rres).await {
        Ok(k) => k,
        Err(e) => Json(serde_json::json!({
            "error": e.to_string(),
        })),
    }
}

fn tick_writers(writer: &mut ValueSeriesWriter, deques: &mut InsertDeques, rt: RetentionTime) -> Result<(), Error> {
    writer.tick(deques.deque(rt))?;
    Ok(())
}

fn finish_writers(writer: &mut ValueSeriesWriter, deques: &mut InsertDeques, rt: RetentionTime) -> Result<(), Error> {
    writer.tick(deques.deque(rt))?;
    Ok(())
}

async fn register_series(
    headers: HeaderMap,
    params: HashMap<String, String>,
    body: axum::body::Body,
    rres: Arc<RoutesResources>,
) -> Result<Json<serde_json::Value>, Error> {
    if let Some(ct) = headers.get("content-type") {
        if let Ok(s) = ct.to_str() {
            if s == APP_CBOR_FRAMED {
            } else {
                return Err(Error::UnsupportedContentType);
            }
        } else {
            return Err(Error::UnsupportedContentType);
        }
    } else {
        return Err(Error::UnsupportedContentType);
    };
    debug_setup!("params {:?}", params);
    let stnow = SystemTime::now();
    let worker_tx = rres.worker_tx.clone();
    let backend = rres.backend.clone();
    let channel = params.get("channelName").ok_or(Error::MissingChannelName)?.into();
    let s = params.get("scalarType").ok_or(Error::MissingScalarType)?;
    let scalar_type = ScalarType::from_variant_str(&s).map_err(|e| Error::Parse(e.to_string()))?;
    let shape: Shape = serde_json::from_str(params.get("shape").map_or("[]", |x| x.as_str()))?;
    debug_setup!("register series  {:?}  {:?}  {:?}", channel, scalar_type, shape);
    let (tx, rx) = async_channel::bounded(8);
    let qu = ChannelInfoQuery {
        backend,
        channel,
        kind: SeriesKind::ChannelData,
        scalar_type: scalar_type.clone(),
        shape: shape.clone(),
        tx: Box::pin(tx),
    };
    rres.worker_tx.send(qu).await.map_err(|_| Error::WorkerQueueMissing)?;
    let chinfo = rx
        .recv()
        .await
        .map_err(|_| Error::ConfigLookup)?
        .map_err(|_| Error::ConfigLookup)?;
    let ret = serde_json::json!({
        "register_series": {
            "status": "ok",
            "chinfo": chinfo,
            "seriesId": chinfo.series.to_series().id(),
        }
    });
    let ret = Json(ret);
    Ok(ret)
}

async fn write_msp(
    headers: HeaderMap,
    params: HashMap<String, String>,
    body: axum::body::Body,
    rres: Arc<RoutesResources>,
) -> Result<Json<serde_json::Value>, Error> {
    if let Some(ct) = headers.get("content-type") {
        if let Ok(s) = ct.to_str() {
            if s == APP_CBOR_FRAMED {
            } else {
                return Err(Error::UnsupportedContentType);
            }
        } else {
            return Err(Error::UnsupportedContentType);
        }
    } else {
        return Err(Error::UnsupportedContentType);
    };
    debug_setup!("params {:?}", params);
    let stnow = SystemTime::now();
    let worker_tx = rres.worker_tx.clone();
    let backend = rres.backend.clone();
    let series_id: u64 = params
        .get("seriesId")
        .ok_or(Error::MissingSeriesId)?
        .parse()
        .map_err(|_| Error::MissingSeriesId)?;
    let series = SeriesId::new(series_id);
    let rt: RetentionTime = params
        .get("retentionTime")
        .and_then(|x| x.parse().ok())
        .unwrap_or(RetentionTime::Short);
    let (conn, pgjh) = dbpg::conn::make_pg_client(&rres.pgconf).await?;
    let conf = dbpg::confbyseries::channel_config_by_series(&conn, series).await?;
    drop(conn);
    pgjh.await?;
    match conf {
        Some(conf) => write_events_exact_2(conf, rt, body, rres).await,
        None => {
            info!("series id not found  {:?}", series);
            let ret = serde_json::json!({
                "write_events_exact": {
                    "status": "SeriesIdNotFound",
                }
            });
            let ret = Json(ret);
            Ok(ret)
        }
    }
}

async fn write_events_exact_2(
    conf: DaqbufChannelConfig,
    rt: RetentionTime,
    body: axum::body::Body,
    rres: Arc<RoutesResources>,
) -> Result<Json<serde_json::Value>, Error> {
    debug_setup!("write_events_exact  {:?}  {:?}", conf, rt);
    let series = SeriesId::new(conf.series);
    let mut writer = SeriesWriter::new(series)?;
    let mut binwriter = None;
    debug_setup!("series writer established");
    let mut iqdqs = InsertDeques::new();
    let mut iqtx = rres.iqtx.clone();
    let mut frames = FramedBytesStream::new(
        body.into_data_stream()
            .map_err(|_| streams::framed_bytes::Error::DataInput),
    );
    loop {
        match timeout(Duration::from_millis(2000), frames.try_next()).await {
            Ok(k) => match k? {
                Some(frame) => {
                    trace_input!("got frame len {}", frame.len());
                    let deque = iqdqs.deque(rt.clone());
                    frame_write(
                        &frame,
                        &conf.name,
                        rt.clone(),
                        conf.scalar_type.clone(),
                        conf.shape.clone(),
                        &mut writer,
                        &mut binwriter,
                        &mut iqdqs,
                    )?;
                    trace_queues!("frame send_all begin  {}  {}", iqdqs.summary(), iqtx.summary());
                    iqtx.send_all(&mut iqdqs).await?;
                    trace_queues!("frame send_all done  {}  {}", iqdqs.summary(), iqtx.summary());
                    tick_writers(&mut writer, &mut iqdqs, rt.clone())?;
                    trace_queues!("frame tick_writers done  {}  {}", iqdqs.summary(), iqtx.summary());
                }
                None => {
                    trace_input!("input stream done");
                    break;
                }
            },
            Err(_) => {
                tick_writers(&mut writer, &mut iqdqs, rt.clone())?;
                continue;
            }
        }
    }
    trace_queues!("after send_all begin  {}  {}", iqdqs.summary(), iqtx.summary());
    iqtx.send_all(&mut iqdqs).await?;
    trace_queues!("after send_all done  {}  {}", iqdqs.summary(), iqtx.summary());
    finish_writers(&mut writer, &mut iqdqs, rt.clone())?;
    trace_queues!("after finish_writers done  {}  {}", iqdqs.summary(), iqtx.summary());
    let ret = serde_json::json!({
        "write_events_exact": {
            "status": "ok",
            "seriesId": series.id(),
        }
    });
    let ret = Json(ret);
    Ok(ret)
}

async fn write_events_exact(
    headers: HeaderMap,
    params: HashMap<String, String>,
    body: axum::body::Body,
    rres: Arc<RoutesResources>,
) -> Result<Json<serde_json::Value>, Error> {
    if let Some(ct) = headers.get("content-type") {
        if let Ok(s) = ct.to_str() {
            if s == APP_CBOR_FRAMED {
            } else {
                return Err(Error::UnsupportedContentType);
            }
        } else {
            return Err(Error::UnsupportedContentType);
        }
    } else {
        return Err(Error::UnsupportedContentType);
    };
    debug_setup!("params {:?}", params);
    let stnow = SystemTime::now();
    let worker_tx = rres.worker_tx.clone();
    let backend = rres.backend.clone();
    let series_id: u64 = params
        .get("seriesId")
        .ok_or(Error::MissingSeriesId)?
        .parse()
        .map_err(|_| Error::MissingSeriesId)?;
    let series = SeriesId::new(series_id);
    let rt: RetentionTime = params
        .get("retentionTime")
        .and_then(|x| x.parse().ok())
        .unwrap_or(RetentionTime::Short);
    let (conn, pgjh) = dbpg::conn::make_pg_client(&rres.pgconf).await?;
    let conf = dbpg::confbyseries::channel_config_by_series(&conn, series).await?;
    drop(conn);
    pgjh.await?;
    match conf {
        Some(conf) => write_events_exact_2(conf, rt, body, rres).await,
        None => {
            info!("series id not found  {:?}", series);
            let ret = serde_json::json!({
                "write_events_exact": {
                    "status": "SeriesIdNotFound",
                }
            });
            let ret = Json(ret);
            Ok(ret)
        }
    }
}
