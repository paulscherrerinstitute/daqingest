pub mod write_v02;

use super::RoutesResources;
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
use netpod::ByteSize;
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
use series::SeriesId;
use serieswriter::msptool::dyngrid::MspSplitDyn;
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

macro_rules! debug_setup { ($($arg:tt)*) => { if true { log::debug!($($arg)*); } }; }

macro_rules! trace_input { ($($arg:tt)*) => { if true { log::trace!($($arg)*); } }; }

macro_rules! trace_queues { ($($arg:tt)*) => { if true { log::trace!($($arg)*); } }; }

type ValueSeriesWriter = SeriesWriter<WritableType, MspSplitDyn>;

#[derive(Debug, Serialize)]
struct WritableTypeState {
    series: SeriesId,
}

impl WritableTypeState {
    fn new(series: SeriesId, rt: RetentionTime) -> Self {
        Self { series }
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
        let bytes = ByteSize(self.byte_size());
        let data_item = self.1;
        serieswriter::writer::EmitRes { data_item, bytes }
    }
}

autoerr::create_error_v1!(
    name(Error, "MetricsIngest"),
    enum variants {
        UnsupportedContentType,
        Logic,
        SeriesWriter(#[from] serieswriter::writer::Error),
        MissingChannelName,
        MissingScalarType,
        MissingShape,
        SendError,
        Decode,
        FramedBytes(#[from] streams::framed_bytes::Error),
        InsertQueues(#[from] scywr::insertqueues::Error),
        Serde(#[from] serde_json::Error),
        Parse(String),
        NotSupported,
    },
);

pub async fn post_v01(
    (headers, Query(params), body): (HeaderMap, Query<HashMap<String, String>>, axum::body::Body),
    rres: Arc<RoutesResources>,
) -> Json<serde_json::Value> {
    match post_v01_try(headers, params, body, rres).await {
        Ok(k) => k,
        Err(e) => Json(serde_json::json!({
            "error": e.to_string(),
        })),
    }
}

async fn post_v01_try(
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
        channel,
        kind: SeriesKind::ChannelData,
        scalar_type: scalar_type.clone(),
        shape: shape.clone(),
        tx: Box::pin(tx),
    };
    rres.worker_tx.send(qu).await.unwrap();
    let chinfo = rx.recv().await.unwrap().unwrap();
    let msp_split = MspSplitDyn::new(1024 * 64, 1024 * 1024 * 10, rt.clone());
    let mut writer = SeriesWriter::new(chinfo.series.to_series(), msp_split)?;
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
                log::trace!("input stream done");
                break;
            }
        };
        trace_input!("got frame len {}", frame.len());
        let deque = iqdqs.deque(rt.clone());
        match &shape {
            Shape::Scalar => match &scalar_type {
                ScalarType::U8 => {
                    evpush_dim0::<u8, _>(&frame, deque, &mut writer, |x| {
                        DataValue::Scalar(ScalarValue::U8(x as _))
                    })?;
                }
                ScalarType::U16 => {
                    evpush_dim0::<u16, _>(&frame, deque, &mut writer, |x| {
                        DataValue::Scalar(ScalarValue::U16(x as _))
                    })?;
                }
                ScalarType::U32 => {
                    evpush_dim0::<u32, _>(&frame, deque, &mut writer, |x| {
                        DataValue::Scalar(ScalarValue::U32(x as _))
                    })?;
                }
                ScalarType::U64 => {
                    evpush_dim0::<u64, _>(&frame, deque, &mut writer, |x| {
                        DataValue::Scalar(ScalarValue::U64(x as _))
                    })?;
                }
                ScalarType::I8 => {
                    evpush_dim0::<i8, _>(&frame, deque, &mut writer, |x| DataValue::Scalar(ScalarValue::I8(x)))?;
                }
                ScalarType::I16 => {
                    evpush_dim0::<i16, _>(&frame, deque, &mut writer, |x| DataValue::Scalar(ScalarValue::I16(x)))?;
                }
                ScalarType::I32 => {
                    evpush_dim0::<i32, _>(&frame, deque, &mut writer, |x| DataValue::Scalar(ScalarValue::I32(x)))?;
                }
                ScalarType::I64 => {
                    evpush_dim0::<i64, _>(&frame, deque, &mut writer, |x| DataValue::Scalar(ScalarValue::I64(x)))?;
                }
                ScalarType::F32 => {
                    evpush_dim0::<f32, _>(&frame, deque, &mut writer, |x| DataValue::Scalar(ScalarValue::F32(x)))?;
                }
                ScalarType::F64 => {
                    evpush_dim0::<f64, _>(&frame, deque, &mut writer, |x| DataValue::Scalar(ScalarValue::F64(x)))?;
                }
                ScalarType::BOOL => {
                    evpush_dim0::<bool, _>(&frame, deque, &mut writer, |x| DataValue::Scalar(ScalarValue::Bool(x)))?;
                }
                ScalarType::STRING => {
                    evpush_dim0::<String, _>(&frame, deque, &mut writer, |x| {
                        DataValue::Scalar(ScalarValue::String(x.into()))
                    })?;
                }
                ScalarType::Enum => {
                    evpush_dim0_enum(&frame, deque, &mut writer)?;
                }
            },
            Shape::Wave(_) => match &scalar_type {
                ScalarType::U8 => {
                    evpush_dim1::<u8, _>(&frame, deque, &mut writer, |x| DataValue::Array(ArrayValue::U8(x)))?;
                }
                ScalarType::U16 => {
                    evpush_dim1::<u16, _>(&frame, deque, &mut writer, |x| DataValue::Array(ArrayValue::U16(x)))?;
                }
                ScalarType::U32 => {
                    evpush_dim1::<u32, _>(&frame, deque, &mut writer, |x| DataValue::Array(ArrayValue::U32(x)))?;
                }
                ScalarType::U64 => {
                    evpush_dim1::<u64, _>(&frame, deque, &mut writer, |x| DataValue::Array(ArrayValue::U64(x)))?;
                }
                ScalarType::I8 => {
                    evpush_dim1::<i8, _>(&frame, deque, &mut writer, |x| DataValue::Array(ArrayValue::I8(x)))?;
                }
                ScalarType::I16 => {
                    evpush_dim1::<i16, _>(&frame, deque, &mut writer, |x| DataValue::Array(ArrayValue::I16(x)))?;
                }
                ScalarType::I32 => {
                    evpush_dim1::<i32, _>(&frame, deque, &mut writer, |x| DataValue::Array(ArrayValue::I32(x)))?;
                }
                ScalarType::I64 => {
                    evpush_dim1::<i64, _>(&frame, deque, &mut writer, |x| DataValue::Array(ArrayValue::I64(x)))?;
                }
                ScalarType::F32 => {
                    evpush_dim1::<f32, _>(&frame, deque, &mut writer, |x| DataValue::Array(ArrayValue::F32(x)))?;
                }
                ScalarType::F64 => {
                    evpush_dim1::<f64, _>(&frame, deque, &mut writer, |x| DataValue::Array(ArrayValue::F64(x)))?;
                }
                ScalarType::BOOL => return Err(Error::NotSupported),
                ScalarType::STRING => return Err(Error::NotSupported),
                ScalarType::Enum => return Err(Error::NotSupported),
            },
            Shape::Image(_, _) => return Err(Error::NotSupported),
        }
        trace_queues!("frame send_all begin  {}  {}", iqdqs.summary(), iqtx.summary());
        iqtx.send_all(&mut iqdqs).await?;
        trace_queues!("frame send_all done  {}  {}", iqdqs.summary(), iqtx.summary());
        tick_writers(&mut writer, &mut iqdqs, rt.clone())?;
        trace_queues!("frame tick_writers done  {}  {}", iqdqs.summary(), iqtx.summary());
    }

    trace_queues!("after send_all begin  {}  {}", iqdqs.summary(), iqtx.summary());
    iqtx.send_all(&mut iqdqs).await?;
    trace_queues!("after send_all done  {}  {}", iqdqs.summary(), iqtx.summary());
    finish_writers(&mut writer, &mut iqdqs, rt.clone())?;
    trace_queues!("after finish_writers done  {}  {}", iqdqs.summary(), iqtx.summary());

    let ret = Json(serde_json::json!({}));
    Ok(ret)
}

fn evpush_dim0<T, F1>(
    frame: &Bytes,
    deque: &mut VecDeque<QueryItem>,
    writer: &mut ValueSeriesWriter,
    f1: F1,
) -> Result<(), Error>
where
    T: EventValueType,
    F1: Fn(<T as EventValueType>::IterTy1<'_>) -> DataValue,
{
    let evs: ContainerEvents<T> = ciborium::de::from_reader(Cursor::new(frame))
        .map_err(|e| {
            log::error!("cbor decode error {e}");
        })
        .map_err(|_| Error::Decode)?;
    // trace_input!("see events {:?}", evs);
    let stnow = SystemTime::now();
    let tsev = TsNano::from_system_time(stnow);
    let tsnow = Instant::now();
    let mut emit_state = WritableTypeState::new(writer.sid(), writer.rt());
    for (i, (ts, val)) in evs.iter_zip().enumerate() {
        let val = val.clone();
        trace_input!("ev  {:6}  {:20}  {:20?}", i, ts, val);
        let val = f1(val);
        writer.write(WritableType(ts, val), &mut emit_state, tsnow, tsev, deque)?;
    }
    Ok(())
}

// Special case for enum
fn evpush_dim0_enum(
    frame: &Bytes,
    deque: &mut VecDeque<QueryItem>,
    writer: &mut ValueSeriesWriter,
) -> Result<(), Error> {
    let evs: ContainerEvents<EnumVariant> = ciborium::de::from_reader(Cursor::new(frame))
        .map_err(|e| {
            log::error!("cbor decode error {e}");
        })
        .map_err(|_| Error::Decode)?;
    // trace_input!("see events {:?}", evs);
    let stnow = SystemTime::now();
    let tsev = TsNano::from_system_time(stnow);
    let tsnow = Instant::now();
    let mut emit_state = WritableTypeState::new(writer.sid(), writer.rt());
    for (i, (ts, val)) in evs.iter_zip().enumerate() {
        let val = val.clone();
        trace_input!("ev  {:6}  {:20}  {:20?}", i, ts, val);
        let val = DataValue::Scalar(ScalarValue::Enum(val.ix as i16, val.name.into()));
        writer.write(WritableType(ts, val), &mut emit_state, tsnow, tsev, deque)?;
    }
    Ok(())
}

fn evpush_dim1<T, F1>(
    frame: &Bytes,
    deque: &mut VecDeque<QueryItem>,
    writer: &mut ValueSeriesWriter,
    f1: F1,
) -> Result<(), Error>
where
    Vec<T>: EventValueType,
    F1: Fn(<Vec<T> as EventValueType>::IterTy1<'_>) -> DataValue,
{
    let evs: ContainerEvents<Vec<T>> = ciborium::de::from_reader(Cursor::new(frame))
        .map_err(|e| {
            log::error!("cbor decode error {e}");
        })
        .map_err(|_| Error::Decode)?;
    trace_input!("see events {:?}", evs);
    log::warn!("TODO require timestamp in input format");
    let stnow = SystemTime::now();
    let tsev = TsNano::from_system_time(stnow);
    let tsnow = Instant::now();
    let mut emit_state = WritableTypeState::new(writer.sid(), writer.rt());
    for (i, (ts, val)) in evs.iter_zip().enumerate() {
        let val = val.clone();
        trace_input!("ev  {:6}  {:20}  {:20?}", i, ts, val);
        let val = f1(val);
        writer.write(WritableType(ts, val), &mut emit_state, tsnow, tsev, deque)?;
    }
    Ok(())
}

fn tick_writers(writer: &mut ValueSeriesWriter, deques: &mut InsertDeques, rt: RetentionTime) -> Result<(), Error> {
    writer.tick(deques.deque(rt))?;
    Ok(())
}

fn finish_writers(writer: &mut ValueSeriesWriter, deques: &mut InsertDeques, rt: RetentionTime) -> Result<(), Error> {
    writer.tick(deques.deque(rt))?;
    Ok(())
}
