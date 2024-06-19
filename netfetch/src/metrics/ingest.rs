use super::RoutesResources;
use axum::extract::FromRequest;
use axum::extract::Query;
use axum::Json;
use err::thiserror;
use err::ThisError;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use items_2::eventsdim0::EventsDim0;
use netpod::log::*;
use netpod::ScalarType;
use netpod::Shape;
use netpod::TsNano;
use scywr::insertqueues::InsertDeques;
use scywr::iteminsertqueue::DataValue;
use scywr::iteminsertqueue::QueryItem;
use scywr::iteminsertqueue::ScalarValue;
use serieswriter::writer::SeriesWriter;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::io::Cursor;
use std::sync::Arc;
use std::time::SystemTime;
use streams::framed_bytes::FramedBytesStream;
// use core::io::BorrowedBuf;

#[derive(Debug, ThisError)]
pub enum Error {
    Logic,
    SeriesWriter(#[from] serieswriter::writer::Error),
    MissingChannelName,
    SendError,
    Decode,
    FramedBytes(#[from] streams::framed_bytes::Error),
}

struct BodyRead {}

pub async fn post_v01(
    (Query(params), body): (Query<HashMap<String, String>>, axum::body::Body),
    rres: Arc<RoutesResources>,
) -> Json<serde_json::Value> {
    match post_v01_try(params, body, rres).await {
        Ok(k) => k,
        Err(e) => Json(serde_json::Value::String(e.to_string())),
    }
}

async fn post_v01_try(
    params: HashMap<String, String>,
    body: axum::body::Body,
    rres: Arc<RoutesResources>,
) -> Result<Json<serde_json::Value>, Error> {
    info!("params {:?}", params);
    let stnow = SystemTime::now();
    let worker_tx = rres.worker_tx.clone();
    let backend = rres.backend.clone();
    let channel = params.get("channelName").ok_or(Error::MissingChannelName)?.into();
    let scalar_type = ScalarType::I16;
    let shape = Shape::Scalar;
    info!("establishing...");
    let mut writer = SeriesWriter::establish(worker_tx, backend, channel, scalar_type, shape, stnow).await?;

    let mut iqdqs = InsertDeques::new();
    let mut iqtx = rres.iqtx.clone();
    // iqtx.send_all(&mut iqdqs).await.map_err(|_| Error::SendError)?;
    // let deque = &mut iqdqs.st_rf3_rx;

    let mut frames = FramedBytesStream::new(body.into_data_stream().map_err(|_| streams::framed_bytes::Error::Logic));
    while let Some(frame) = frames.try_next().await? {
        info!("got frame len {}", frame.len());
        let evs: EventsDim0<i16> = ciborium::de::from_reader(Cursor::new(frame)).map_err(|_| Error::Decode)?;
        info!("see events {:?}", evs);
        let deque = &mut iqdqs.st_rf3_rx;
        for (i, (&ts, &val)) in evs.tss.iter().zip(evs.values.iter()).enumerate() {
            info!("ev  {:6}  {:20}  {:20}", i, ts, val);
            let val = DataValue::Scalar(ScalarValue::I16(val));
            writer.write(TsNano::from_ns(ts), TsNano::from_ns(ts), val, deque)?;
        }
        iqtx.send_all(&mut iqdqs).await.map_err(|_| Error::SendError)?;
    }

    let deque = &mut iqdqs.st_rf3_rx;
    finish_writers(vec![&mut writer], deque)?;
    iqtx.send_all(&mut iqdqs).await.map_err(|_| Error::SendError)?;

    let ret = Json(serde_json::json!({
        "result": true,
    }));
    Ok(ret)
}

fn tick_writers(sws: Vec<&mut SeriesWriter>, deque: &mut VecDeque<QueryItem>) -> Result<(), Error> {
    for sw in sws {
        sw.tick(deque)?;
    }
    Ok(())
}

fn finish_writers(sws: Vec<&mut SeriesWriter>, deque: &mut VecDeque<QueryItem>) -> Result<(), Error> {
    for sw in sws {
        sw.tick(deque)?;
    }
    Ok(())
}
