use crate::conf::ChannelConfig;
use netpod::ScalarType;
use netpod::Shape;
use serde::Serialize;
use series::ChannelStatusSeriesId;
use series::SeriesId;
use std::net::SocketAddrV4;
use std::time::Instant;
use std::time::SystemTime;

#[derive(Clone, Debug, Serialize)]
pub enum ChannelConnectedInfo {
    Disconnected,
    Connecting,
    Connected,
    Error,
}

#[derive(Clone, Debug, Serialize)]
pub struct ChannelStateInfo {
    pub stnow: SystemTime,
    pub cssid: ChannelStatusSeriesId,
    pub addr: SocketAddrV4,
    pub series: Option<SeriesId>,
    pub channel_connected_info: ChannelConnectedInfo,
    pub ping_last: Option<SystemTime>,
    pub pong_last: Option<SystemTime>,
    pub scalar_type: Option<ScalarType>,
    pub shape: Option<Shape>,
    // NOTE: this solution can yield to the same Instant serialize to different string representations.
    // #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "ser_instant")]
    pub ts_created: Option<Instant>,
    // #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "ser_instant")]
    pub ts_event_last: Option<Instant>,
    pub recv_count: Option<u64>,
    pub recv_bytes: Option<u64>,
    // #[serde(skip_serializing_if = "Option::is_none")]
    pub item_recv_ivl_ema: Option<f32>,
    pub interest_score: f32,
    pub conf: ChannelConfig,
    pub recv_last: SystemTime,
    pub write_st_last: SystemTime,
    pub write_mt_last: SystemTime,
    pub write_lt_last: SystemTime,
    pub status_emit_count: u64,
}

mod ser_instant {
    use super::*;
    use netpod::DATETIME_FMT_3MS;
    use serde::Deserializer;
    use serde::Serializer;

    pub fn serialize<S>(val: &Option<Instant>, ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match val {
            Some(val) => {
                let now = chrono::Utc::now();
                let tsnow = Instant::now();
                let t1 = if tsnow >= *val {
                    let dur = tsnow.duration_since(*val);
                    let dur2 = chrono::Duration::try_seconds(dur.as_secs() as i64)
                        .unwrap()
                        .checked_add(&chrono::Duration::microseconds(dur.subsec_micros() as i64))
                        .unwrap();
                    now.checked_sub_signed(dur2).unwrap()
                } else {
                    let dur = (*val).duration_since(tsnow);
                    let dur2 = chrono::Duration::try_seconds(dur.as_secs() as i64)
                        .unwrap()
                        .checked_sub(&chrono::Duration::microseconds(dur.subsec_micros() as i64))
                        .unwrap();
                    now.checked_add_signed(dur2).unwrap()
                };
                let s = t1.format(DATETIME_FMT_3MS).to_string();
                ser.serialize_str(&s)
            }
            None => ser.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(_de: D) -> Result<Option<Instant>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let e = serde::de::Error::custom("todo deserialize for ser_instant");
        Err(e)
    }
}
