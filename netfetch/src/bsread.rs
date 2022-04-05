use crate::zmtp::ZmtpMessage;
use err::Error;
#[allow(unused)]
use log::*;
use netpod::{ByteOrder, ScalarType, Shape};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsVal;

// TODO
pub struct ParseError {
    pub err: Error,
    pub msg: ZmtpMessage,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GlobalTimestamp {
    pub sec: u64,
    pub ns: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChannelDesc {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
    pub shape: JsVal,
    pub encoding: String,
    pub compression: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HeadA {
    pub htype: String,
    pub hash: String,
    pub pulse_id: serde_json::Number,
    pub global_timestamp: GlobalTimestamp,
    pub dh_compression: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HeadB {
    pub htype: String,
    pub channels: Vec<ChannelDesc>,
}

impl HeadB {
    pub fn empty() -> Self {
        Self {
            htype: String::new(),
            channels: vec![],
        }
    }
}

#[derive(Clone, Debug)]
pub struct BsreadMessage {
    pub head_a: HeadA,
    pub head_b: HeadB,
    pub head_b_md5: String,
}

pub fn parse_zmtp_message(msg: &ZmtpMessage) -> Result<BsreadMessage, Error> {
    if msg.frames().len() < 2 {
        return Err(Error::with_msg_no_trace("not enough frames for bsread"));
    }
    let head_a: HeadA = serde_json::from_slice(&msg.frames()[0].data())?;
    let head_b_md5 = {
        use md5::Digest;
        let mut hasher = md5::Md5::new();
        hasher.update(msg.frames()[1].data());
        let h = hasher.finalize();
        hex::encode(&h)
    };
    let dhdecompr = match &head_a.dh_compression {
        Some(m) => match m.as_str() {
            "none" => msg.frames()[1].data(),
            "lz4" => {
                error!("data header lz4 compression not yet implemented");
                return Err(Error::with_msg_no_trace(
                    "data header lz4 compression not yet implemented",
                ));
            }
            "bitshuffle_lz4" => {
                error!("data header bitshuffle_lz4 compression not yet implemented");
                return Err(Error::with_msg_no_trace(
                    "data header bitshuffle_lz4 compression not yet implemented",
                ));
            }
            _ => msg.frames()[1].data(),
        },
        None => msg.frames()[1].data(),
    };
    let head_b: HeadB = serde_json::from_slice(dhdecompr)?;
    if false && msg.frames().len() == head_b.channels.len() + 3 {
        for (ch, fr) in head_b.channels.iter().zip(msg.frames()[2..].iter()) {
            let sty = ScalarType::from_bsread_str(ch.ty.as_str())?;
            let bo = ByteOrder::from_bsread_str(&ch.encoding)?;
            let shape = Shape::from_bsread_jsval(&ch.shape)?;
            match sty {
                ScalarType::I64 => match &bo {
                    ByteOrder::LE => match &shape {
                        Shape::Scalar => {
                            assert_eq!(fr.data().len(), 8);
                            let _v = i64::from_le_bytes(fr.data().try_into()?);
                        }
                        Shape::Wave(_) => {}
                        Shape::Image(_, _) => {}
                    },
                    _ => {}
                },
                _ => {}
            }
        }
    }
    let ret = BsreadMessage {
        head_a,
        head_b,
        head_b_md5,
    };
    Ok(ret)
}

pub struct BsreadCollector {}

impl BsreadCollector {
    pub fn new<S: Into<String>>(_addr: S) -> Self {
        err::todoval()
    }
}
