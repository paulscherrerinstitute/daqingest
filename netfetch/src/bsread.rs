use crate::zmtp::ZmtpMessage;
use err::Error;
#[allow(unused)]
use log::*;
use netpod::{ByteOrder, ScalarType, Shape};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsVal;
use std::fmt;

// TODO
pub struct ParseError {
    pub err: Error,
    pub msg: ZmtpMessage,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GlobalTimestamp {
    pub sec: u64,
    pub ns: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChannelDesc {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
    pub shape: JsVal,
    pub encoding: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HeadA {
    pub htype: String,
    pub hash: String,
    pub pulse_id: serde_json::Number,
    pub global_timestamp: GlobalTimestamp,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HeadB {
    pub htype: String,
    pub channels: Vec<ChannelDesc>,
}

#[derive(Debug)]
pub struct BsreadMessage {
    pub head_a: HeadA,
    pub head_b: HeadB,
    pub values: Vec<Box<dyn fmt::Debug>>,
}

pub fn parse_zmtp_message(msg: &ZmtpMessage) -> Result<BsreadMessage, Error> {
    if msg.frames().len() < 3 {
        return Err(Error::with_msg_no_trace("not enough frames for bsread"));
    }
    let head_a: HeadA = serde_json::from_slice(&msg.frames()[0].data())?;
    let head_b: HeadB = serde_json::from_slice(&msg.frames()[1].data())?;
    let mut values = vec![];
    if msg.frames().len() == head_b.channels.len() + 3 {
        for (ch, fr) in head_b.channels.iter().zip(msg.frames()[2..].iter()) {
            let sty = ScalarType::from_bsread_str(ch.ty.as_str())?;
            let bo = ByteOrder::from_bsread_str(&ch.encoding)?;
            let shape = Shape::from_bsread_jsval(&ch.shape)?;
            match sty {
                ScalarType::I64 => match &bo {
                    ByteOrder::LE => match &shape {
                        Shape::Scalar => {
                            assert_eq!(fr.data().len(), 8);
                            let v = i64::from_le_bytes(fr.data().try_into()?);
                            values.push(Box::new(v) as _);
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
    {
        let fr = &msg.frames()[msg.frames().len() - 1];
        if fr.data().len() == 8 {
            let pulse = u64::from_le_bytes(fr.data().try_into()?);
            info!("pulse {}", pulse);
        }
    }
    let ret = BsreadMessage { head_a, head_b, values };
    Ok(ret)
}

pub struct BsreadCollector {}

impl BsreadCollector {
    pub fn new<S: Into<String>>(_addr: S) -> Self {
        err::todoval()
    }
}
