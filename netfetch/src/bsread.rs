use crate::zmtp::ZmtpMessage;
use err::Error;
#[allow(unused)]
use log::*;
use netpod::{AggKind, ByteOrder, ScalarType, Shape};
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
    #[serde(rename = "type", default = "bsread_type_default")]
    pub ty: String,
    #[serde(default = "bsread_shape_default")]
    pub shape: JsVal,
    #[serde(default = "bsread_encoding_default")]
    pub encoding: String,
    pub compression: Option<String>,
}

fn bsread_type_default() -> String {
    "float64".into()
}

fn bsread_shape_default() -> JsVal {
    JsVal::Array(vec![])
}

fn bsread_encoding_default() -> String {
    "little".into()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CompressionKind {
    Lz4,
    BitshuffleLz4,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChannelDescDecoded {
    pub name: String,
    pub scalar_type: ScalarType,
    pub shape: Shape,
    pub byte_order: ByteOrder,
    pub compression: Option<CompressionKind>,
    pub agg_kind: AggKind,
}

impl TryFrom<&ChannelDesc> for ChannelDescDecoded {
    type Error = Error;

    fn try_from(cd: &ChannelDesc) -> Result<Self, Self::Error> {
        let ret = ChannelDescDecoded {
            name: cd.name.clone(),
            scalar_type: ScalarType::from_bsread_str(&cd.ty)?,
            shape: Shape::from_bsread_jsval(&cd.shape)?,
            compression: match &cd.compression {
                None => None,
                Some(k) => match k.as_str() {
                    "none" => None,
                    "lz4" => Some(CompressionKind::Lz4),
                    "bitshuffle_lz4" => Some(CompressionKind::BitshuffleLz4),
                    _ => {
                        return Err(Error::with_msg_no_trace(format!(
                            "can not understand bsread compression kind: {k:?}"
                        )))
                    }
                },
            },
            byte_order: match cd.encoding.as_str() {
                "little" => ByteOrder::LE,
                "big" => ByteOrder::BE,
                _ => ByteOrder::LE,
            },
            agg_kind: AggKind::Plain,
        };
        Ok(ret)
    }
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

pub struct Parser {
    tmp1: Vec<u8>,
}

impl Parser {
    pub fn new() -> Self {
        Self { tmp1: vec![0; 1024] }
    }

    pub fn parse_zmtp_message(&mut self, msg: &ZmtpMessage) -> Result<BsreadMessage, Error> {
        if msg.frames().len() < 2 {
            return Err(Error::with_msg_no_trace("not enough frames for bsread"));
        }
        let head_a: HeadA =
            serde_json::from_slice(&msg.frames()[0].data()).map_err(|e| format!("main header parse error {e:?}"))?;
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
                    let inp = msg.frames()[1].data();
                    let nd = u32::from_be_bytes(inp[0..4].try_into()?) as usize;
                    loop {
                        let g = self.tmp1.len();
                        if g >= nd {
                            break;
                        }
                        if g > 1024 * 512 {
                            return Err(Error::with_public_msg("decomp buffer too large"));
                        }
                        let u = self.tmp1.len() * 2;
                        info!("resize buffer to {u}");
                        self.tmp1.resize(u, 0);
                    }
                    match bitshuffle::lz4_decompress(&inp[4..], &mut self.tmp1) {
                        Ok(_) => {}
                        Err(e) => {
                            // TODO throttle log output
                            error!("lz4 error {e:?}");
                            return Err(Error::with_public_msg(format!("lz4 error {e:?}")));
                        }
                    }
                    &self.tmp1[..nd]
                }
                "bitshuffle_lz4" => {
                    let inp = msg.frames()[1].data();
                    let nd = u64::from_be_bytes(inp[0..8].try_into()?) as usize;
                    let bs = u32::from_be_bytes(inp[8..12].try_into()?) as usize;
                    loop {
                        let g = self.tmp1.len();
                        if g >= nd {
                            break;
                        }
                        if g > 1024 * 512 {
                            return Err(Error::with_public_msg("decomp buffer too large"));
                        }
                        let u = self.tmp1.len() * 2;
                        info!("resize buffer to {u}");
                        self.tmp1.resize(u, 0);
                    }
                    match bitshuffle::bitshuffle_decompress(&inp[12..], &mut self.tmp1, nd, 1, bs) {
                        Ok(_) => {}
                        Err(e) => {
                            // TODO throttle log output
                            error!("bitshuffle_lz4 error {e:?}");
                            return Err(Error::with_public_msg(format!("bitshuffle_lz4 error {e:?}")));
                        }
                    }
                    &self.tmp1[..nd]
                }
                _ => msg.frames()[1].data(),
            },
            None => msg.frames()[1].data(),
        };
        let head_b: HeadB = serde_json::from_slice(dhdecompr).map_err(|e| format!("data header parse error: {e:?}"))?;
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
}

pub struct BsreadCollector {}

impl BsreadCollector {
    pub fn new<S: Into<String>>(_addr: S) -> Self {
        err::todoval()
    }
}
