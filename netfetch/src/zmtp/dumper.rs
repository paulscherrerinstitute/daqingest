use crate::bsread::ChannelDescDecoded;
use crate::bsread::HeadB;
use crate::bsread::Parser;
use crate::zmtp::zmtpproto;
use crate::zmtp::zmtpproto::SocketType;
use crate::zmtp::zmtpproto::Zmtp;
use crate::zmtp::ZmtpEvent;
use err::thiserror;
use futures_util::StreamExt;
use netpod::log::*;
use netpod::timeunits::SEC;
use std::io;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("IO({0})")]
    IO(#[from] io::Error),
    #[error("Msg({0})")]
    Msg(String),
    #[error("ZmtpProto({0})")]
    ZmtpProto(#[from] zmtpproto::Error),
}

impl From<err::Error> for Error {
    fn from(value: err::Error) -> Self {
        Self::Msg(value.to_string())
    }
}

pub struct BsreadDumper {
    source_addr: String,
    parser: Parser,
}

impl BsreadDumper {
    pub fn new(source_addr: String) -> Self {
        Self {
            source_addr,
            parser: Parser::new(),
        }
    }

    pub async fn run(&mut self) -> Result<(), Error> {
        let src = if self.source_addr.starts_with("tcp://") {
            self.source_addr[6..].into()
        } else {
            self.source_addr.clone()
        };
        let conn = tokio::net::TcpStream::connect(&src).await?;
        let mut zmtp = Zmtp::new(conn, SocketType::PULL);
        let mut i1 = 0u64;
        let mut msgc = 0u64;
        let mut dh_md5_last = String::new();
        let mut frame_diff_count = 0u64;
        let mut hash_mismatch_count = 0u64;
        let mut head_b = HeadB::empty();
        while let Some(item) = zmtp.next().await {
            match item {
                Ok(ev) => match ev {
                    ZmtpEvent::ZmtpCommand(_) => (),
                    ZmtpEvent::ZmtpMessage(msg) => {
                        msgc += 1;
                        match self.parser.parse_zmtp_message(&msg) {
                            Ok(bm) => {
                                if msg.frames().len() - 2 * bm.head_b.channels.len() != 2 {
                                    frame_diff_count += 1;
                                    if frame_diff_count < 1000 {
                                        warn!(
                                            "chn len {}  frame diff {}",
                                            bm.head_b.channels.len(),
                                            msg.frames().len() - 2 * bm.head_b.channels.len()
                                        );
                                    }
                                }
                                if bm.head_b_md5 != bm.head_a.hash {
                                    hash_mismatch_count += 1;
                                    // TODO keep logging data header changes, just suppress too frequent messages.
                                    if hash_mismatch_count < 200 {
                                        error!(
                                            "Invalid bsread message: hash mismatch.  dhcompr {:?}",
                                            bm.head_a.dh_compression
                                        );
                                    }
                                }
                                if bm.head_b_md5 != dh_md5_last {
                                    head_b = bm.head_b.clone();
                                    if dh_md5_last.is_empty() {
                                        info!("data header hash {}", bm.head_b_md5);
                                    } else {
                                        error!("changed data header hash {}  mh {}", bm.head_b_md5, bm.head_a.hash);
                                    }
                                    dh_md5_last = bm.head_b_md5.clone();
                                }
                                if msg.frames.len() < 2 + 2 * head_b.channels.len() {
                                    // TODO count always, throttle log.
                                    error!("not enough frames for data header");
                                }
                                let gts = bm.head_a.global_timestamp;
                                let ts = (gts.sec as u64) * SEC + gts.ns as u64;
                                let pulse = bm.head_a.pulse_id.as_u64().unwrap_or(0);
                                let mut bytes_payload = 0u64;
                                for i1 in 0..head_b.channels.len() {
                                    let chn = &head_b.channels[i1];
                                    let _cd: ChannelDescDecoded = chn.try_into()?;
                                    let fr = &msg.frames[2 + 2 * i1];
                                    bytes_payload += fr.data().len() as u64;
                                }
                                info!("zmtp message  ts {ts}  pulse {pulse}  bytes_payload {bytes_payload}");
                            }
                            Err(e) => {
                                for frame in &msg.frames {
                                    info!("Frame: {:?}", frame);
                                }
                                zmtp.dump_input_state();
                                zmtp.dump_conn_state();
                                error!("bsread parse error: {e:?}");
                                break;
                            }
                        }
                    }
                },
                Err(e) => {
                    error!("zmtp item error: {e:?}");
                    return Err(e.into());
                }
            }
            i1 += 1;
            if true && i1 > 20 {
                break;
            }
            if true && msgc > 20 {
                break;
            }
        }
        Ok(())
    }
}
