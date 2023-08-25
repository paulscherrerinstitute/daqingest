use crate::batchquery::series_by_channel::ChannelInfoQuery;
use crate::bsread::BsreadMessage;
use crate::bsread::ChannelDescDecoded;
use crate::bsread::HeadB;
use crate::bsread::Parser;
use crate::ca::proto::CaDataArrayValue;
use crate::ca::proto::CaDataValue;
use crate::ca::IngestCommons;
use crate::series::SeriesId;
use crate::zmtp::zmtpproto;
use crate::zmtp::zmtpproto::SocketType;
use crate::zmtp::zmtpproto::Zmtp;
use crate::zmtp::zmtpproto::ZmtpFrame;
use crate::zmtp::zmtpproto::ZmtpMessage;
use crate::zmtp::ZmtpClientOpts;
use crate::zmtp::ZmtpEvent;
use async_channel::Sender;
use err::thiserror;
use err::ThisError;
use futures_util::StreamExt;
use netpod::log::*;
use netpod::timeunits::HOUR;
use netpod::timeunits::SEC;
use netpod::ScalarType;
use netpod::Shape;
use netpod::TS_MSP_GRID_SPACING;
use netpod::TS_MSP_GRID_UNIT;
use scywr::iteminsertqueue::ArrayValue;
use scywr::iteminsertqueue::CommonInsertItemQueueSender;
use scywr::iteminsertqueue::DataValue;
use scywr::iteminsertqueue::InsertItem;
use scywr::iteminsertqueue::QueryItem;
use scywr::session::ScySession;
use stats::CheckEvery;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use taskrun::tokio;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("InsertQueueSenderMissing")]
    InsertQueueSenderMissing,
    #[error("AsyncChannelSend")]
    AsyncChannelSend,
    #[error("IO({0})")]
    IO(#[from] io::Error),
    #[error("Msg({0})")]
    Msg(String),
    #[error("ZmtpProto({0})")]
    ZmtpProto(#[from] zmtpproto::Error),
    #[error("BadSlice")]
    BadSlice,
}

impl<T> From<async_channel::SendError<T>> for Error {
    fn from(value: async_channel::SendError<T>) -> Self {
        Self::AsyncChannelSend
    }
}

impl From<err::Error> for Error {
    fn from(value: err::Error) -> Self {
        Self::Msg(value.to_string())
    }
}

pub struct BsreadClient {
    opts: ZmtpClientOpts,
    source_addr: SocketAddr,
    do_pulse_id: bool,
    rcvbuf: Option<usize>,
    print_stats: CheckEvery,
    parser: Parser,
    ingest_commons: Arc<IngestCommons>,
    insqtx: CommonInsertItemQueueSender,
    tmp_evtset_series: Option<SeriesId>,
    channel_info_query_tx: Sender<ChannelInfoQuery>,
    inserted_in_ts_msp_count: u32,
    ts_msp_last: u64,
    ts_msp_grid_last: u32,
}

impl BsreadClient {
    pub async fn new(
        opts: ZmtpClientOpts,
        ingest_commons: Arc<IngestCommons>,
        channel_info_query_tx: Sender<ChannelInfoQuery>,
    ) -> Result<Self, Error> {
        let insqtx = ingest_commons
            .insert_item_queue
            .sender()
            .ok_or_else(|| Error::InsertQueueSenderMissing)?;
        let ret = Self {
            source_addr: opts.addr,
            do_pulse_id: opts.do_pulse_id,
            rcvbuf: opts.rcvbuf,
            opts,
            print_stats: CheckEvery::new(Duration::from_millis(2000)),
            parser: Parser::new(),
            ingest_commons,
            insqtx,
            tmp_evtset_series: None,
            channel_info_query_tx,
            inserted_in_ts_msp_count: 0,
            ts_msp_last: 0,
            ts_msp_grid_last: 0,
        };
        Ok(ret)
    }

    async fn test_evtset_extract(
        &mut self,
        msg: &ZmtpMessage,
        bm: &BsreadMessage,
        ts: u64,
        pulse: u64,
    ) -> Result<(), Error> {
        let chname = "SAR-CVME-TIFALL5:EvtSet";
        // Test the bool set write
        let mut i3 = usize::MAX;
        for (i, ch) in bm.head_b.channels.iter().enumerate() {
            if ch.name == chname {
                i3 = i;
                break;
            }
        }
        if i3 != usize::MAX {
            if let Some(fr) = msg.frames.get(2 + 2 * i3) {
                debug!("try to extract bools  {}  {}", fr.msglen, fr.data.len());
                let setlen = fr.data.len();
                debug!("flags {:?}", &fr.data[..setlen.min(16)]);
                let evtset: Vec<_> = fr.data.iter().map(|&x| x != 0).collect();
                let scalar_type = ScalarType::BOOL;
                let shape = Shape::Wave(256);
                if self.tmp_evtset_series.is_none() {
                    debug!("try to fetch series id");
                    let (tx, rx) = async_channel::bounded(8);
                    let item = ChannelInfoQuery {
                        backend: self.opts.backend.clone(),
                        channel: chname.into(),
                        scalar_type: ScalarType::BOOL.to_scylla_i32(),
                        shape_dims: Shape::Wave(setlen as _).to_scylla_vec(),
                        tx,
                    };
                    self.channel_info_query_tx.send(item).await?;
                    match rx.recv().await {
                        Ok(res) => match res {
                            Ok(res) => {
                                debug!("got series id: {res:?}");
                                self.tmp_evtset_series = Some(res.into_inner());
                            }
                            Err(e) => {
                                error!("{e}");
                            }
                        },
                        Err(e) => {
                            error!("{e}");
                        }
                    }
                }
                if let Some(series) = self.tmp_evtset_series.clone() {
                    let (ts_msp, ts_msp_changed) =
                        if self.inserted_in_ts_msp_count >= 6400 || self.ts_msp_last + HOUR <= ts {
                            let div = SEC * 10;
                            let ts_msp = ts / div * div;
                            if ts_msp == self.ts_msp_last {
                                (ts_msp, false)
                            } else {
                                self.ts_msp_last = ts_msp;
                                self.inserted_in_ts_msp_count = 1;
                                (ts_msp, true)
                            }
                        } else {
                            self.inserted_in_ts_msp_count += 1;
                            (self.ts_msp_last, false)
                        };
                    let ts_lsp = ts - ts_msp;
                    let ts_msp_grid = (ts / TS_MSP_GRID_UNIT / TS_MSP_GRID_SPACING * TS_MSP_GRID_SPACING) as u32;
                    let ts_msp_grid = if self.ts_msp_grid_last != ts_msp_grid {
                        self.ts_msp_grid_last = ts_msp_grid;
                        Some(ts_msp_grid)
                    } else {
                        None
                    };
                    let item = InsertItem {
                        series: series.into(),
                        ts_msp,
                        ts_lsp,
                        msp_bump: ts_msp_changed,
                        ts_msp_grid,
                        pulse,
                        scalar_type,
                        shape,
                        val: DataValue::Array(ArrayValue::Bool(evtset)),
                    };
                    let item = QueryItem::Insert(item);
                    match self.insqtx.send(item).await {
                        Ok(_) => {
                            debug!("item send ok  pulse {}", pulse);
                        }
                        Err(e) => {
                            error!("can not send item {:?}", e.0);
                        }
                    }
                } else {
                    error!("still no series id");
                    tokio::time::sleep(Duration::from_millis(1000)).await;
                }
            }
        }
        Ok(())
    }

    pub async fn run(&mut self) -> Result<(), Error> {
        let mut conn = tokio::net::TcpStream::connect(&self.source_addr).await?;
        if let Some(v) = self.rcvbuf {
            crate::linuxhelper::set_rcv_sock_opts(&mut conn, v as u32)?;
        }
        let mut zmtp = Zmtp::new(conn, SocketType::PULL);
        let mut i1 = 0u64;
        let mut msgc = 0u64;
        let mut dh_md5_last = String::new();
        let mut frame_diff_count = 0u64;
        let mut hash_mismatch_count = 0u64;
        let mut head_b = HeadB::empty();
        let mut bytes_payload = 0u64;
        let mut rows_inserted = 0u32;
        let mut time_spent_inserting = Duration::from_millis(0);
        let mut msg_dt_ema = stats::EMA::with_k(0.01);
        let mut msg_ts_last = Instant::now();
        while let Some(item) = zmtp.next().await {
            let tsnow = Instant::now();
            match item {
                Ok(ev) => match ev {
                    ZmtpEvent::ZmtpCommand(_) => (),
                    ZmtpEvent::ZmtpMessage(msg) => {
                        msgc += 1;
                        {
                            let dt = tsnow.duration_since(msg_ts_last);
                            msg_dt_ema.update(dt.as_secs_f32());
                            msg_ts_last = tsnow;
                        }
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
                                {
                                    if bm.head_b_md5 != dh_md5_last {
                                        // TODO header changed, don't support this at the moment.
                                        head_b = bm.head_b.clone();
                                        if dh_md5_last.is_empty() {
                                            debug!("data header hash {}", bm.head_b_md5);
                                            dh_md5_last = bm.head_b_md5.clone();
                                            // TODO must fetch series ids on-demand.
                                            // For the time being, assume that channel list never changes, but WARN!
                                            /*let scy = self.scy.clone();
                                            for chn in &head_b.channels {
                                                info!("Setup writer for {}", chn.name);
                                                let cd: ChannelDescDecoded = chn.try_into()?;
                                                match self.setup_channel_writers(&scy, &cd).await {
                                                    Ok(_) => {}
                                                    Err(e) => {
                                                        warn!("can not set up writer for {}  {e:?}", chn.name);
                                                    }
                                                }
                                            }*/
                                        } else {
                                            error!("TODO  changed data header hash {}", bm.head_b_md5);
                                            dh_md5_last = bm.head_b_md5.clone();
                                            // TODO
                                            // Update only the changed channel writers.
                                            // Flush buffers before creating new channel writer.
                                        }
                                    }
                                }
                                if self.do_pulse_id {
                                    let nframes = msg.frames().len();
                                    debug!("nframes {nframes}");
                                    let mut i3 = u32::MAX;
                                    for (i, ch) in head_b.channels.iter().enumerate() {
                                        if ch.name == "SINEG01-RLLE-STA:MASTER-EVRPULSEID"
                                            || ch.name == "SAR-CVME-TIFALL4:EvtSet"
                                        {
                                            i3 = i as u32;
                                        }
                                    }
                                    // TODO need to know the facility!
                                    if i3 < u32::MAX {
                                        let i4 = 2 * i3 + 2;
                                        if let Some(fr) = msg.frames.get(i4 as usize) {
                                            self.insert_pulse_map(fr, &msg, &bm).await?;
                                        }
                                    }
                                }
                                if msg.frames.len() < 2 + 2 * head_b.channels.len() {
                                    // TODO count always, throttle log.
                                    error!("not enough frames for data header");
                                }
                                let gts = &bm.head_a.global_timestamp;
                                let ts = (gts.sec as u64) * SEC + gts.ns as u64;
                                let pulse = bm.head_a.pulse_id.as_u64().unwrap_or(0);
                                debug!("ts {ts:20}  pulse{pulse:20}");
                                // TODO limit warn rate
                                if pulse != 0 && (pulse < 14781000000 || pulse > 49000000000) {
                                    // TODO limit log rate
                                    warn!("pulse out of range {}  addr {}", pulse, self.source_addr);
                                }
                                if pulse % 1000000 != ts % 1000000 {
                                    warn!(
                                        "pulse-ts mismatch  ts {}  pulse {}  addr {}",
                                        ts, pulse, self.source_addr
                                    );
                                }
                                self.test_evtset_extract(&msg, &bm, ts, pulse).await?;
                                let nch = head_b.channels.len();
                                let nmax = self.opts.process_channel_count_limit.unwrap_or(4000);
                                let nlim = if nch > nmax {
                                    // TODO count this event
                                    4000
                                } else {
                                    nch
                                };
                                for i1 in 0..nlim {
                                    // TODO skip decoding if header unchanged.
                                    let chn = &head_b.channels[i1];
                                    let chd: ChannelDescDecoded = chn.try_into()?;
                                    let fr = &msg.frames[2 + 2 * i1];
                                    // TODO store the channel information together with series in struct.
                                }
                            }
                            Err(e) => {
                                error!("{}", e);
                                for frame in &msg.frames {
                                    info!("Frame: {:?}", frame);
                                }
                                zmtp.dump_input_state();
                                zmtp.dump_conn_state();
                            }
                        }
                    }
                },
                Err(e) => {
                    error!("{}", e);
                    return Err(e)?;
                }
            }
            i1 += 1;
            if false && i1 > 10000 {
                break;
            }
            if false && msgc > 10000 {
                break;
            }
            let dt = self.print_stats.is_elapsed_now();
            if dt > 0. {
                let nrs = rows_inserted as f32 / dt;
                let dt_ins = time_spent_inserting.as_secs_f32() * 1e3;
                let r = bytes_payload as f32 / dt * 1e-3;
                info!("insert {nrs:.0} 1/s  dt-ins {dt_ins:4.0} ms  payload {r:8.3} kB/s");
                rows_inserted = 0;
                time_spent_inserting = Duration::from_millis(0);
                bytes_payload = 0;
                if msg_dt_ema.update_count() > 100 {
                    let ema = msg_dt_ema.ema();
                    if ema < 0.005 {
                        let emv = msg_dt_ema.emv().sqrt();
                        warn!("MSG FREQ  {}  {:9.5}  {:9.5}", self.source_addr, ema, emv);
                    }
                }
            }
        }
        Ok(())
    }

    async fn setup_channel_writers(&mut self, scy: &ScySession, cd: &ChannelDescDecoded) -> Result<(), Error> {
        let has_comp = cd.compression.is_some();
        if has_comp {
            warn!("Compression not yet supported  [{}]", cd.name);
            return Ok(());
        }
        let shape_dims = cd.shape.to_scylla_vec();
        Ok(())
    }

    async fn insert_pulse_map(&mut self, fr: &ZmtpFrame, msg: &ZmtpMessage, bm: &BsreadMessage) -> Result<(), Error> {
        debug!("data len {}", fr.data.len());
        // TODO take pulse-id also from main header and compare.
        let pulse_f64 = f64::from_be_bytes(fr.data[..8].try_into().map_err(|_| Error::BadSlice)?);
        debug!("pulse_f64 {pulse_f64}");
        let pulse = pulse_f64 as u64;
        if false {
            let i4 = 3;
            // TODO this next frame should be described somehow in the json header or?
            debug!("next val len {}", msg.frames[i4 as usize + 1].data.len());
            let ts_a = u64::from_be_bytes(
                msg.frames[i4 as usize + 1].data[0..8]
                    .try_into()
                    .map_err(|_| Error::BadSlice)?,
            );
            let ts_b = u64::from_be_bytes(
                msg.frames[i4 as usize + 1].data[8..16]
                    .try_into()
                    .map_err(|_| Error::BadSlice)?,
            );
            debug!("ts_a {ts_a}  ts_b {ts_b}");
        }
        let ts = bm.head_a.global_timestamp.sec * SEC + bm.head_a.global_timestamp.ns;
        /*let pulse_a = (pulse >> 14) as i64;
        let pulse_b = (pulse & 0x3fff) as i32;
        let ts_a = bm.head_a.global_timestamp.sec as i64;
        let ts_b = bm.head_a.global_timestamp.ns as i32;*/
        debug!("ts {ts:20}  pulse {pulse:20}");
        Ok(())
    }
}
