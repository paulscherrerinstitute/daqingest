mod connecting;

use super::conncmd::ConnCommand;
use super::connevent::CaConnEvent;
use super::connevent::EndOfStreamReason;
use crate::ca::conn::CaConnOpts;
use crate::ca::conn2::progpend::HaveProgressPending;
use async_channel::Sender;
use ca_proto::ca::proto;
use connecting::Connecting;
use dbpg::seriesbychannel::ChannelInfoQuery;
use futures_util::Future;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use hashbrown::HashMap;
use log::*;
use proto::CaProto;
use scywr::insertqueues::InsertDeques;
use scywr::insertqueues::InsertQueuesTx;
use scywr::iteminsertqueue::QueryItem;
use stats::rand_xoshiro::Xoshiro128PlusPlus;
use stats::CaConnStats;
use stats::CaProtoStats;
use std::collections::VecDeque;
use std::fmt;
use std::net::SocketAddrV4;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;
use taskrun::tokio;
use tokio::net::TcpStream;

autoerr::create_error_v1!(
    name(Error, "Conn"),
    enum variants {
        TickerPoll,
    },
);

struct DurationMeasureSteps {
    ts: Instant,
    durs: smallvec::SmallVec<[Duration; 8]>,
}

impl DurationMeasureSteps {
    fn new() -> Self {
        Self {
            ts: Instant::now(),
            durs: smallvec::SmallVec::new(),
        }
    }

    fn step(&mut self) {
        let ts = Instant::now();
        let d = ts.saturating_duration_since(self.ts);
        self.durs.push(d);
        self.ts = ts;
    }
}

enum ConnectedState {
    Init(CaProto),
    Handshake(CaProto),
    PeerReady(CaProto),
}

#[derive(Debug)]
enum CaConnState {
    Connecting(Connecting),
    Connected(CaProto),
    Shutdown(EndOfStreamReason),
    Done,
}

struct CaConn {
    opts: CaConnOpts,
    backend: String,
    state: CaConnState,
    iqdqs: InsertDeques,
    ca_conn_event_out_queue: VecDeque<CaConnEvent>,
    ca_conn_event_out_queue_max: usize,
    rng: Xoshiro128PlusPlus,
    stats: Arc<CaConnStats>,
}

impl CaConn {
    fn new(
        opts: CaConnOpts,
        backend: String,
        remote_addr: SocketAddrV4,
        local_epics_hostname: String,
        iqtx: InsertQueuesTx,
        channel_info_query_tx: Sender<ChannelInfoQuery>,
        stats: Arc<CaConnStats>,
        ca_proto_stats: Arc<CaProtoStats>,
    ) -> Self {
        let tsnow = Instant::now();
        let (cq_tx, cq_rx) = async_channel::bounded::<ConnCommand>(32);
        let rng = stats::xoshiro_from_time();
        Self {
            opts,
            backend,
            state: CaConnState::Connecting(Connecting::dummy_new(remote_addr, tsnow)),
            iqdqs: InsertDeques::new(),
            ca_conn_event_out_queue: VecDeque::new(),
            ca_conn_event_out_queue_max: 2000,
            rng,
            stats,
        }
    }

    fn poll_own_ticker(mut self: Pin<&mut Self>, cx: &mut Context) -> Result<Poll<()>, Error> {
        // TODO nothing taken yet
        todo!()
    }

    // call this only from the main fn poll
    fn shutdown_on_error(&mut self, e: Error) {
        self.state = CaConnState::Shutdown(EndOfStreamReason::Error(e));
        todo!()
    }
}

macro_rules! handle_poll_res {
    ($res:expr, $hpp:expr) => {
        match $res {
            Ready(x) => match x {
                Ok(x) => match x {
                    Some(x) => {
                        $hpp.have_progress();
                    }
                    None => {}
                },
                Err(e) => {
                    // TODO how to handle error:
                    // Transition state, emit item.
                    error!("{}", e);
                }
            },
            Pending => {
                $hpp.have_pending();
            }
        }
    };
}

impl Stream for CaConn {
    type Item = CaConnEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let mut durs = DurationMeasureSteps::new();
        self.stats.poll_fn_begin().inc();
        let ret = loop {
            self.stats.poll_loop_begin().inc();
            let qlen = self.iqdqs.len();
            if qlen >= self.opts.insert_queue_max * 2 / 3 {
                self.stats.insert_item_queue_pressure().inc();
            } else if qlen >= self.opts.insert_queue_max {
                self.stats.insert_item_queue_full().inc();
            }
            let mut hppv = HaveProgressPending::new();
            let hpp = &mut hppv;
            if let CaConnState::Done = self.state {
                break Ready(None);
            } else if let Some(item) = self.ca_conn_event_out_queue.pop_front() {
                break Ready(Some(item));
            }

            // TODO add up duration of this scope
            match self.as_mut().poll_own_ticker(cx) {
                Ok(Ready(())) => {
                    hpp.have_progress();
                }
                Ok(Pending) => {
                    hpp.have_pending();
                }
                Err(e) => {
                    self.shutdown_on_error(e);
                    continue;
                }
            }

            {
                let n = self.iqdqs.len();
                self.stats.iiq_len().ingest(n as u32);
            }

            {
                // let stats2 = self.stats.clone();
                // let stats_fn = move |item: &VecDeque<QueryItem>| {
                //     stats2.iiq_batch_len().ingest(item.len() as u32);
                // };
                // flush_queue_dqs!(
                //     self,
                //     st_rf1_qu,
                //     st_rf1_sp_pin,
                //     send_batched::<256, _>,
                //     32,
                //     (&mut have_progress, &mut have_pending),
                //     "st_rf1_rx",
                //     cx,
                //     stats_fn
                // );

                // let stats2 = self.stats.clone();
                // let stats_fn = move |item: &VecDeque<QueryItem>| {
                //     stats2.iiq_batch_len().ingest(item.len() as u32);
                // };
                // flush_queue_dqs!(
                //     self,
                //     st_rf3_qu,
                //     st_rf3_sp_pin,
                //     send_batched::<256, _>,
                //     32,
                //     (&mut have_progress, &mut have_pending),
                //     "st_rf3_rx",
                //     cx,
                //     stats_fn
                // );

                // let stats2 = self.stats.clone();
                // let stats_fn = move |item: &VecDeque<QueryItem>| {
                //     stats2.iiq_batch_len().ingest(item.len() as u32);
                // };
                // flush_queue_dqs!(
                //     self,
                //     mt_rf3_qu,
                //     mt_rf3_sp_pin,
                //     send_batched::<256, _>,
                //     32,
                //     (&mut have_progress, &mut have_pending),
                //     "mt_rf3_rx",
                //     cx,
                //     stats_fn
                // );

                // let stats2 = self.stats.clone();
                // let stats_fn = move |item: &VecDeque<QueryItem>| {
                //     stats2.iiq_batch_len().ingest(item.len() as u32);
                // };
                // flush_queue_dqs!(
                //     self,
                //     lt_rf3_qu,
                //     lt_rf3_sp_pin,
                //     send_batched::<256, _>,
                //     32,
                //     (&mut have_progress, &mut have_pending),
                //     "lt_rf3_rx",
                //     cx,
                //     stats_fn
                // );
            }

            // if !self.is_shutdown() {
            //     flush_queue!(
            //         self,
            //         channel_info_query_qu,
            //         channel_info_query_tx,
            //         send_individual,
            //         32,
            //         (&mut have_progress, &mut have_pending),
            //         "chinf",
            //         cx,
            //         |_| {}
            //     );
            // }

            // match self.as_mut().handle_writer_establish_result(cx) {
            //     Ok(Ready(Some(()))) => {
            //         have_progress = true;
            //     }
            //     Ok(Ready(None)) => {}
            //     Ok(Pending) => {
            //         have_pending = true;
            //     }
            //     Err(e) => break Ready(Some(CaConnEvent::err_now(e))),
            // }

            // match self.as_mut().handle_conn_command(cx) {
            //     Ok(Ready(Some(()))) => {
            //         have_progress = true;
            //     }
            //     Ok(Ready(None)) => {}
            //     Ok(Pending) => {
            //         have_pending = true;
            //     }
            //     Err(e) => break Ready(Some(CaConnEvent::err_now(e))),
            // }

            // match self.loop_inner(cx) {
            //     Ok(Ready(Some(()))) => {
            //         have_progress = true;
            //     }
            //     Ok(Ready(None)) => {}
            //     Ok(Pending) => {
            //         have_pending = true;
            //     }
            //     Err(e) => {
            //         error!("{e}");
            //         self.state = CaConnState::EndOfStream;
            //         break Ready(Some(CaConnEvent::err_now(e)));
            //     }
            // }

            match &mut self.state {
                CaConnState::Connecting(st2) => handle_poll_res!(st2.poll_unpin(cx), hpp),
                CaConnState::Connected(_) => todo!(),
                CaConnState::Shutdown(_) => {
                    // TODO still attempt to flush queues.
                    // If all queues are flushed, go into Done state.
                    todo!()
                }
                CaConnState::Done => todo!(),
            }

            // break if self.is_shutdown() {
            //     if self.queues_out_flushed() {
            //         debug!("is_shutdown  queues_out_flushed  set EOS  {}", self.remote_addr_dbg);
            //         if let CaConnState::Shutdown(x) = std::mem::replace(&mut self.state, CaConnState::EndOfStream) {
            //             Ready(Some(CaConnEvent::new_now(CaConnEventValue::EndOfStream(x))))
            //         } else {
            //             continue;
            //         }
            //     } else {
            //         if have_progress {
            //             debug!("is_shutdown  NOT queues_out_flushed  prog  {}", self.remote_addr_dbg);
            //             self.stats.poll_reloop().inc();
            //             reloops += 1;
            //             continue;
            //         } else if have_pending {
            //             debug!("is_shutdown  NOT queues_out_flushed  pend  {}", self.remote_addr_dbg);
            //             self.log_queues_summary();
            //             self.stats.poll_pending().inc();
            //             Pending
            //         } else {
            //             // TODO error
            //             error!("shutting down, queues not flushed, no progress, no pending");
            //             self.stats.logic_error().inc();
            //             let e = Error::ShutdownWithQueuesNoProgressNoPending;
            //             Ready(Some(CaConnEvent::err_now(e)))
            //         }
            //     }
            // } else {
            //     if have_progress {
            //         if poll_ts1.elapsed() > Duration::from_millis(5) {
            //             self.stats.poll_wake_break().inc();
            //             cx.waker().wake_by_ref();
            //             break Ready(Some(CaConnEvent::new(self.poll_tsnow, CaConnEventValue::None)));
            //         } else {
            //             self.stats.poll_reloop().inc();
            //             reloops += 1;
            //             continue;
            //         }
            //     } else if have_pending {
            //         self.stats.poll_pending().inc();
            //         Pending
            //     } else {
            //         self.stats.poll_no_progress_no_pending().inc();
            //         let e = Error::NoProgressNoPending;
            //         Ready(Some(CaConnEvent::err_now(e)))
            //     }
            // };

            break if hpp.is_progress() {
                continue;
            } else if hpp.is_pending() {
                Pending
            } else {
                Ready(None)
            };
        };

        durs.step();
        // if self.trace_channel_poll {
        //     self.stats.poll_all_dt().ingest_dur_dms(dt);
        //     if dt >= Duration::from_millis(10) {
        //         trace!("long poll {dt:?}");
        //     } else if dt >= Duration::from_micros(400) {
        //         let v = self.stats.poll_all_dt.to_display();
        //         let ip = self.remote_addr_dbg;
        //         trace!("poll_all_dt  {ip}  {v}");
        //     }
        // }
        // self.stats.read_ioids_len().set(self.read_ioids.len() as u64);
        // let n = match &self.proto {
        //     Some(x) => x.proto_out_len() as u64,
        //     None => 0,
        // };
        // self.stats.proto_out_len().set(n);
        // self.stats.poll_reloops().ingest(reloops);
        ret
    }
}
