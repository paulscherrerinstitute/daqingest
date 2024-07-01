use super::CaConn;
use super::CreatedState;
use super::Ioid;
use crate::ca::proto::CaMsg;
use crate::ca::proto::ReadNotify;
use err::thiserror;
use err::ThisError;
use log::*;
use serieswriter::establish_worker::EstablishWorkerJob;
use std::pin::Pin;
use std::task::Poll;
use std::time::Instant;

#[derive(Debug, ThisError)]
pub enum Error {}

pub trait ConnFuture: Send {
    fn camsg(self: Pin<&mut Self>, camsg: CaMsg, conn: &mut CaConn) -> Poll<()>;
}

pub struct EnumFetch {
    created_state: CreatedState,
    ioid: Ioid,
    min_quiets: serieswriter::rtwriter::MinQuiets,
}

impl EnumFetch {
    pub fn new(created_state: CreatedState, conn: &mut CaConn, min_quiets: serieswriter::rtwriter::MinQuiets) -> Self {
        let name = created_state.name();
        info!("EnumFetch::new  name {name}");
        let dbr_ctrl_enum = 31;
        let ioid = conn.ioid_next();
        let ty = crate::ca::proto::CaMsgTy::ReadNotify(ReadNotify {
            data_type: dbr_ctrl_enum,
            data_count: 0,
            sid: created_state.sid.to_u32(),
            ioid: ioid.0,
        });
        let ts = Instant::now();
        let item = CaMsg::from_ty_ts(ty, ts);
        conn.proto().unwrap().push_out(item);
        Self {
            created_state,
            ioid,
            min_quiets,
        }
    }

    pub fn ioid(&self) -> Ioid {
        self.ioid
    }
}

impl ConnFuture for EnumFetch {
    fn camsg(self: Pin<&mut Self>, camsg: CaMsg, conn: &mut CaConn) -> Poll<()> {
        use Poll::*;
        let tsnow = Instant::now();
        let crst = &self.created_state;

        let name = self.created_state.name();
        info!("EnumFetch::poll  {name}");

        //*chst =
        super::ChannelState::MakingSeriesWriter(super::MakingSeriesWriterState {
            tsbeg: tsnow,
            channel: crst.clone(),
        });
        let job = EstablishWorkerJob::new(
            serieswriter::establish_worker::JobId(crst.cid.0 as _),
            conn.backend.clone(),
            crst.name().into(),
            crst.cssid.clone(),
            crst.scalar_type.clone(),
            crst.shape.clone(),
            self.min_quiets.clone(),
            conn.writer_tx.clone(),
            conn.tmp_ts_poll,
        );
        conn.writer_establish_qu.push_back(job);

        Pending
    }
}
