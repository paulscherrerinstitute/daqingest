use super::CaConn;
use super::CreatedState;
use super::Ioid;
use crate::ca::proto::CaMsg;
use crate::ca::proto::ReadNotify;
use dbpg::seriesbychannel::ChannelInfoQuery;
use err::thiserror;
use err::ThisError;
use log::*;
use std::pin::Pin;
use std::time::Instant;

#[derive(Debug, ThisError)]
#[cstm(name = "NetfetchEnumfetch")]
pub enum Error {
    MissingState,
}

pub trait ConnFuture: Send {
    fn camsg(self: Pin<&mut Self>, camsg: CaMsg, conn: &mut CaConn) -> Result<(), Error>;
}

pub struct EnumFetch {
    created_state: CreatedState,
    ioid: Ioid,
    min_quiets: serieswriter::rtwriter::MinQuiets,
}

impl EnumFetch {
    pub fn new(created_state: CreatedState, conn: &mut CaConn, min_quiets: serieswriter::rtwriter::MinQuiets) -> Self {
        if created_state.cssid.id() == 4705698279895902114 {}
        let name = created_state.name();
        // info!("EnumFetch::new  name {name}");
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
    fn camsg(mut self: Pin<&mut Self>, camsg: CaMsg, conn: &mut CaConn) -> Result<(), Error> {
        let tsnow = Instant::now();
        let crst = &mut self.created_state;

        let name = crst.name();
        // info!("EnumFetch::poll  {name}");

        match camsg.ty {
            crate::ca::proto::CaMsgTy::ReadNotifyRes(msg2) => match msg2.value.meta {
                super::proto::CaMetaValue::CaMetaVariants(meta) => {
                    crst.enum_str_table = Some(meta.variants);
                }
                _ => {
                    warn!("unexpected message");
                }
            },
            _ => {
                warn!("unexpected message");
            }
        };

        // TODO create a channel for the answer.
        // TODO register the channel for the answer.
        let cid = crst.cid.clone();
        let (tx, rx) = async_channel::bounded(8);
        let item = ChannelInfoQuery {
            backend: conn.backend.clone(),
            channel: crst.name().into(),
            kind: netpod::SeriesKind::ChannelData,
            scalar_type: crst.scalar_type.clone(),
            shape: crst.shape.clone(),
            tx: Box::pin(tx),
        };
        conn.channel_info_query_qu.push_back(item);
        conn.channel_info_query_res_rxs.push_back((Box::pin(rx), cid));

        // This handler must not exist if the channel gets removed.
        let conf = conn.channels.get_mut(&crst.cid).ok_or(Error::MissingState)?;
        conf.state = super::ChannelState::MakingSeriesWriter(super::MakingSeriesWriterState {
            tsbeg: tsnow,
            channel: crst.clone(),
        });

        conn.handler_by_ioid.remove(&self.ioid);
        Ok(())
    }
}
