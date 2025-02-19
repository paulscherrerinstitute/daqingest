use super::CaConn;
use super::CreatedState;
use super::Ioid;
use ca_proto::ca::proto;
use dbpg::seriesbychannel::ChannelInfoQuery;
use log::*;
use proto::CaMsg;
use proto::ReadNotify;
use series::SeriesId;
use std::pin::Pin;
use std::time::Instant;

autoerr::create_error_v1!(
    name(Error, "NetfetchEnumfetch"),
    enum variants {
        MissingState,
    },
);

pub trait ConnFuture: Send {
    fn camsg(self: Pin<&mut Self>, camsg: CaMsg, conn: &mut CaConn) -> Result<(), Error>;
}

pub struct EnumFetch {
    created_state: CreatedState,
    ioid: Ioid,
}

impl EnumFetch {
    pub fn new(created_state: CreatedState, conn: &mut CaConn) -> Self {
        if created_state.cssid.id() == 4705698279895902114 {}
        // info!("EnumFetch::new  name {}", created_state.name());
        let dbr_ctrl_enum = 31;
        let ioid = conn.ioid_next();
        let ty = proto::CaMsgTy::ReadNotify(ReadNotify {
            data_type: dbr_ctrl_enum,
            data_count: 0,
            sid: created_state.sid.to_u32(),
            ioid: ioid.0,
        });
        let ts = Instant::now();
        let item = CaMsg::from_ty_ts(ty, ts);
        conn.proto().unwrap().push_out(item);
        Self { created_state, ioid }
    }

    pub fn ioid(&self) -> Ioid {
        self.ioid
    }
}

impl ConnFuture for EnumFetch {
    fn camsg(mut self: Pin<&mut Self>, camsg: CaMsg, conn: &mut CaConn) -> Result<(), Error> {
        let tsnow = Instant::now();
        let crst = &mut self.created_state;
        match camsg.ty {
            proto::CaMsgTy::ReadNotifyRes(msg2) => match msg2.value.meta {
                proto::CaMetaValue::CaMetaVariants(meta) => {
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

        let cid = crst.cid.clone();
        let (tx, rx) = async_channel::bounded(8);
        let item = ChannelInfoQuery {
            backend: conn.backend.clone(),
            channel: crst.name().into(),
            kind: netpod::SeriesKind::CaStatus,
            scalar_type: netpod::ScalarType::I16,
            shape: netpod::Shape::Scalar,
            tx: Box::pin(tx),
        };
        conn.channel_info_query_qu.push_back(item);
        conn.channel_info_query_res_rxs.push_back((Box::pin(rx), cid));

        // This handler must not exist if the channel gets removed.
        let conf = conn.channels.get_mut(&crst.cid).ok_or(Error::MissingState)?;
        conf.state = super::ChannelState::FetchCaStatusSeries(super::MakingSeriesWriterState {
            tsbeg: tsnow,
            channel: crst.clone(),
            series_status: SeriesId::new(0),
        });

        conn.handler_by_ioid.remove(&self.ioid);
        Ok(())
    }
}
