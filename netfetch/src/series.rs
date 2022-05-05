use crate::bsread::ChannelDescDecoded;
use crate::zmtp::ErrConv;
use err::Error;
#[allow(unused)]
use log::*;
use scylla::Session as ScySession;
use std::time::Duration;

#[derive(Clone, Debug)]
pub enum Existence<T> {
    Created(T),
    Existing(T),
}

#[derive(Clone, Debug)]
pub struct SeriesId(u64);

impl SeriesId {
    pub fn id(&self) -> u64 {
        self.0
    }
}

// TODO don't need byte_order or compression from ChannelDescDecoded for channel registration.
pub async fn get_series_id(scy: &ScySession, cd: &ChannelDescDecoded) -> Result<Existence<SeriesId>, Error> {
    let facility = "scylla";
    let channel_name = &cd.name;
    let scalar_type = cd.scalar_type.to_scylla_i32();
    let shape = cd.shape.to_scylla_vec();
    info!("get_series_id  {facility:?}  {channel_name:?}  {scalar_type:?}  {shape:?}");
    let res = scy
        .query(
            "select series, agg_kind from series_by_channel where facility = ? and channel_name = ? and scalar_type = ? and shape_dims = ?",
            (facility, channel_name, &scalar_type, &shape),
        )
        .await
        .err_conv()?;
    let mut all = vec![];
    for row in res.rows_typed_or_empty::<(i64, Option<String>)>() {
        match row {
            Ok(k) => {
                if k.1.is_none() {
                    all.push(k.0);
                }
            }
            Err(e) => return Err(Error::with_msg_no_trace(format!("{e:?}"))),
        }
    }
    info!("all: {all:?}");
    let rn = all.len();
    if rn == 0 {
        use md5::Digest;
        let mut h = md5::Md5::new();
        h.update(facility.as_bytes());
        h.update(channel_name.as_bytes());
        h.update(format!("{:?} {:?}", scalar_type, shape).as_bytes());
        let f = h.finalize();
        let mut series = u64::from_le_bytes(f.as_slice()[0..8].try_into().unwrap());
        // TODO technically we could/should assert that we run on 2-complement machine.
        const SMASK: u64 = 0x7fffffffffffffff;
        series = series & SMASK;
        for _ in 0..2000 {
            let res = scy
                .query(
                    concat!(
                        "insert into series_by_channel",
                        " (facility, channel_name, scalar_type, shape_dims, agg_kind, series)",
                        " values (?, ?, ?, ?, null, ?) if not exists"
                    ),
                    (facility, channel_name, &scalar_type, &shape, series as i64),
                )
                .await
                .err_conv()?;
            let row = res.first_row().err_conv()?;
            if row.columns[0].as_ref().unwrap().as_boolean().unwrap() {
                return Ok(Existence::Created(SeriesId(series)));
            } else {
                error!("tried to insert but series exists...");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
            series += 1;
            series = series & SMASK;
        }
        Err(Error::with_msg_no_trace(format!("can not create and insert series id")))
    } else {
        let series = all[0] as u64;
        info!("series: {:?}", series);
        Ok(Existence::Existing(SeriesId(series)))
    }
}
