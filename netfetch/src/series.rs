use crate::bsread::ChannelDescDecoded;
use crate::errconv::ErrConv;
use err::Error;
#[allow(unused)]
use log::*;
use std::time::Duration;
use tokio_postgres::Client as PgClient;

#[derive(Clone, Debug)]
pub enum Existence<T> {
    Created(T),
    Existing(T),
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct SeriesId(u64);

impl SeriesId {
    pub fn id(&self) -> u64 {
        self.0
    }
}

// TODO don't need byte_order or compression from ChannelDescDecoded for channel registration.
pub async fn get_series_id(pg_client: &PgClient, cd: &ChannelDescDecoded) -> Result<Existence<SeriesId>, Error> {
    let facility = "scylla";
    let channel_name = &cd.name;
    let scalar_type = cd.scalar_type.to_scylla_i32();
    let shape = cd.shape.to_scylla_vec();
    let res = pg_client
        .query(
            "select series from series_by_channel where facility = $1 and channel = $2 and scalar_type = $3 and shape_dims = $4 and agg_kind = 0",
            &[&facility, channel_name, &scalar_type, &shape],
        )
        .await
        .err_conv()?;
    let mut all = vec![];
    for row in res {
        let series: i64 = row.get(0);
        let series = series as u64;
        all.push(series);
    }
    let rn = all.len();
    if rn == 0 {
        use md5::Digest;
        let mut h = md5::Md5::new();
        h.update(facility.as_bytes());
        h.update(channel_name.as_bytes());
        h.update(format!("{:?} {:?}", scalar_type, shape).as_bytes());
        let f = h.finalize();
        let mut series = u64::from_le_bytes(f.as_slice()[0..8].try_into().unwrap());
        if series > i64::MAX as u64 {
            series &= 0x7fffffffffffffff;
        }
        for _ in 0..2000 {
            if series < 1 || series > i64::MAX as u64 {
                series = 1;
            }
            let res = pg_client
                .execute(
                    concat!(
                        "insert into series_by_channel",
                        " (series, facility, channel, scalar_type, shape_dims, agg_kind)",
                        " values ($1, $2, $3, $4, $5, 0) on conflict do nothing"
                    ),
                    &[&(series as i64), &facility, channel_name, &scalar_type, &shape],
                )
                .await
                .unwrap();
            if res == 1 {
                let series = Existence::Created(SeriesId(series));
                return Ok(series);
            } else {
                warn!(
                    "tried to insert {series:?} for {facility} {channel_name} {scalar_type:?} {shape:?} trying again..."
                );
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
            series += 1;
        }
        error!("tried to insert {series:?} for {facility} {channel_name} {scalar_type:?} {shape:?} but it failed");
        Err(Error::with_msg_no_trace(format!("get_series_id  can not create and insert series id  {facility:?}  {channel_name:?}  {scalar_type:?}  {shape:?}")))
    } else {
        let series = all[0] as u64;
        let series = Existence::Existing(SeriesId(series));
        debug!("get_series_id  {facility:?}  {channel_name:?}  {scalar_type:?}  {shape:?}  {series:?}");
        Ok(series)
    }
}
