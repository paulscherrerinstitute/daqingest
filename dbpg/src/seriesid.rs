use crate::conn::PgClient;
use err::thiserror;
use err::ThisError;
use log::*;
use netpod::ScalarType;
use netpod::Shape;
use series::series::Existence;
use series::SeriesId;
use std::time::Duration;
use std::time::Instant;
use taskrun::tokio;

#[derive(Debug, ThisError)]
pub enum Error {
    Postgres(#[from] tokio_postgres::Error),
    IocAddrNotFound,
    BadIdGenerated,
    CanNotInsertSeriesId,
}

// TODO don't need byte_order or compression from ChannelDescDecoded for channel registration.
pub async fn get_series_id(
    name: &str,
    scalar_type: &ScalarType,
    shape: &Shape,
    pg_client: &PgClient,
    backend: String,
) -> Result<Existence<SeriesId>, Error> {
    let channel_name = name;
    let scalar_type = scalar_type.to_scylla_i32();
    let shape = shape.to_scylla_vec();
    let res = pg_client
        .query(
            "select series from series_by_channel where facility = $1 and channel = $2 and scalar_type = $3 and shape_dims = $4 and agg_kind = 0",
            &[&backend, &channel_name, &scalar_type, &shape],
        )
        .await?;
    let mut all = Vec::new();
    for row in res {
        let series: i64 = row.get(0);
        let series = series as u64;
        all.push(series);
    }
    let rn = all.len();
    let tsbeg = Instant::now();
    if rn == 0 {
        use md5::Digest;
        let mut h = md5::Md5::new();
        h.update(backend.as_bytes());
        h.update(channel_name.as_bytes());
        h.update(format!("{:?}", scalar_type).as_bytes());
        h.update(format!("{:?}", shape).as_bytes());
        for _ in 0..200 {
            h.update(tsbeg.elapsed().subsec_nanos().to_ne_bytes());
            let f = h.clone().finalize();
            let series = u64::from_le_bytes(f.as_slice()[0..8].try_into().unwrap());
            if series > i64::MAX as u64 {
                continue;
            }
            if series == 0 {
                continue;
            }
            if series <= 0 || series > i64::MAX as u64 {
                return Err(Error::BadIdGenerated);
            }
            let sql = concat!(
                "insert into series_by_channel",
                " (series, facility, channel, scalar_type, shape_dims, agg_kind)",
                " values ($1, $2, $3, $4, $5, 0) on conflict do nothing"
            );
            let res = pg_client
                .execute(sql, &[&(series as i64), &backend, &channel_name, &scalar_type, &shape])
                .await
                .unwrap();
            if res == 1 {
                let series = Existence::Created(SeriesId::new(series));
                return Ok(series);
            } else {
                warn!(
                    "tried to insert {series:?} for {backend:?} {channel_name:?} {scalar_type:?} {shape:?} trying again..."
                );
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        error!("tried to insert new series id for {backend:?} {channel_name:?} {scalar_type:?} {shape:?} but failed");
        Err(Error::CanNotInsertSeriesId)
    } else {
        let series = all[0] as u64;
        let series = Existence::Existing(SeriesId::new(series));
        debug!("get_series_id  {backend:?}  {channel_name:?}  {scalar_type:?}  {shape:?}  {series:?}");
        Ok(series)
    }
}
