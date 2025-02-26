use crate::conn::PgClient;
use netpod::ChannelConfigResponse;
use netpod::DaqbufChannelConfig;
use netpod::ScalarType;
use netpod::SeriesKind;
use netpod::Shape;
use series::SeriesId;
use tokio_postgres::types::Type as PgTy;

autoerr::create_error_v1!(
    name(Error, "ConfigBySeries"),
    enum variants {
        Postgres(#[from] tokio_postgres::Error),
        Netpod(#[from] netpod::Error),
    },
);

pub async fn channel_config_by_series(pg: &PgClient, series: SeriesId) -> Result<Option<DaqbufChannelConfig>, Error> {
    let sql = concat!(
        "select t.series, t.facility, t.kind, t.scalar_type, t.shape_dims, t.channel",
        " from series_by_channel t",
        " where t.series = $1",
        " limit 10",
    );
    let qu_select = pg.prepare_typed(sql, &[PgTy::INT8]).await?;
    let id = series.id() as i64;
    let res = pg.query(&qu_select, &[&id]).await?;
    for row in res {
        let series: i64 = row.try_get(0)?;
        let backend = row.try_get(1)?;
        let kind = row.try_get(2)?;
        let scalar_type = row.try_get(3)?;
        let shape_dims: Vec<_> = row.try_get(4)?;
        let name = row.try_get(5)?;
        let series = series as u64;
        let series_kind = SeriesKind::from_db_i16(kind)?;
        let scalar_type = ScalarType::from_scylla_i32(scalar_type)?;
        let shape = Shape::from_scylla_shape_dims(&shape_dims)?;
        let conf = DaqbufChannelConfig {
            backend,
            series,
            kind: series_kind,
            scalar_type,
            shape,
            name,
        };
        return Ok(Some(conf));
    }
    Ok(None)
}
