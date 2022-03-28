use log::*;
use scylla::batch::Consistency;
use scylla::transport::errors::{NewSessionError, QueryError};
use scylla::SessionBuilder;

pub struct Error(err::Error);

impl err::ToErr for Error {
    fn to_err(self) -> err::Error {
        self.0
    }
}

impl From<NewSessionError> for Error {
    fn from(e: NewSessionError) -> Self {
        Self(err::Error::with_msg_no_trace(format!("{e:?}")))
    }
}

impl From<QueryError> for Error {
    fn from(e: QueryError) -> Self {
        Self(err::Error::with_msg_no_trace(format!("{e:?}")))
    }
}

pub async fn list_pkey() -> Result<(), Error> {
    let scy = SessionBuilder::new()
        .known_node("127.0.0.1:19042")
        .default_consistency(Consistency::One)
        .use_keyspace("ks1", false)
        .build()
        .await?;
    let query = scy
        .prepare("select distinct token(pulse_a), pulse_a from pulse where token(pulse_a) >= ? and token(pulse_a) <= ?")
        .await?;
    let td = i64::MAX / 27;
    let mut t1 = i64::MIN;
    let mut pulse_a_max = 0;
    loop {
        let t2 = if t1 < i64::MAX - td { t1 + td } else { i64::MAX };
        let pct = (t1 - i64::MIN) as u64 / (u64::MAX / 100000);
        info!("Token range {:.2}%", pct as f32 * 1e-3);
        let qr = scy.execute(&query, (t1, t2)).await?;
        if let Some(rows) = qr.rows {
            for r in rows {
                if r.columns.len() < 2 {
                    warn!("see {} columns", r.columns.len());
                } else {
                    let pulse_a_token = r.columns[0].as_ref().unwrap().as_bigint().unwrap();
                    let pulse_a = r.columns[1].as_ref().unwrap().as_bigint().unwrap();
                    info!("pulse_a_token {pulse_a_token}  pulse_a {pulse_a}");
                    pulse_a_max = pulse_a_max.max(pulse_a);
                }
            }
        }
        if t2 == i64::MAX {
            info!("end of token range");
            break;
        } else {
            t1 = t2 + 1;
        }
    }
    info!("pulse_a_max {pulse_a_max}");
    Ok(())
}

pub async fn list_pulses() -> Result<(), Error> {
    let scy = SessionBuilder::new()
        .known_node("127.0.0.1:19042")
        .default_consistency(Consistency::One)
        .use_keyspace("ks1", false)
        .build()
        .await?;
    let query = scy
        .prepare("select token(tsa) as tsatok, tsa, tsb, pulse from pulse where token(tsa) >= ? and token(tsa) <= ?")
        .await?;
    let td = i64::MAX / 31;
    let mut t1 = i64::MIN;
    loop {
        let t2 = if t1 < i64::MAX - td { t1 + td } else { i64::MAX };
        let pct = (t1 - i64::MIN) as u64 / (u64::MAX / 100000);
        info!("Token range {:.2}%", pct as f32 * 1e-3);
        let qr = scy.execute(&query, (t1, t2)).await?;
        if let Some(rows) = qr.rows {
            for r in rows {
                if r.columns.len() < 2 {
                    warn!("see {} columns", r.columns.len());
                } else {
                    let tsa_token = r.columns[0].as_ref().unwrap().as_bigint().unwrap();
                    let tsa = r.columns[1].as_ref().unwrap().as_int().unwrap() as u32;
                    let tsb = r.columns[2].as_ref().unwrap().as_int().unwrap() as u32;
                    let pulse = r.columns[3].as_ref().unwrap().as_bigint().unwrap() as u64;
                    info!("tsa_token {tsa_token:21}  tsa {tsa:12}  tsb {tsb:12}  pulse {pulse:21}");
                }
            }
        }
        if t2 == i64::MAX {
            info!("end of token range");
            break;
        } else {
            t1 = t2 + 1;
        }
    }
    Ok(())
}
