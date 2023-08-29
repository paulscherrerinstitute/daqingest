#[allow(non_snake_case)]
mod serde_Instant {
    use serde::Serializer;
    use std::time::Instant;

    #[allow(unused)]
    pub fn serialize<S>(val: &Instant, ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let dur = val.elapsed();
        ser.serialize_u64(dur.as_secs() * 1000 + dur.subsec_millis() as u64)
    }
}
