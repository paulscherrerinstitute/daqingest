#[allow(non_snake_case)]
pub mod serde_dummy {
    use serde::Serializer;

    #[allow(unused)]
    pub fn serialize<S, T>(val: &T, ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        ser.serialize_str("DUMMY")
    }
}
