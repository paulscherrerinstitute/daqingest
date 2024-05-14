use core::fmt;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug)]
pub enum Existence<T> {
    Created(T),
    Existing(T),
}

impl<T> Existence<T> {
    pub fn into_inner(self) -> T {
        use Existence::*;
        match self {
            Created(x) => x,
            Existing(x) => x,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SeriesId(u64);

impl SeriesId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn id(&self) -> u64 {
        self.0
    }

    pub fn to_i64(&self) -> i64 {
        self.0 as i64
    }
}

impl fmt::Display for SeriesId {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "SeriesId {{ {:20} }}", self.0)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ChannelStatusSeriesId(u64);

impl ChannelStatusSeriesId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn id(&self) -> u64 {
        self.0
    }

    pub fn to_i64(&self) -> i64 {
        self.0 as i64
    }
}
