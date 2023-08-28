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

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub struct SeriesId(u64);

impl SeriesId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn id(&self) -> u64 {
        self.0
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize)]
pub struct ChannelStatusSeriesId(u64);

impl ChannelStatusSeriesId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn id(&self) -> u64 {
        self.0
    }
}
