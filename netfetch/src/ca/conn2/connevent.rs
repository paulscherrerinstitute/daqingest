use super::channelstateinfo::ChannelStateInfo;
use core::fmt;
use series::ChannelStatusSeriesId;
use std::collections::BTreeMap;
use std::time::Instant;

#[derive(Debug)]
pub struct CaConnEvent {
    pub ts: Instant,
    pub value: CaConnEventValue,
}

impl CaConnEvent {
    pub fn new(ts: Instant, value: CaConnEventValue) -> Self {
        Self { ts, value }
    }

    pub fn err_now(err: super::conn::Error) -> Self {
        Self::new_now(CaConnEventValue::EndOfStream(EndOfStreamReason::Error(err)))
    }

    pub fn new_now(value: CaConnEventValue) -> Self {
        Self {
            ts: Instant::now(),
            value,
        }
    }

    pub fn desc_short(&self) -> CaConnEventDescShort {
        CaConnEventDescShort { inner: self }
    }
}

pub struct CaConnEventDescShort<'a> {
    inner: &'a CaConnEvent,
}

impl<'a> fmt::Display for CaConnEventDescShort<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "CaConnEventDescShort {{ ts: {:?}, value: {} }}",
            self.inner.ts,
            self.inner.value.desc_short()
        )
    }
}

#[derive(Debug)]
pub enum CaConnEventValue {
    None,
    EchoTimeout,
    // ConnCommandResult(ConnCommandResult),
    ChannelStatus(ChannelStatusPartial),
    ChannelCreateFail(String),
    EndOfStream(EndOfStreamReason),
}

impl CaConnEventValue {
    pub fn desc_short(&self) -> &'static str {
        match self {
            CaConnEventValue::None => "None",
            CaConnEventValue::EchoTimeout => "EchoTimeout",
            // CaConnEventValue::ConnCommandResult(_) => "ConnCommandResult",
            CaConnEventValue::ChannelStatus(_) => "ChannelStatus",
            CaConnEventValue::ChannelCreateFail(_) => "ChannelCreateFail",
            CaConnEventValue::EndOfStream(_) => "EndOfStream",
        }
    }
}

#[derive(Debug)]
pub enum EndOfStreamReason {
    UnspecifiedReason,
    Error(super::conn::Error),
    ConnectRefused,
    ConnectTimeout,
    OnCommand,
    RemoteClosed,
    IocTimeout,
    IoError,
}

#[derive(Debug)]
pub struct ChannelStatusPartial {
    pub channel_statuses: BTreeMap<ChannelStatusSeriesId, ChannelStateInfo>,
}
