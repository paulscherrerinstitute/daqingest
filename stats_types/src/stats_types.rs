use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::AcqRel;
use std::sync::atomic::Ordering::Acquire;
use std::sync::atomic::Ordering::Release;

#[derive(Debug)]
pub struct CounterDesc {
    pub name: String,
}

#[derive(Debug)]
pub struct StatsStruct {
    pub name: String,
    pub counters: Vec<CounterDesc>,
}

#[derive(Debug)]
pub struct StatsStructDef {
    pub name: String,
    pub counters: Vec<CounterDesc>,
}

#[derive(Debug)]
pub struct Counter(AtomicU64);

impl Counter {
    pub fn new() -> Self {
        Counter(AtomicU64::new(0))
    }

    pub fn init(x: u64) -> Self {
        Counter(AtomicU64::new(x))
    }

    pub fn inc(&self) {
        self.0.fetch_add(1, AcqRel);
    }

    pub fn add(&self, x: u64) {
        self.0.fetch_add(x, AcqRel);
    }

    pub fn load(&self) -> u64 {
        self.0.load(Acquire)
    }

    pub fn __set(&self, x: u64) {
        self.0.store(x, Release);
    }
}

#[derive(Debug)]
pub struct Value(AtomicU64);

impl Value {
    pub fn new() -> Self {
        Value(AtomicU64::new(0))
    }

    pub fn init(x: u64) -> Self {
        Value(AtomicU64::new(x))
    }

    pub fn set(&self, x: u64) {
        self.0.store(x, Release);
    }

    pub fn load(&self) -> u64 {
        self.0.load(Acquire)
    }
}

pub trait DropMark {
    fn field(&self) -> &Value;
}

pub struct DropGuard<'a> {
    mark: &'a Value,
}

impl<'a> Drop for DropGuard<'a> {
    fn drop(&mut self) {
        self.mark.set(1);
    }
}

#[allow(unused)]
struct StatsAInner {
    count0: Counter,
    val0: Value,
    done: Value,
}

#[allow(unused)]
struct StatsA {
    inner: std::sync::Arc<StatsAInner>,
}

impl Drop for StatsA {
    fn drop(&mut self) {
        self.inner.done.set(1);
    }
}

#[allow(unused)]
struct StatsAReader {
    inner: std::sync::Arc<StatsAInner>,
}

impl StatsAReader {}
