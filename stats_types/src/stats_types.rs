pub use serde_json;

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

    pub fn inc(&self) {
        self.0.fetch_add(1, AcqRel);
    }

    pub fn dec(&self) {
        self.0.fetch_sub(1, AcqRel);
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

pub struct HistoLog2 {
    histo: [AtomicU64; 20],
    sum: AtomicU64,
    sub: u16,
}

macro_rules! rep16 {
    ([$x:expr]) => {
        [
            $x, $x, $x, $x, $x, $x, $x, $x, $x, $x, $x, $x, $x, $x, $x, $x, $x, $x, $x, $x,
        ]
    };
}

impl HistoLog2 {
    pub fn new(sub: u16) -> Self {
        Self {
            histo: rep16!([AtomicU64::new(0)]),
            sum: AtomicU64::new(0),
            sub,
        }
    }

    #[inline]
    pub fn ingest(&self, mut v: u32) {
        self.sum.fetch_add(v as u64, AcqRel);
        v >>= self.sub;
        let mut po = 0;
        while v != 0 && po < self.histo.len() - 1 {
            v >>= 1;
            po += 1;
        }
        self.histo[po].fetch_add(1, AcqRel);
    }

    pub fn to_prometheus(&self, name: &str) -> String {
        let base: u32 = 2;
        let mut ret = String::with_capacity(2048);
        ret.push_str("# HELP ");
        ret.push_str(name);
        ret.push_str(" help-text-missing\n");
        ret.push_str("# TYPE ");
        ret.push_str(name);
        ret.push_str(" histogram\n");
        let mut cnt = 0;
        let lastix = (self.histo.len() - 1) as u32;
        for (i, a) in self.histo.iter().enumerate() {
            use std::ops::Sub;
            let i = i as u32;
            let le = base.pow(i).sub(1);
            let v = a.load(Acquire);
            cnt += v;
            ret.push_str(name);
            ret.push_str("_bucket{le=\"");
            if i == lastix {
                ret.push_str("+Inf");
            } else {
                ret.push_str(&le.to_string());
            }
            ret.push_str("\"} ");
            ret.push_str(&cnt.to_string());
            ret.push_str("\n");
        }
        ret.push_str(name);
        ret.push_str("_count ");
        ret.push_str(&cnt.to_string());
        ret.push_str("\n");

        let sum = self.sum.load(Acquire);
        ret.push_str(name);
        ret.push_str("_sum ");
        ret.push_str(&sum.to_string());
        ret.push_str("\n");
        ret
    }

    pub fn to_json(&self, _name: &str) -> serde_json::Value {
        serde_json::Value::Null
    }
}

#[test]
fn histo_00() {
    let histo = HistoLog2::new(0);
    // histo.ingest(0);
    // histo.ingest(1);
    // histo.ingest(2);
    histo.ingest(3);
    histo.ingest(4);
    histo.ingest(262143);
    histo.ingest(262144);
    let s = histo.to_prometheus("the_metric");
    // eprintln!("{s}");
    let exp = r##"# HELP the_metric help-text-missing
# TYPE the_metric histogram
the_metric_bucket{le="0"} 0
the_metric_bucket{le="1"} 0
the_metric_bucket{le="3"} 1
the_metric_bucket{le="7"} 2
the_metric_bucket{le="15"} 2
the_metric_bucket{le="31"} 2
the_metric_bucket{le="63"} 2
the_metric_bucket{le="127"} 2
the_metric_bucket{le="255"} 2
the_metric_bucket{le="511"} 2
the_metric_bucket{le="1023"} 2
the_metric_bucket{le="2047"} 2
the_metric_bucket{le="4095"} 2
the_metric_bucket{le="8191"} 2
the_metric_bucket{le="16383"} 2
the_metric_bucket{le="32767"} 2
the_metric_bucket{le="65535"} 2
the_metric_bucket{le="131071"} 2
the_metric_bucket{le="262143"} 3
the_metric_bucket{le="+Inf"} 4
the_metric_count 4
the_metric_sum 524294
"##;
    assert_eq!(s, exp);
}
