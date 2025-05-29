use super::BinWriter;
use crate::rtwriter::MinQuiets;
use log::*;
use netpod::DtMs;
use netpod::ScalarType;
use netpod::Shape;
use netpod::TsMs;
use netpod::TsNano;
use scywr::insertqueues::InsertDeques;
use scywr::iteminsertqueue::QueryItem;
use scywr::iteminsertqueue::TimeBinSimpleF32V02;
use series::ChannelStatusSeriesId;
use series::SeriesId;
use series::msp::PrebinnedPartitioning;
use std::collections::VecDeque;

const T0: TsMs = TsMs::from_ms_u64(1000 * 60 * 60 * 36);

macro_rules! debug_item {
    ($fmt:expr) => {
        eprintln!("{0}  {0:?}", format_args!($fmt));
    };
    ($fmt:expr, $arg1:expr) => {
        // let h = format_args!($fmt, $arg1);
        eprintln!("h: {:?}", format_args!($fmt, $arg1));
    };
    ($fmt:expr, $($args:expr),*) => {
        // let h = format_args!($fmt, $arg1);
        // eprintln!("h: {:?}", format_args!($fmt, $arg1));
        eprintln!("h: {:?}", format_args!($fmt, $($args),*));
    };
}

fn def<T: Default>() -> T {
    Default::default()
}

fn sec(sec: f32) -> TsNano {
    T0.add_dt_ms(DtMs::from_ms_u64((1e3 * sec) as u64)).ns()
    // TsNano::from_ms((1e3 * sec) as u64)
}

fn collect_bin_write_only(qu: &VecDeque<QueryItem>) -> VecDeque<TimeBinSimpleF32V02> {
    // TODO
    // take the scylla insert items and store them in hashmap according to binlen
    // in a deque for more easy comparison later.
    // let mut ret = def();
    let mut ret = VecDeque::new();
    for item in qu {
        match item {
            QueryItem::TimeBinSimpleF32V02(bin) => {
                ret.push_back(bin.clone());
            }
            _ => {}
        }
    }
    ret
}

fn print_binscol(rt: &str, binscol: &VecDeque<TimeBinSimpleF32V02>) {
    for bin in binscol {
        debug!(
            "{}  bl {:5}  msp {:4}  off {:4}  cnt {:3}  min {:7.2}  max {:7.2}  lst {:7.2}",
            rt,
            // min = bin.min,
            bin.binlen / 1000,
            bin.msp,
            bin.off,
            bin.cnt,
            bin.min,
            bin.max,
            bin.lst
        );
    }
}

#[test]
fn binwriter_nest01_00() {
    let _ = PrebinnedPartitioning::Day1;
    let _ = format_args!("");
    let _ = format_args!(concat!("", ""));
    debug_item!("plain fmt string");
    debug_item!("plain fmt string with comma");
    debug_item!("param as param: {:?}", "some--string123");
    let some_string = "some--string123";
    debug_item!("param interpolated: {some_string:?}");
    debug_item!("param interpolated: {some_string}");
    let beg = TsNano::from_ms(1000 * 40);
    let min_quiets = MinQuiets::test_1_10_60();
    let is_polled = false;
    let cssid = ChannelStatusSeriesId::new(50);
    let sid = SeriesId::new(51);
    let scalar_type = ScalarType::F32;
    let shape = Shape::Scalar;
    let chname2 = String::from("daqbuftest");
    let mut iqdqs = InsertDeques::new();
    let mut binwriter = BinWriter::new(beg, min_quiets, is_polled, cssid, sid, scalar_type, shape, chname2).unwrap();
    binwriter.ingest(sec(39.9), 2.2, &mut iqdqs).unwrap();
    binwriter.ingest(sec(40.0), 2., &mut iqdqs).unwrap();
    binwriter.ingest(sec(40.1), 2., &mut iqdqs).unwrap();
    binwriter.ingest(sec(50.0), 2., &mut iqdqs).unwrap();
    binwriter.ingest(sec(50.1), 2., &mut iqdqs).unwrap();
    binwriter.ingest(sec(51.0), 2., &mut iqdqs).unwrap();
    binwriter.ingest(sec(51.1), 2., &mut iqdqs).unwrap();
    binwriter.ingest(sec(60.0), 2., &mut iqdqs).unwrap();
    // binwriter.ingest(sec(70.0), 2., &mut iqdqs).unwrap();
    // binwriter.ingest(sec(70.1), 2., &mut iqdqs).unwrap();
    binwriter.ingest(sec(80.0), 2., &mut iqdqs).unwrap();
    binwriter.tick(&mut iqdqs).unwrap();
    debug!("iqdqs summary {}", iqdqs.summary());
    for x in &iqdqs.st_rf3_qu {
        debug!("ST  {:?}", x);
    }
    for x in &iqdqs.mt_rf3_qu {
        debug!("MT  {:?}", x);
    }
    for x in &iqdqs.lt_rf3_qu {
        debug!("LT  {:?}", x);
    }
    {
        let rt = "ST";
        let binscol = collect_bin_write_only(&iqdqs.st_rf3_qu);
        print_binscol(rt, &binscol);
        let pbp = PrebinnedPartitioning::Sec1;
        let x = pbp.msp_lsp(T0);
        let msp_exp = T0.ms() / pbp.bin_len().ms() / pbp.patch_len() as u64;
        assert_eq!(x.0 as u64, msp_exp);
        let x = pbp.msp_lsp(sec(39.9).to_ts_ms());
        assert_eq!(binscol[0].msp as u32, x.0);
        assert_eq!(binscol[0].off as u32, x.1);
        let x = pbp.msp_lsp(sec(40.0).to_ts_ms());
        assert_eq!(binscol[1].msp as u32, x.0);
        assert_eq!(binscol[1].off as u32, x.1);
        assert_eq!(binscol[1].max, 2.0);
    }
    {
        let rt = "MT";
        let binscol = collect_bin_write_only(&iqdqs.mt_rf3_qu);
        print_binscol(rt, &binscol);
    }
    {
        let rt = "LT";
        let binscol = collect_bin_write_only(&iqdqs.lt_rf3_qu);
        print_binscol(rt, &binscol);
    }
    {
        let pbp = PrebinnedPartitioning::Sec10;
        debug!(
            "expect MT msp at T0 {}",
            T0.ms() / pbp.bin_len().ms() / pbp.patch_len() as u64
        );
    }
    log_v2_trace!("ARG-1 {}", 42);
}
