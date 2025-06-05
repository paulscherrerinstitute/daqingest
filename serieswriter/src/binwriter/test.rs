use super::BinWriter;
use crate::binwriter::DiscardFirstOutput;
use crate::binwriter::WriteCntZero;
use crate::rtwriter::MinQuiets;
use log;
use netpod::DtMs;
use netpod::ScalarType;
use netpod::Shape;
use netpod::TsMs;
use netpod::TsNano;
use netpod::f32_close;
use scywr::insertqueues::InsertDeques;
use scywr::iteminsertqueue::QueryItem;
use scywr::iteminsertqueue::TimeBinSimpleF32V02;
use series::ChannelStatusSeriesId;
use series::SeriesId;
use series::msp::LspU32;
use series::msp::MspU32;
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

macro_rules! trace_binscol {
    ($($arg:tt)*) => { if false { log::trace!($($arg)*); } };
}

macro_rules! trace_iqdqs {
    ($($arg:tt)*) => { if false { log::trace!($($arg)*); } };
}

macro_rules! error_cmp {
    ($($arg:tt)*) => { if false { log::error!($($arg)*); } };
}

macro_rules! take_par {
    ($fmt:expr, $($args:expr),*) => {};
}

struct BinsExp {
    pbp: PrebinnedPartitioning,
    curmsp: MspU32,
    curlsp: LspU32,
    cnt_zero_default: WriteCntZero,
    msp: VecDeque<u32>,
    lsp: VecDeque<u32>,
    cnt: VecDeque<Option<u32>>,
    min: VecDeque<Option<f32>>,
    max: VecDeque<Option<f32>>,
}

impl BinsExp {
    fn new(pbp: PrebinnedPartitioning, msp: MspU32, lsp: LspU32, cnt_zero_default: WriteCntZero) -> Self {
        Self {
            pbp,
            curmsp: msp,
            curlsp: lsp,
            cnt_zero_default,
            msp: def(),
            lsp: def(),
            cnt: def(),
            min: def(),
            max: def(),
        }
    }

    fn len(&self) -> usize {
        self.msp.len()
    }

    fn inc_lsp(&mut self) {
        let (m2, l2) = self.pbp.lsp_inc(self.curmsp, self.curlsp);
        self.curmsp = m2;
        self.curlsp = l2;
    }

    fn skip_lsp(&mut self, n: u32) {
        for _ in 0..n {
            self.inc_lsp();
        }
    }

    fn push_back_dont_care(&mut self) {
        self.msp.push_back(self.curmsp.0);
        self.lsp.push_back(self.curlsp.0);
        self.cnt.push_back(None);
        self.min.push_back(None);
        self.max.push_back(None);
        self.inc_lsp();
    }

    fn push_back_cnt(&mut self, cnt: u32) {
        self.msp.push_back(self.curmsp.0);
        self.lsp.push_back(self.curlsp.0);
        self.cnt.push_back(Some(cnt));
        self.min.push_back(None);
        self.max.push_back(None);
        self.inc_lsp();
    }

    fn push_back_cmm(&mut self, cnt: u32, min: f32, max: f32) {
        self.msp.push_back(self.curmsp.0);
        self.lsp.push_back(self.curlsp.0);
        self.cnt.push_back(Some(cnt));
        self.min.push_back(Some(min));
        self.max.push_back(Some(max));
        self.inc_lsp();
    }

    fn push_cnt_zero(&mut self, n: u32, lst: f32) {
        if self.cnt_zero_default.enabled() {
            for _ in 0..n {
                self.push_back_cmm(0, lst, lst);
            }
        } else {
            self.skip_lsp(n);
        }
    }

    fn cmp(&self, bins: &VecDeque<TimeBinSimpleF32V02>) -> Result<(), ()> {
        let mut bad = false;
        for i in 0..self.msp.len() {
            let bin = if let Some(x) = bins.get(i) {
                x
            } else {
                break;
            };
            let msp = self.msp[i];
            let lsp = self.lsp[i];
            let cnt = self.cnt[i];
            let min = self.min[i];
            let max = self.max[i];
            if bin.msp as u32 != msp {
                bad = true;
                error_cmp!("i {i:3}  bad msp  {}  vs  {}", bin.msp, msp);
            }
            if bin.off as u32 != lsp {
                bad = true;
                error_cmp!("i {i:3}  bad lsp  {}  vs  {}", bin.off, lsp);
            }
            if let Some(cnt) = cnt {
                if bin.cnt as u32 != cnt {
                    bad = true;
                    error_cmp!("i {i:3}  bad cnt  {}  vs  {}", bin.cnt, cnt);
                }
            }
            if let Some(min) = min {
                if !f32_close(bin.min, min) {
                    bad = true;
                    error_cmp!("i {i:3}  bad min  {:.5e}  vs  {:.5e}", bin.min, min);
                }
            }
            if let Some(max) = max {
                if !f32_close(bin.max, max) {
                    bad = true;
                    error_cmp!("i {i:3}  bad max  {:.5e}  vs  {:.5e}", bin.max, max);
                }
            }
        }
        if self.len() > bins.len() {
            bad = true;
            error_cmp!("less bins than expected  {} vs {}", bins.len(), self.len());
        }
        if self.len() < bins.len() {
            bad = true;
            error_cmp!("more bins than expected  {} vs {}", bins.len(), self.len());
        }
        if bad { Err(()) } else { Ok(()) }
    }
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
        trace_binscol!(
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

#[allow(unused)]
fn format_check() {
    let _ = format_args!("");
    let _ = format_args!(concat!("", ""));
    debug_item!("plain fmt string");
    debug_item!("plain fmt string with comma");
    debug_item!("param as param: {:?}", "some--string123");
    let some_string = "some--string123";
    debug_item!("param interpolated: {some_string:?}");
    debug_item!("param interpolated: {some_string}");
    take_par!("fmtstring {par}", par = 123);
    log::direct_trace!("fmtstring {par}", par = 123);
    log::trace!("fmtstring {par}", par = 123);
    log::trace!("fmtstring {par}", par = 123,);
    log::trace!("fmtstring {}", 123);
    log::trace!("fmtstring {}", 123,);
}

fn binwriter_nest01_00_case(cnt_zero_default: WriteCntZero, do_discard_front: DiscardFirstOutput) {
    let _ = PrebinnedPartitioning::Day1;
    let beg = TsNano::from_ms(1000 * 20);
    let min_quiets = MinQuiets::test_1_10_60();
    let is_polled = false;
    let cssid = ChannelStatusSeriesId::new(50);
    let sid = SeriesId::new(51);
    let scalar_type = ScalarType::F32;
    let shape = Shape::Scalar;
    let chname2 = String::from("daqbuftest");
    let mut iqdqs = InsertDeques::new();
    let mut binwriter = BinWriter::new(
        beg,
        min_quiets,
        is_polled,
        cnt_zero_default.clone(),
        do_discard_front,
        cssid,
        sid,
        scalar_type,
        shape,
        chname2,
    )
    .unwrap();
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
    // TODO signal input done range final
    binwriter.ingest(sec(120.0), 0., &mut iqdqs).unwrap();
    binwriter.ingest(sec(121.0), 0., &mut iqdqs).unwrap();
    binwriter.tick(&mut iqdqs).unwrap();
    trace_iqdqs!("iqdqs summary {}", iqdqs.summary());
    for x in &iqdqs.st_rf3_qu {
        trace_iqdqs!("ST  {:?}", x);
    }
    for x in &iqdqs.mt_rf3_qu {
        trace_iqdqs!("MT  {:?}", x);
    }
    for x in &iqdqs.lt_rf3_qu {
        trace_iqdqs!("LT  {:?}", x);
    }
    {
        let rt = "ST";
        let binscol = collect_bin_write_only(&iqdqs.st_rf3_qu);
        print_binscol(rt, &binscol);
        let pbp = PrebinnedPartitioning::Sec1;
        let x = pbp.msp_lsp(T0);
        let msp_exp = T0.ms() / pbp.bin_len().ms() / pbp.patch_len() as u64;
        assert_eq!(x.0 as u64, msp_exp);
        {
            let (msp, lsp) = if do_discard_front.enabled() {
                pbp.msp_lsp(sec(40.0).to_ts_ms())
            } else {
                pbp.msp_lsp(sec(39.0).to_ts_ms())
            };
            let mut exp = BinsExp::new(pbp.clone(), MspU32(msp), LspU32(lsp), cnt_zero_default);
            // exp.push_back_cnt(2);
            if do_discard_front.enabled() {
            } else {
                exp.push_back_cnt(1);
            }
            exp.push_back_cmm(2, 2.0, 2.0);
            if cnt_zero_default.enabled() {
                for _ in 0..9 {
                    exp.push_back_dont_care();
                }
            } else {
                exp.skip_lsp(9);
            }
            exp.push_back_cmm(2, 2.0, 2.0);
            exp.push_back_cmm(2, 2.0, 2.0);
            exp.push_cnt_zero(8, 2.0);
            exp.push_back_cmm(1, 2.0, 2.0);
            exp.push_cnt_zero(19, 2.0);
            exp.push_back_cmm(1, 2.0, 2.0);
            exp.push_cnt_zero(39, 2.0);
            assert_eq!(exp.curlsp.0, 120);
            exp.push_back_cmm(1, 0., 0.);
            exp.cmp(&binscol).unwrap();
        }
    }
    {
        let rt = "MT";
        let binscol = collect_bin_write_only(&iqdqs.mt_rf3_qu);
        print_binscol(rt, &binscol);
        let pbp = PrebinnedPartitioning::Sec10;
        let x = pbp.msp_lsp(T0);
        let msp_exp = T0.ms() / pbp.bin_len().ms() / pbp.patch_len() as u64;
        assert_eq!(x.0 as u64, msp_exp);
        {
            let (msp, lsp) = if do_discard_front.enabled() {
                pbp.msp_lsp(sec(40.0).to_ts_ms())
            } else {
                pbp.msp_lsp(sec(30.0).to_ts_ms())
            };
            let mut exp = BinsExp::new(pbp.clone(), MspU32(msp), LspU32(lsp), cnt_zero_default);
            if do_discard_front.enabled() {
            } else {
                exp.push_back_cmm(1, 2.2, 2.2);
            }
            exp.push_back_cmm(2, 2.0, 2.0);
            exp.push_back_cmm(4, 2.0, 2.0);
            // assert 60s
            assert_eq!(exp.curlsp.0, 6);
            exp.push_back_cmm(1, 2.0, 2.0);
            exp.push_cnt_zero(1, 2.0);
            assert_eq!(exp.curlsp.0, 8);
            exp.push_back_cmm(1, 2.0, 2.0);
            exp.push_cnt_zero(3, 2.0);
            exp.cmp(&binscol).unwrap();
        }
    }
    {
        let rt = "LT";
        let binscol = collect_bin_write_only(&iqdqs.lt_rf3_qu);
        print_binscol(rt, &binscol);
        let pbp = PrebinnedPartitioning::Min1;
        let x = pbp.msp_lsp(T0);
        let msp_exp = T0.ms() / pbp.bin_len().ms() / pbp.patch_len() as u64;
        assert_eq!(x.0 as u64, msp_exp);
    }
}

#[test]
fn binwriter_nest01_00_ed() {
    binwriter_nest01_00_case(WriteCntZero::Enable, DiscardFirstOutput::Disable);
}

#[test]
fn binwriter_nest01_00_ee() {
    binwriter_nest01_00_case(WriteCntZero::Enable, DiscardFirstOutput::Enable);
}

#[test]
fn binwriter_nest01_00_dd() {
    binwriter_nest01_00_case(WriteCntZero::Disable, DiscardFirstOutput::Disable);
}

#[test]
fn binwriter_nest01_00_de() {
    binwriter_nest01_00_case(WriteCntZero::Disable, DiscardFirstOutput::Enable);
}
