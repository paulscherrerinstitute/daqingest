use super::BinWriter;
use crate::rtwriter::MinQuiets;
use netpod::ScalarType;
use netpod::Shape;
use netpod::TsNano;
use scywr::insertqueues::InsertDeques;
use series::ChannelStatusSeriesId;
use series::SeriesId;

fn sec(sec: f32) -> TsNano {
    TsNano::from_ms((1e3 * sec) as u64)
}

#[test]
fn binwriter_nest01_00() {
    let beg = TsNano::from_ms(1000 * 40);
    let min_quiets = MinQuiets::test_1_10_60();
    let is_polled = false;
    let cssid = ChannelStatusSeriesId::new(50);
    let sid = SeriesId::new(51);
    let scalar_type = ScalarType::F32;
    let shape = Shape::Scalar;
    let chname2 = String::from("DUMMY");
    let mut iqdqs = InsertDeques::new();
    let mut binwriter = BinWriter::new(beg, min_quiets, is_polled, cssid, sid, scalar_type, shape, chname2).unwrap();
    binwriter.ingest(sec(39.9), 2., &mut iqdqs).unwrap();
    binwriter.ingest(sec(40.0), 2., &mut iqdqs).unwrap();
    binwriter.ingest(sec(40.1), 2., &mut iqdqs).unwrap();
    binwriter.ingest(sec(50.0), 2., &mut iqdqs).unwrap();
    binwriter.ingest(sec(60.0), 2., &mut iqdqs).unwrap();
    // binwriter.ingest(sec(70.0), 2., &mut iqdqs).unwrap();
    binwriter.tick(&mut iqdqs).unwrap();
    eprintln!("iqdqs summary {}", iqdqs.summary());
    for x in iqdqs.st_rf3_qu {
        eprintln!("ST  {:?}", x);
    }
    for x in iqdqs.mt_rf3_qu {
        eprintln!("MT  {:?}", x);
    }
    for x in iqdqs.lt_rf3_qu {
        eprintln!("LT  {:?}", x);
    }
}
