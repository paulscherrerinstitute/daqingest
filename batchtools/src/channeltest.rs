use async_channel::Receiver;
use async_channel::Sender;
use std::time::Instant;

#[test]
fn prod_cons() {
    let rt = taskrun::get_runtime();
    rt.block_on(run());
}

async fn run() {
    let n = 10_000_000;
    let ts1 = Instant::now();
    let (tx, rx) = async_channel::bounded(1000);
    let mut jhs = Vec::new();
    let jh = taskrun::spawn(consumer(rx.clone()));
    jhs.push(jh);
    let jh = taskrun::spawn(consumer(rx));
    jhs.push(jh);
    let jh = taskrun::spawn(producer(tx, n));
    jhs.push(jh);
    for jh in jhs {
        jh.await.unwrap();
    }
    let ts2 = Instant::now();
    let dt = ts2.duration_since(ts1);
    eprintln!("dt {:.3} MHz", n as f32 / dt.as_secs_f32() * 1e-6);
    panic!();
}

async fn producer(tx: Sender<Item>, n: u64) {
    for i in 0..n {
        let item = Item { x: i, y: i };
        tx.send(item).await.unwrap();
    }
}

async fn consumer(rx: Receiver<Item>) {
    while let Ok(_x) = rx.recv().await {}
}

struct Item {
    x: u64,
    y: u64,
}
