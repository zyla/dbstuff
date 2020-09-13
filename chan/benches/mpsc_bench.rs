#![feature(test)]
extern crate test;

use ::chan::mpsc;

#[bench]
fn send_recv_unbounded(b: &mut test::bench::Bencher) {
    b.iter(|| {
        let (tx, mut rx) = mpsc::channel();
        std::thread::spawn(move || {
            for i in 1..=1000 {
                tx.send(i);
            }
        });
        while rx.recv() < 1000 {}
    })
}

#[bench]
fn send_recv_bounded(b: &mut test::bench::Bencher) {
    b.iter(|| {
        let (tx, mut rx) = mpsc::bounded_channel(100);
        std::thread::spawn(move || {
            for i in 1..=1000 {
                tx.send(i);
            }
        });
        while rx.recv() < 1000 {}
    })
}
