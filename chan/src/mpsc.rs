use crate::sync::{Condvar, Mutex};
use std::collections::VecDeque;
use std::sync::Arc;

#[derive(Clone)]
pub struct Sender<T> {
    chan: Arc<Chan<T>>,
}

impl<T> Sender<T> {
    pub fn send(&self, x: T) {
        let mut chan = self.chan.mu.lock().unwrap();
        if let Some(cap) = self.chan.capacity {
            while chan.data.len() >= cap {
                chan = self.chan.not_full.wait(chan).unwrap();
            }
        }
        chan.data.push_back(x);
        if chan.data.len() == 1 {
            self.chan.not_empty.notify_one();
        }
    }
}

pub struct Receiver<T> {
    chan: Arc<Chan<T>>,
    data: VecDeque<T>,
}

impl<T> Receiver<T> {
    pub fn try_recv(&mut self) -> Option<T> {
        self.recv_impl(false)
    }

    pub fn recv(&mut self) -> T {
        self.recv_impl(true).unwrap()
    }

    pub fn recv_impl(&mut self, wait: bool) -> Option<T> {
        if let Some(x) = self.data.pop_front() {
            return Some(x);
        }
        let mut chan = self.chan.mu.lock().unwrap();
        while chan.data.is_empty() {
            if !wait {
                return None;
            }
            chan = self.chan.not_empty.wait(chan).unwrap();
        }
        std::mem::swap(&mut chan.data, &mut self.data);
        drop(chan);
        if let Some(cap) = self.chan.capacity {
            if self.data.len() == cap {
                self.chan.not_full.notify_one();
            }
        }
        self.data.pop_front()
    }
}

struct Chan<T> {
    mu: Mutex<ChanInner<T>>,
    not_empty: Condvar,
    not_full: Condvar,
    capacity: Option<usize>,
}

pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    channel_with_capacity(None)
}

pub fn bounded_channel<T>(cap: usize) -> (Sender<T>, Receiver<T>) {
    channel_with_capacity(Some(cap))
}

fn channel_with_capacity<T>(capacity: Option<usize>) -> (Sender<T>, Receiver<T>) {
    let chan = Arc::new(Chan {
        mu: Mutex::new(ChanInner {
            data: Default::default(),
        }),
        not_empty: Default::default(),
        not_full: Default::default(),
        capacity,
    });
    let sender = Sender { chan: chan.clone() };
    let receiver = Receiver {
        chan,
        data: Default::default(),
    };
    (sender, receiver)
}

struct ChanInner<T> {
    data: VecDeque<T>,
}

#[cfg(all(test, not(loom)))]
mod single_threaded_tests {
    use super::*;

    #[test]
    fn send_and_try_recv() {
        let (tx, mut rx) = channel();
        tx.send(42);
        assert_eq!(rx.try_recv(), Some(42));
    }
}

#[cfg(all(test, loom))]
mod loom_concurrent_tests {
    use super::*;
    use crate::loom_utils::*;
    use loom::thread;

    #[test]
    fn send_and_try_recv() {
        let results = collect_all_outcomes(|| {
            let (tx, mut rx) = channel();
            thread::spawn(move || tx.send(42));
            rx.try_recv()
        });
        assert_eq!(results, &[None, Some(42)]);
    }

    #[test]
    fn send_and_recv() {
        loom::model(|| {
            let (tx, mut rx) = channel();
            thread::spawn(move || tx.send(42));
            assert_eq!(rx.recv(), 42);
        });
    }

    #[test]
    fn two_sends_and_recv() {
        let results = collect_all_outcomes(|| {
            let (tx, mut rx) = channel();
            let tx2 = tx.clone();
            thread::spawn(move || tx.send(42));
            thread::spawn(move || tx2.send(100));
            (rx.recv(), rx.recv())
        });
        assert_eq!(results, &[(42, 100), (100, 42)]);
    }

    #[test]
    fn bounded_two_sends_and_recv() {
        let results = collect_all_outcomes(|| {
            let (tx, mut rx) = bounded_channel(1);
            let tx2 = tx.clone();
            thread::spawn(move || tx.send(42));
            thread::spawn(move || tx2.send(100));
            (rx.recv(), rx.recv())
        });
        assert_eq!(results, &[(42, 100), (100, 42)]);
    }
}
