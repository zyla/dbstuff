use crate::sync::{AtomicPtr, Condvar, Mutex, Ordering::*};
use std::collections::VecDeque;
use std::ptr;
use std::sync::Arc;

#[derive(Clone)]
pub struct Sender<T> {
    chan: Arc<Chan<T>>,
}

impl<T> Sender<T> {
    pub fn send(&self, value: T) {
        let node = Box::into_raw(Box::new(Node {
            value,
            next: ptr::null_mut(),
        }));
        loop {
            let next = self.chan.head.load(Relaxed);
            unsafe { &mut *node }.next = next;
            let _guard = if next.is_null() {
                Some(self.chan.mu.lock().unwrap())
            } else {
                None
            };
            if self
                .chan
                .head
                .compare_exchange(next, node, Release, Relaxed)
                .is_ok()
            {
                if next.is_null() {
                    self.chan.not_empty.notify_one();
                }
                break;
            }
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
        let mut head = self.chan.head.swap(ptr::null_mut(), Acquire);
        if wait && head.is_null() {
            let mut guard = self.chan.mu.lock().unwrap();
            head = self.chan.head.swap(ptr::null_mut(), Acquire);
            while head.is_null() {
                guard = self.chan.not_empty.wait(guard).unwrap();
                head = self.chan.head.swap(ptr::null_mut(), Acquire);
            }
        }
        while !head.is_null() {
            let node = unsafe { Box::from_raw(head) };
            head = node.next;
            self.data.push_back(node.value);
        }
        self.data.pop_front()
    }
}

struct Chan<T> {
    head: AtomicPtr<Node<T>>,
    mu: Mutex<()>,
    not_empty: Condvar,
}

pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let chan = Arc::new(Chan {
        head: AtomicPtr::new(ptr::null_mut()),
        mu: Default::default(),
        not_empty: Default::default(),
    });
    let sender = Sender { chan: chan.clone() };
    let receiver = Receiver {
        chan,
        data: Default::default(),
    };
    (sender, receiver)
}

struct Node<T> {
    value: T,
    next: *mut Node<T>,
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
    #[ignore = "Buggy"]
    fn two_sends_and_recv() {
        let results = collect_all_outcomes(|| {
            let (tx, mut rx) = channel();
            let tx2 = tx.clone();
            let t1 = thread::spawn(move || tx.send(42));
            let t2 = thread::spawn(move || tx2.send(100));
            let result = (rx.recv(), rx.recv());
            t1.join().unwrap();
            t2.join().unwrap();
            result
        });
        assert_eq!(results, &[(42, 100), (100, 42)]);
    }
}
