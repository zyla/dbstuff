use crate::sync::{Condvar, Mutex};
use std::collections::VecDeque;
use std::sync::Arc;

pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let chan = Arc::new(Chan {
        mu: Mutex::new(ChanInner {
            data: Default::default(),
        }),
    });
    let sender = Sender { chan: chan.clone() };
    let receiver = Receiver { chan };
    (sender, receiver)
}

pub struct Sender<T> {
    chan: Arc<Chan<T>>,
}

pub struct Receiver<T> {
    chan: Arc<Chan<T>>,
}

struct Chan<T> {
    mu: Mutex<ChanInner<T>>,
}

struct ChanInner<T> {
    data: VecDeque<T>,
}

#[cfg(test)]
mod tests {}
