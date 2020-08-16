mod sync;

use sync::{AtomicUsize, Ordering::*};

pub struct Entry {
    buggy: bool,
    key: AtomicUsize,
    value: AtomicUsize,
}

impl Entry {
    pub fn new(buggy: bool) -> Self {
        Self {
            buggy,
            key: AtomicUsize::new(0),
            value: AtomicUsize::new(0),
        }
    }

    pub fn set(&self, key: usize, value: usize) {
        self.value.store(0, SeqCst);

        if !self.buggy {
            self.key.load(SeqCst);
        }

        self.key.store(key, SeqCst);
        self.value.store(value, SeqCst);
    }

    pub fn get(&self) -> Option<(usize, usize)> {
        loop {
            let key = self.key.load(SeqCst);
            let value = self.value.load(SeqCst);
            if value == 0 {
                return None;
            }
            if self.key.load(SeqCst) != key {
                continue;
            }
            return Some((key, value));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_buggy() {
        test(true);
    }

    #[test]
    fn test_non_buggy() {
        test(false);
    }

    fn test(buggy: bool) {
        loom::model(move || {
            let entry = Arc::new(Entry::new(buggy));
            let entry1 = entry.clone();
            let entry2 = entry.clone();

            entry.set(1, 101);

            let t1 = loom::thread::spawn(move || {
                entry1.set(2, 102);
                entry1.set(1, 101);
            });

            let t2 = loom::thread::spawn(move || match entry2.get() {
                Some((1, 101)) => {}
                Some((2, 102)) => {}
                None => {}
                Some((k, v)) => panic!("unknown kv pair: {:?}", (k, v)),
            });

            t1.join().unwrap();
            t2.join().unwrap();
        });
    }
}
