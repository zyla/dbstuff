use loom::sync::atomic::{AtomicUsize, Ordering::*};
use std::hash::Hash;
use std::sync::Arc;

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

        if self.buggy {
            self.key.load(SeqCst);
        }

        self.key.store(key, SeqCst);
        self.value.store(value, SeqCst);
    }

    pub fn get(&self) -> (usize, usize) {
        loop {
            let key = self.key.load(SeqCst);
            let value = self.value.load(SeqCst);
            if self.key.load(SeqCst) != key {
                continue;
            }
            return (key, value);
        }
    }
}

fn collect_all_outcomes<A: Hash + Ord + std::marker::Send + 'static>(
    f: impl Fn() -> A + Sync + Send + 'static,
) -> Vec<A> {
    use std::collections::HashSet;
    use std::sync::Mutex;
    let result_set: Arc<Mutex<HashSet<A>>> = Arc::new(Mutex::new(HashSet::new()));
    let result_set_2 = result_set.clone();
    loom::model(move || {
        let result = f();
        result_set.lock().unwrap().insert(result);
    });
    let mut results = result_set_2.lock().unwrap().drain().collect::<Vec<_>>();
    results.sort();
    results
}

#[test]
fn test_buggy() {
    test(true);
}

#[test]
fn test_non_buggy() {
    test(false);
}

fn test(buggy: bool) {
    let results = collect_all_outcomes(move || {
        let entry = Arc::new(Entry::new(buggy));
        let entry1 = entry.clone();
        let entry2 = entry.clone();

        entry.set(1, 101);

        let t1 = loom::thread::spawn(move || {
            entry1.set(2, 102);
            entry1.set(1, 101);
        });

        let t2 = loom::thread::spawn(move || entry2.get());

        t1.join().unwrap();
        t2.join().unwrap()
    });
    assert_eq!(
        results,
        vec![(1, 0), (1, 101), (1, 102), (2, 0), (2, 102)]
    );
}
