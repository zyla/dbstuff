use loom::sync::atomic::{AtomicUsize, Ordering::*};
use std::hash::Hash;
use std::sync::Arc;

pub struct Entry {
    key: AtomicUsize,
    value: AtomicUsize,
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
        let entry = Arc::new(Entry {
            key: AtomicUsize::new(1),
            value: AtomicUsize::new(101),
        });
        let entry1 = entry.clone();
        let entry2 = entry.clone();

        let t1 = loom::thread::spawn(move || {
            entry1.value.store(0, SeqCst);
            entry1.key.store(2, SeqCst);
            entry1.value.store(102, SeqCst);

            entry1.value.store(0, SeqCst);

            if buggy {
                entry1.key.load(SeqCst);
            }

            entry1.key.store(1, SeqCst);
            entry1.value.store(101, SeqCst);
        });

        let t2 = loom::thread::spawn(move || loop {
            let key = entry2.key.load(SeqCst);
            let value = entry2.value.load(SeqCst);
            if entry2.key.load(SeqCst) != key {
                continue;
            }
            return (key, value);
        });

        t1.join().unwrap();
        t2.join().unwrap()
    });
    assert_eq!(results, vec![(1, 0), (1, 101), (1, 102), (2, 0), (2, 102)]);
}

/// Run all interleavings of the given function using Loom and return the sorted list of all
/// observed outcomes.
fn collect_all_outcomes<A: Hash + Ord + Send + 'static>(
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
