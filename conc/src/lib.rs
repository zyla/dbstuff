use loom::sync::atomic::{AtomicUsize, Ordering::*};
use std::hash::Hash;
use std::sync::Arc;

pub struct Entry {
    key: AtomicUsize,
    value: AtomicUsize,
}

#[test]
fn test_outcomes_should_be_the_same() {
    assert_eq!(run_example(false), run_example(true));
}

fn run_example(with_extra_load: bool) -> Vec<(usize, usize)> {
    println!("run_example(with_extra_load={})", with_extra_load);
    collect_all_outcomes(move || {
        let entry = Arc::new(Entry {
            key: AtomicUsize::new(1),
            value: AtomicUsize::new(0),
        });
        let entry1 = entry.clone();
        let entry2 = entry.clone();

        let t1 = loom::thread::spawn(move || {
            entry1.value.store(1, SeqCst);
            entry1.value.store(0, SeqCst);

            if with_extra_load {
                entry1.key.load(SeqCst);
            }

            entry1.key.store(0, SeqCst);
        });

        let t2 = loom::thread::spawn(move || loop {
            let value = entry2.value.load(SeqCst);
            let key = entry2.key.load(SeqCst);
            return (value, key);
        });

        t1.join().unwrap();
        t2.join().unwrap()
    })
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
