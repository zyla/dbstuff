use std::collections::HashSet;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::Mutex;

/// Run all interleavings of the given function using Loom and return the sorted list of all
/// observed outcomes.
pub fn collect_all_outcomes<A: Hash + Ord + Send + 'static>(
    f: impl Fn() -> A + Sync + Send + 'static,
) -> Vec<A> {
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
