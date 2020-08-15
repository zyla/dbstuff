use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering, AtomicBool};

pub trait Data: Copy + Eq {
    fn to_u64(self) -> u64;
    fn from_u64(value: u64) -> Self;
    fn sentinel() -> Self;
}

pub struct HashTable<K, V, H = FNV1> {
    data: Box<[Entry<K, V>]>,
    hasher: H,
}

struct Entry<K, V> {
    key: AtomicU64,
    value: AtomicU64,
    _phantom: PhantomData<(K, V)>,
}

impl<K: Data, V: Data> Entry<K, V> {
    fn empty() -> Self {
        Self {
            key: AtomicU64::new(K::sentinel().to_u64()),
            value: AtomicU64::new(V::sentinel().to_u64()),
            _phantom: PhantomData,
        }
    }
}

impl<K: Data, V: Data, H: Default + Hasher> HashTable<K, V, H> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_and_hasher(capacity, H::default())
    }
}

#[derive(PartialEq, Eq, Debug)]
pub enum InsertError<V> {
    AlreadyExists(V),
    TableFull,
}

impl<K: Data, V: Data, H: Hasher> HashTable<K, V, H> {
    pub fn with_capacity_and_hasher(capacity: usize, hasher: H) -> Self {
        assert!(is_power_of_2(capacity), "capacity not a power of two");
        Self {
            data: (0..capacity)
                .map(|_| Entry::<K, V>::empty())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            hasher,
        }
    }

    pub fn insert(&self, key: K, value: V) -> Result<(), InsertError<V>> {
        debug_assert!(key != K::sentinel());
        debug_assert!(value != V::sentinel());
        let h = self.hasher.hash(key.to_u64());
        let mut index = self.hash_to_index(h);
        let mut num_tries = 0;
        loop {
            let entry = &self.data[index];
            let k = entry.key.load(Ordering::SeqCst);
            if k == K::sentinel().to_u64() {
                if entry
                    .key
                    .compare_exchange_weak(k, key.to_u64(), Ordering::SeqCst, Ordering::SeqCst)
                    .is_err()
                {
                    // Someone already inserted something here - try again
                    continue;
                }
                match entry.value.compare_exchange_weak(
                    V::sentinel().to_u64(),
                    value.to_u64(),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => return Ok(()),
                    Err(existing_value) => {
                        // Someone raced with us and inserted something else into the "deleted
                        // entry"
                        return Err(InsertError::AlreadyExists(V::from_u64(existing_value)));
                    }
                }
            } else if k == key.to_u64() {
                // No overwrites - only insert if it's empty
                match entry.value.compare_exchange_weak(
                    V::sentinel().to_u64(),
                    value.to_u64(),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => return Ok(()),
                    Err(existing_value) => {
                        // Someone raced with us and inserted something else into the "deleted
                        // entry"
                        return Err(InsertError::AlreadyExists(V::from_u64(existing_value)));
                    }
                }
            } else {
                num_tries += 1;
                if num_tries > self.data.len() {
                    return Err(InsertError::TableFull);
                }
                index = (index + 1) & (self.data.len() - 1);
            }
        }
    }

    pub fn delete(&self, key: K) -> Option<V> {
        debug_assert!(key != K::sentinel());
        let h = self.hasher.hash(key.to_u64());
        let mut index = self.hash_to_index(h);
        let mut num_tries = 0;
        loop {
            let entry = &self.data[index];
            let k = entry.key.load(Ordering::SeqCst);
            if k == K::sentinel().to_u64() {
                return None;
            } else if k == key.to_u64() {
                // Don't break the chain - only replace value
                let v = entry.value.load(Ordering::SeqCst);
                if v == V::sentinel().to_u64() {
                    return None;
                }
                match entry.value.compare_exchange_weak(
                    v,
                    V::sentinel().to_u64(),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => {
                        return Some(V::from_u64(v));
                    }
                    Err(_) => {
                        // Something changed - recheck
                        continue;
                    }
                }
            } else {
                num_tries += 1;
                if num_tries > self.data.len() {
                    return None;
                }
                index = (index + 1) & (self.data.len() - 1);
            }
        }
    }

    pub fn lookup(&self, key: K) -> Option<V> {
        debug_assert!(key != K::sentinel());
        let h = self.hasher.hash(key.to_u64());
        let mut index = self.hash_to_index(h);
        let mut num_tries = 0;
        loop {
            let entry = &self.data[index];
            let k = entry.key.load(Ordering::SeqCst);

            // Buggify
            std::thread::sleep_ms(1);

            if k == K::sentinel().to_u64() {
                return None;
            } else if k == key.to_u64() {
                // Don't break the chain - only replace value
                let v = entry.value.load(Ordering::SeqCst);
                if v == V::sentinel().to_u64() {
                    return None;
                } else {
                    // FIXME: we should probably recheck the key here - otherwise how do we know
                    // we're not reading value for a different key?
                    return Some(V::from_u64(v));
                }
            } else {
                num_tries += 1;
                if num_tries > self.data.len() {
                    return None;
                }
                index = (index + 1) & (self.data.len() - 1);
            }
        }
    }

    fn hash_to_index(&self, h: u64) -> usize {
        h as usize & (self.data.len() - 1)
    }
}

fn is_power_of_2(x: usize) -> bool {
    x & x.wrapping_sub(1) == 0
}

pub trait Hasher {
    fn hash(&self, x: u64) -> u64;
}

#[derive(Default)]
pub struct FNV1;

impl Hasher for FNV1 {
    // FNV-1 for little-endian representation of the value
    fn hash(&self, x: u64) -> u64 {
        let mut h: u64 = 0xcbf29ce484222325;
        for i in 0..8 {
            h = (h ^ ((x >> (i * 8)) & 0xff)).wrapping_mul(0x100000001b3);
        }
        h
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use InsertError::*;
    use crossbeam_utils::thread;
    use std::collections::{HashMap as StdHashMap};
    use rand::Rng;

    // Identity hash for testing
    #[derive(Default)]
    struct TestHash;

    impl Hasher for TestHash {
        fn hash(&self, x: u64) -> u64 {
            x & 0xff
        }
    }

    #[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
    struct X(u64);

    impl Data for X {
        fn to_u64(self) -> u64 {
            self.0
        }
        fn from_u64(value: u64) -> Self {
            Self(value)
        }
        fn sentinel() -> Self {
            Self(0)
        }
    }

    #[test]
    fn test_whitebox_insert_1() {
        let table = HashTable::<X, X, TestHash>::with_capacity(8);
        assert_eq!(table.insert(X(1), X(100)), Ok(()));
        assert_eq!(table.data[0].key.load(Ordering::SeqCst), 0);
        assert_eq!(table.data[0].value.load(Ordering::SeqCst), 0);
        assert_eq!(table.data[1].key.load(Ordering::SeqCst), 1);
        assert_eq!(table.data[1].value.load(Ordering::SeqCst), 100);
        assert_eq!(table.data[2].key.load(Ordering::SeqCst), 0);
        assert_eq!(table.data[2].value.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_whitebox_insert_collision() {
        let table = HashTable::<X, X, TestHash>::with_capacity(8);
        assert_eq!(table.insert(X(1), X(100)), Ok(()));
        assert_eq!(table.insert(X(0x101), X(101)), Ok(()));
        assert_eq!(table.data[0].key.load(Ordering::SeqCst), 0);
        assert_eq!(table.data[0].value.load(Ordering::SeqCst), 0);
        assert_eq!(table.data[1].key.load(Ordering::SeqCst), 1);
        assert_eq!(table.data[1].value.load(Ordering::SeqCst), 100);
        assert_eq!(table.data[2].key.load(Ordering::SeqCst), 0x101);
        assert_eq!(table.data[2].value.load(Ordering::SeqCst), 101);
    }

    #[test]
    fn test_whitebox_delete_1() {
        let table = HashTable::<X, X, TestHash>::with_capacity(8);
        assert_eq!(table.insert(X(1), X(100)), Ok(()));
        assert_eq!(table.delete(X(1)), Some(X(100)));
        assert_eq!(table.data[0].key.load(Ordering::SeqCst), 0);
        assert_eq!(table.data[0].value.load(Ordering::SeqCst), 0);
        assert_eq!(table.data[1].key.load(Ordering::SeqCst), 1);
        assert_eq!(table.data[1].value.load(Ordering::SeqCst), 0);
        assert_eq!(table.data[2].key.load(Ordering::SeqCst), 0);
        assert_eq!(table.data[2].value.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_lookup_not_exists() {
        let table = HashTable::<X, X, TestHash>::with_capacity(8);
        assert_eq!(table.lookup(X(1)), None);
    }

    #[test]
    fn test_insert_not_exists() {
        let table = HashTable::<X, X, TestHash>::with_capacity(8);
        assert_eq!(table.insert(X(1), X(10)), Ok(()));
        assert_eq!(table.lookup(X(1)), Some(X(10)));
    }

    #[test]
    fn test_insert_exists() {
        let table = HashTable::<X, X, TestHash>::with_capacity(8);
        assert_eq!(table.insert(X(1), X(10)), Ok(()));
        assert_eq!(table.insert(X(1), X(12)), Err(AlreadyExists(X(10))));
        assert_eq!(table.lookup(X(1)), Some(X(10)));
    }

    #[test]
    fn test_lookup_not_exists_collision() {
        let table = HashTable::<X, X, TestHash>::with_capacity(8);
        assert_eq!(table.insert(X(1), X(10)), Ok(()));
        assert_eq!(table.lookup(X(0x101)), None);
    }

    #[test]
    fn test_insert_delete_lookup() {
        let table = HashTable::<X, X, TestHash>::with_capacity(8);
        assert_eq!(table.insert(X(1), X(10)), Ok(()));
        assert_eq!(table.delete(X(1)), Some(X(10)));
        assert_eq!(table.lookup(X(1)), None);
    }
    
    #[derive(Default)]
    struct BadHash;

    impl Hasher for BadHash {
        fn hash(&self, x: u64) -> u64 {
            0 //x & 0xf0
        }
    }

    #[test]
    fn test_threaded_insert_lookup() {
        const ITERATIONS: usize = 100_000;
        const SIZE: usize = 128;
        let table = HashTable::<X, X, BadHash>::with_capacity(SIZE);
        let finished = AtomicBool::new(false);
        thread::scope(|s| {
            s.spawn(|_| {
                let mut rng = rand::thread_rng();
                let mut local = StdHashMap::with_capacity(SIZE);

                for _ in 0..ITERATIONS {
                    let k = X(rng.gen_range(1, SIZE as u64 + 1));
                    if local.contains_key(&k) {
                        local.remove(&k);
                        assert_eq!(table.delete(k), Some(k));
                    } else {
                        local.insert(k, k);
                        assert_eq!(table.insert(k, k), Ok(()));
                    }
                }
                finished.store(true, Ordering::SeqCst);
            });
            s.spawn(|_| {
                let mut rng = rand::thread_rng();
                let mut num_successes = 0;

                while !finished.load(Ordering::Relaxed) {
                    let k = X(rng.gen_range(1, SIZE as u64 + 1));
                    match table.lookup(k) {
                        Some(value) => {
                            assert_eq!(value, k);
                            num_successes += 1;
                        }
                        None => {}
                    }
                }

                println!("num_successes={}", num_successes);
            });
        }).unwrap();
    }

    // FIXME why isn't this failing?
    #[test]
    fn test_threaded_alternating_values() {
        const ITERATIONS: usize = 100_000;
        const SIZE: usize = 2;
        let table = HashTable::<X, X, BadHash>::with_capacity(SIZE);
        let finished = AtomicBool::new(false);
        thread::scope(|s| {
            s.spawn(|_| {
                let mut current_k = 0;

                for _ in 0..ITERATIONS {
                    let k = X(current_k + 100);
                    table.insert(k, k).unwrap();
                    table.delete(k).unwrap();
                    current_k = (current_k + 1) & 1;
                }
                finished.store(true, Ordering::SeqCst);
            });
            s.spawn(|_| {
                let mut num_successes = 0;
                let mut current_k = 0;

                while !finished.load(Ordering::Relaxed) {
                    let k = X(current_k + 100);
                    match table.lookup(k) {
                        Some(value) => {
                            assert_eq!(value, k);
                            num_successes += 1;
                        }
                        None => {}
                    }
                    current_k = (current_k + 1) & 1;
                }

                println!("num_successes={}", num_successes);
            });
        }).unwrap();
    }
}
