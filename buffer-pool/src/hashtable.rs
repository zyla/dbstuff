use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};

pub trait Data: Copy {
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

    // Identity hash for testing
    #[derive(Default)]
    struct TestHash;

    impl Hasher for TestHash {
        fn hash(&self, x: u64) -> u64 {
            x & 0xff
        }
    }

    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
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

    #[test]
    fn test_threaded_insert_lookup() {
        // TODO
    }
}
