#![cfg(loom)]

use loom::sync::atomic::{AtomicU64, Ordering::*};

struct Entry {
    key: AtomicU64,
    value: AtomicU64,
}

const EMPTY: u64 = 0xffff_ffff_ffff_ffff;

impl Entry {
    fn new() -> Self {
        Entry {
            key: AtomicU64::new(EMPTY),
            value: AtomicU64::new(EMPTY),
        }
    }

    /// Set value for the given key, if entry is empty.
    /// If the entry is already set, returns Err.
    fn set(&self, key: u64, value: u64) -> Result<(), ()> {
        if self.key.compare_exchange(EMPTY, key, SeqCst, SeqCst) {
            self.value.store(value, SeqCst);
            Ok(())
        } else {
            Err(())
        }
    }

    /// Delete value for the given key.
    /// If the entry is empty or contains a different key, returns Err. Otherwise returns value for
    /// the key.
    fn delete(&self, key: u64) -> Result<u64, ()> {
        if self.key.compare_exchange(key, EMPTY, SeqCst, SeqCst) {
            Ok(self.value.load(SeqCst))
        } else {
            Err(())
        }
    }

    /// Get value for the given key, or None if it's not the key stored in the entry.
    fn get(&self, key: u64) -> Option<u64> {
        if self.key.load(SeqCst) == key {
            Some(self.value.load(SeqCst))
        } else {
            None
        }
    }
}
