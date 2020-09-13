#[cfg(loom)]
pub use loom::sync::{
    atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering},
    Condvar, Mutex,
};

#[cfg(not(loom))]
pub use std::sync::{
    atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering},
    Condvar, Mutex,
};
