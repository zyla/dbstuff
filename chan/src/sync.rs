#[cfg(loom)]
pub use loom::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    Condvar, Mutex,
};

#[cfg(not(loom))]
pub use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    Condvar, Mutex,
};
