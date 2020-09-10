#[cfg(loom)]
pub use loom::sync::{
    atomic::{AtomicUsize, AtomicU64, AtomicBool, Ordering},
    Mutex,
};

#[cfg(not(loom))]
pub use std::sync::{
    atomic::{AtomicUsize, AtomicU64, AtomicBool, Ordering},
    Mutex,
};
