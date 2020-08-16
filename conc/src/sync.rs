#[cfg(loom)]
pub use loom::sync::*;

#[cfg(not(loom))]
pub use std::sync::*;

pub use atomic::*;
