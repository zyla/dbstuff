use buffer_pool::disk_manager::PageData;
use std::ops::{Deref, DerefMut};

pub struct TablePage<T> {
    data: T,
}

impl<T: Deref<Target=PageData>> TablePage<T> {
    /// Unsafe: It is the caller's responsibility to ensure that `data` is formatted like a table
    /// page.
    pub unsafe fn from_existing(data: T) -> Self {
        TablePage { data }
    }
}

impl<T: DerefMut<Target=PageData>> TablePage<T> {
    /// Initialize a new page in the given storage.
    pub fn new(data: T) -> Self {
    }
}
