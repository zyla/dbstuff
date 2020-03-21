#[macro_use] extern crate assert_matches;
extern crate rand;

use buffer_pool::*;
use buffer_pool::disk_manager::*;

use tokio::fs;
use tokio::prelude::*;
use std::path::Path;
use std::collections::{HashMap};
use tokio::sync::{RwLock};
use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};
use std::ops::{Deref, DerefMut};
use std::future::Future;
use bitvec::vec::BitVec;


async fn with_temp_db<R, RF: Future<Output = Result<R>>, F: FnOnce(DiskManager) -> RF>(f: F) -> Result<R> {
    let filename = format!("test.db.{}", rand::random::<usize>());
    let disk_manager = DiskManager::open(&filename).await?;
    let result = f(disk_manager).await;
    fs::remove_file(filename).await?;
    result
}

#[tokio::test]
async fn test_allocate_and_read_one_page() {
    with_temp_db(|disk_manager| async {
        let buffer_pool = BufferPool::new(disk_manager, 1);
        let page = buffer_pool.allocate_page().await?;
        assert_eq!(page.id, PageId(0));
        page.data.write().await[0] = 5;

        let page = buffer_pool.get_page(PageId(0)).await?;
        assert_eq!(page.data.read().await[0], 5);

        Ok(())
    }).await.unwrap()
}

#[tokio::test]
async fn test_allocate_more_pages_than_capacity() {
    with_temp_db(|disk_manager| async {
        let buffer_pool = BufferPool::new(disk_manager, 1);
        let page0 = buffer_pool.allocate_page().await?;

        // Allocating second page should fail - we don't have free slots
        assert_matches!(buffer_pool.allocate_page().await, Err(Error::NoFreeFrames));

        drop(page0);

        // Now it should succeed, as we have unpinned the previous page
        buffer_pool.allocate_page().await?;

        Ok(())
    }).await.unwrap()
}

#[tokio::test]
async fn test_write_and_read_evicted_page() {
    with_temp_db(|disk_manager| async {
        let buffer_pool = BufferPool::new(disk_manager, 1);

        let page = buffer_pool.allocate_page().await?;
        assert_eq!(page.id, PageId(0));
        page.data.write().await[0] = 5;
        page.dirty();
        drop(page);

        // Allocate another page to evict the one we've written to
        buffer_pool.allocate_page().await?;
        assert!(!buffer_pool.is_page_in_memory(PageId(0)).await);

        let page = buffer_pool.get_page(PageId(0)).await?;
        assert_eq!(page.data.read().await[0], 5);

        Ok(())
    }).await.unwrap()
}
