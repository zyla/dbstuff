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
use rand::{SeedableRng, Rng};
use std::collections::HashSet;

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


#[tokio::test]
async fn random_multi_pin_test() {
    with_temp_db(|disk_manager| async {
        const buffer_pool_size: usize = 2;
        const num_pages: usize = 4;

        let buffer_pool = BufferPool::new(disk_manager, buffer_pool_size);

        let mut rng = rand::rngs::StdRng::from_seed([0; 32]);

        for _ in 0..num_pages {
            let page = buffer_pool.allocate_page().await?;
            page.dirty();
        }

        let mut values = Box::new([0u8; num_pages]);
        let mut pinned_pages: Vec<PinnedPage> = Vec::new();

        fn num_unique_pinned_pages(pinned_pages: &Vec<PinnedPage>) -> usize {
            pinned_pages.iter().map(|page| page.id).collect::<HashSet<_>>().len()
        }

        println!("Begin test");

        for _ in 0..10000usize {
            let should_unpin =
                    if pinned_pages.len() == 0 {
                        false
                    } else if num_unique_pinned_pages(&pinned_pages) >= buffer_pool_size {
                        true
                    } else {
                        rng.gen()
                    };

            let page =
                if should_unpin {
                    let index = rng.gen_range(0, pinned_pages.len());
                    pinned_pages.remove(index)
                } else {
                    let page_id = PageId(rng.gen_range(0, num_pages));
                    println!("Pinning {:?}", page_id);
                    buffer_pool.get_page(page_id).await?
                };

            println!("Reading {:?}", page.id);
            let value = page.data.read().await[0];
            assert_eq!(value, values[page.id.0]);

            if rng.gen() {
                println!("Writing to {:?}", page.id);
                values[page.id.0] = values[page.id.0].wrapping_add(1);
                page.data.write().await[0] = values[page.id.0];
                page.dirty();
            }

            if should_unpin {
                println!("Unpinning {:?}", page.id);
                drop(page);
            } else {
                pinned_pages.push(page);
            }

            println!("Pinned pages: {:?}", pinned_pages.iter().map(|p| (p.id, p.pin_count())).collect::<Vec<_>>());
        }

        panic!("ok");

        Ok(())
    }).await.unwrap()
}
