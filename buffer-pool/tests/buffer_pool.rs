#![cfg(not(loom))]
#![allow(non_upper_case_globals)]

#[macro_use]
extern crate assert_matches;

#[macro_use]
extern crate log;

use ::buffer_pool::buffer_pool::*;
use ::buffer_pool::disk_manager::*;
use ::buffer_pool::disk_manager_mem::*;




use rand::{Rng, SeedableRng};
use std::collections::HashSet;
use std::sync::Arc;

#[tokio::test]
async fn test_allocate_and_read_one_page() -> Result<()> {
    let buffer_pool = BufferPool::new(Box::new(DiskManagerMem::new()), 1);
    let page = buffer_pool.allocate_page().await?;
    assert_eq!(page.id(), PageId(0));
    page.data().write().await[0] = 5;

    let page = buffer_pool.get_page(PageId(0)).await?;
    assert_eq!(page.data().read().await[0], 5);
    Ok(())
}

#[tokio::test]
async fn test_allocate_more_pages_than_capacity() -> Result<()> {
    let buffer_pool = BufferPool::new(Box::new(DiskManagerMem::new()), 1);
    let page0 = buffer_pool.allocate_page().await?;

    // Allocating second page should fail - we don't have free slots
    assert_matches!(buffer_pool.allocate_page().await, Err(Error::NoFreeFrames));

    drop(page0);

    // Now it should succeed, as we have unpinned the previous page
    buffer_pool.allocate_page().await?;

    Ok(())
}

#[tokio::test]
async fn test_write_and_read_evicted_page() -> Result<()> {
    let buffer_pool = BufferPool::new(Box::new(DiskManagerMem::new()), 1);

    let page = buffer_pool.allocate_page().await?;
    assert_eq!(page.id(), PageId(0));
    page.data().write().await[0] = 5;
    page.dirty();
    drop(page);

    // Allocate another page to evict the one we've written to
    buffer_pool.allocate_page().await?;
    assert!(!buffer_pool.is_page_in_memory(PageId(0)).await);

    let page = buffer_pool.get_page(PageId(0)).await?;
    assert_eq!(page.data().read().await[0], 5);

    Ok(())
}

#[tokio::test]
async fn random_multi_pin_test() -> Result<()> {
    const buffer_pool_size: usize = 2;
    const num_pages: usize = 4;

    let buffer_pool = BufferPool::new(Box::new(DiskManagerMem::new()), buffer_pool_size);

    let mut rng = rand::rngs::StdRng::from_seed([0; 32]);

    for _ in 0..num_pages {
        let page = buffer_pool.allocate_page().await?;
        page.dirty();
    }

    let mut values = Box::new([0u8; num_pages]);
    let mut pinned_pages: Vec<(PageId, PinnedPage)> = Vec::new();

    fn num_unique_pinned_pages(pinned_pages: &Vec<(PageId, PinnedPage)>) -> usize {
        pinned_pages
            .iter()
            .map(|(id, _)| id)
            .collect::<HashSet<_>>()
            .len()
    }

    debug!("Begin test");

    for _ in 0..100usize {
        let should_unpin = if pinned_pages.len() == 0 {
            false
        } else if num_unique_pinned_pages(&pinned_pages) >= buffer_pool_size {
            true
        } else {
            rng.gen()
        };

        let (page_id, page) = if should_unpin {
            let index = rng.gen_range(0, pinned_pages.len());
            pinned_pages.remove(index)
        } else {
            let page_id = PageId(rng.gen_range(0, num_pages));
            debug!("Pinning {:?}", page_id);
            (page_id, buffer_pool.get_page(page_id).await?)
        };

        debug!("Reading {:?}", page_id);
        let value = page.data().read().await[0];
        assert_eq!(value, values[page_id.0]);

        if rng.gen() {
            debug!("Writing to {:?}", page_id);
            values[page.id().0] = values[page_id.0].wrapping_add(1);
            page.data().write().await[0] = values[page_id.0];
            page.dirty();
        }

        if should_unpin {
            debug!("Unpinning {:?}", page_id);
            drop(page);
        } else {
            pinned_pages.push((page_id, page));
        }

        debug!(
            "Pinned pages: {:?}",
            pinned_pages
                .iter()
                .map(|(id, p)| (id, p.pin_count()))
                .collect::<Vec<_>>()
        );
    }

    Ok(())
}

#[tokio::test(core_threads = 6)]
async fn random_multithreaded_multi_pin_test() -> Result<()> {
    const num_threads: usize = 6;
    const max_pins_per_thread: usize = 3;
    const buffer_pool_size: usize = num_threads * max_pins_per_thread;
    const num_pages: usize = max_pins_per_thread * 2;

    let buffer_pool = BufferPool::new(Box::new(DiskManagerMem::new()), buffer_pool_size);

    for _ in 0..num_pages {
        let page = buffer_pool.allocate_page().await?;
        page.dirty();
    }

    let pool_arc = Arc::new(buffer_pool);

    let mut threads = vec![];

    for thread_id in 0..num_threads {
        let buffer_pool = pool_arc.clone();
        let thread_id = thread_id.clone();

        threads.push(tokio::spawn(async move {
            let mut rng = rand::rngs::StdRng::from_seed([thread_id as u8; 32]);

            let mut values = Box::new([0u8; num_pages]);
            let mut pinned_pages: Vec<(PageId, PinnedPage)> = Vec::new();

            fn num_unique_pinned_pages(pinned_pages: &Vec<(PageId, PinnedPage)>) -> usize {
                pinned_pages
                    .iter()
                    .map(|(id, _)| id)
                    .collect::<HashSet<_>>()
                    .len()
            }

            debug!("t{} begin", thread_id);

            for i in 0..100000usize {
                let should_unpin = if pinned_pages.len() == 0 {
                    false
                } else if num_unique_pinned_pages(&pinned_pages) >= buffer_pool_size {
                    true
                } else {
                    rng.gen()
                };

                let (page_id, page) = if should_unpin {
                    let index = rng.gen_range(0, pinned_pages.len());
                    pinned_pages.remove(index)
                } else {
                    let page_id = PageId(rng.gen_range(0, num_pages));
                    //                            debug!("Pinning {:?}", page_id);
                    (page_id, buffer_pool.get_page(page_id).await?)
                };

                //                    debug!("Reading {:?}", page_id);
                let value = page.data().read().await[thread_id];
                assert_eq!(value, values[page_id.0]);

                if rng.gen() {
                    //                        debug!("Writing to {:?}", page_id);
                    values[page.id().0] = values[page_id.0].wrapping_add(1);
                    page.data().write().await[thread_id] = values[page_id.0];
                    page.dirty();
                }

                if should_unpin {
                    //                        debug!("Unpinning {:?}", page_id);
                    drop(page);
                } else {
                    pinned_pages.push((page_id, page));
                }

                if i % 100 == 0 {
                    debug!(
                        "t{} Pinned pages: {:?}",
                        thread_id,
                        pinned_pages
                            .iter()
                            .map(|(id, p)| (id, p.pin_count()))
                            .collect::<Vec<_>>()
                    );
                    tokio::task::yield_now().await;
                }
            }

            debug!("t{} finished", thread_id);

            Ok(()) as Result<()>
        }));
    }

    for join_handle in threads.into_iter() {
        join_handle.await.unwrap()?;
    }

    debug!("Finished");

    Ok(())
}

#[tokio::test(core_threads = 6)]
async fn random_multithreaded_single_pin_per_thread_test() -> Result<()> {
    env_logger::init();

    const num_threads: usize = 6;
    const max_pins_per_thread: usize = 3;
    const buffer_pool_size: usize = num_threads * max_pins_per_thread;
    const num_pages: usize = buffer_pool_size * 2;

    let buffer_pool = BufferPool::new(Box::new(DiskManagerMem::new()), buffer_pool_size);

    for _ in 0..num_pages {
        let page = buffer_pool.allocate_page().await?;
        page.dirty();
    }

    let pool_arc = Arc::new(buffer_pool);

    let mut threads = vec![];

    fn num_pinned_pages(pinned_pages: &Vec<Option<PinnedPage>>) -> usize {
        pinned_pages.iter().filter(|x| x.is_some()).count()
    }

    for thread_id in 0..num_threads {
        let buffer_pool = pool_arc.clone();
        let thread_id = thread_id.clone();

        threads.push(tokio::spawn(async move {
            let mut rng = rand::rngs::StdRng::from_seed([thread_id as u8; 32]);

            let mut values = Box::new([0u8; num_pages]);
            let mut pinned_pages: Vec<Option<PinnedPage>> = vec![];
            for _page_id in 0..num_pages {
                pinned_pages.push(None);
            }

            debug!("t{} begin", thread_id);

            for i in 0..100000usize {
                let page_id = PageId(rng.gen_range(0, pinned_pages.len()));
                let mut page_to_save: Option<PinnedPage> = None;
                let (page, should_unpin): (&PinnedPage, bool) = match &pinned_pages[page_id.0] {
                    None => {
                        if num_pinned_pages(&pinned_pages) >= max_pins_per_thread {
                            continue;
                        }
                        page_to_save = Some(buffer_pool.get_page(page_id).await?);
                        //                                debug!("Pinning {:?}", page_id);
                        match &page_to_save {
                            Some(p) => (p, false),
                            None => panic!("Expected Some"),
                        }
                    }
                    Some(page) => (page, true),
                };
                assert_eq!(page.id(), page_id);

                //                    debug!("Reading {:?}", page_id);
                let value = page.data().read().await[thread_id];
                assert_eq!(value, values[page_id.0]);

                if rng.gen() {
                    //                        debug!("Writing to {:?}", page_id);
                    values[page_id.0] = values[page_id.0].wrapping_add(1);
                    page.data().write().await[thread_id] = values[page_id.0];
                    page.dirty();
                }

                if should_unpin {
                    //                        debug!("Unpinning {:?}", page_id);
                    pinned_pages[page_id.0] = None;
                } else {
                    pinned_pages[page_id.0] = page_to_save;
                }

                if i % 1000 == 0 {
                    debug!(
                        "t{} pinned pages: {:?}",
                        thread_id,
                        pinned_pages
                            .iter()
                            .enumerate()
                            .filter_map(|(id, op)| op.as_ref().map(|p| (id, p.id(), p.pin_count())))
                            .collect::<Vec<_>>()
                    );
                    //                        tokio::task::yield_now().await;
                }
            }

            debug!("t{} finished", thread_id);

            Ok(()) as Result<()>
        }));
    }

    for join_handle in threads.into_iter() {
        join_handle.await.unwrap()?;
    }

    debug!("Finished");

    Ok(())
}
