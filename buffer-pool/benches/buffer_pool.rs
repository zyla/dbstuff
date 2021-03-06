#![feature(test)]
#![allow(non_upper_case_globals)]

extern crate rand;
extern crate test;

use ::buffer_pool::buffer_pool::*;
use ::buffer_pool::disk_manager::*;
use ::buffer_pool::disk_manager_mem::*;

use rand::{Rng, SeedableRng};

use std::convert::TryInto;
use std::sync::Arc;

#[allow(soft_unstable)]
#[bench]
fn multithreaded_single_pin_per_thread_bench(b: &mut test::bench::Bencher) {
    b.iter(|| {
        tokio::runtime::Builder::new()
            .threaded_scheduler()
            .build()
            .unwrap()
            .block_on(async {
                const num_threads: usize = 6;
                const max_pins_per_thread: usize = 3;
                const buffer_pool_size: usize = num_threads * max_pins_per_thread;
                const num_pages: usize = buffer_pool_size * 2;

                let buffer_pool =
                    BufferPool::new(Box::new(DiskManagerMem::new()), buffer_pool_size);

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

                        for _i in 0..1000usize {
                            let page_id =
                                PageId(rng.gen_range(0, pinned_pages.len()).try_into().unwrap());
                            let mut page_to_save: Option<PinnedPage> = None;
                            let (page, should_unpin): (&PinnedPage, bool) =
                                match &pinned_pages[page_id.0 as usize] {
                                    None => {
                                        if num_pinned_pages(&pinned_pages) >= max_pins_per_thread {
                                            continue;
                                        }
                                        page_to_save = Some(buffer_pool.get_page(page_id).await?);
                                        //                                println!("Pinning {:?}", page_id);
                                        match &page_to_save {
                                            Some(p) => (p, false),
                                            None => panic!("Expected Some"),
                                        }
                                    }
                                    Some(page) => (page, true),
                                };

                            //                    println!("Reading {:?}", page_id);
                            let value = page.data().read().await[thread_id];
                            assert_eq!(value, values[page_id.0 as usize]);

                            if rng.gen() {
                                //                        println!("Writing to {:?}", page_id);
                                values[page_id.0 as usize] =
                                    values[page_id.0 as usize].wrapping_add(1);
                                page.data().write().await[thread_id] = values[page_id.0 as usize];
                                page.dirty();
                            }

                            if should_unpin {
                                //                        println!("Unpinning {:?}", page_id);
                                pinned_pages[page_id.0 as usize] = None;
                            } else {
                                pinned_pages[page_id.0 as usize] = page_to_save;
                            }
                        }

                        Ok(()) as Result<()>
                    }));
                }

                for join_handle in threads.into_iter() {
                    join_handle.await.unwrap()?;
                }

                println!("Finished");

                Ok(()) as Result<()>
            })
            .unwrap();
    });
}
