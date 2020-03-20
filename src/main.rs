#![feature(async_closure)]

mod disk_manager;

use tokio::fs;
use tokio::prelude::*;
use std::path::Path;
use std::collections::{HashMap};
use std::sync::{RwLock};
use crate::disk_manager::*;
use std::sync::atomic::{AtomicUsize, AtomicBool};
use std::ops::DerefMut;
use std::future::Future;

struct Page {
    id: PageId,
    dirty: AtomicBool,
    pin_count: AtomicUsize,
    data: RwLock<PageData>,
}

type FrameId = usize;

struct BufferPool {
    capacity: usize,
    frames: Box<[Page]>,
    lock: RwLock<BufferPoolInner>,
}

struct BufferPoolInner {
    disk_manager: DiskManager,
    page_table: HashMap<PageId, FrameId>,
    free_frames: Vec<FrameId>,
}

impl BufferPool {
    fn new(disk_manager: DiskManager, capacity: usize) -> BufferPool {
        let mut frames = Vec::with_capacity(capacity);
        let mut free_frames = Vec::with_capacity(capacity);
        for i in 0..capacity {
            frames.push(Page {
                id: PageId(std::usize::MAX),
                dirty: AtomicBool::default(),
                pin_count: AtomicUsize::default(),
                data: RwLock::new([0; PAGE_SIZE])
            });
            free_frames.push(i);
        }
        BufferPool {
            capacity: capacity,
            frames: frames.into_boxed_slice(),
            lock: RwLock::new(BufferPoolInner {
                disk_manager: disk_manager,
                page_table: HashMap::with_capacity(capacity),
                free_frames: free_frames,
            }),
        }
    }

    pub async fn get_page(&self, page_id: PageId) -> io::Result<&Page> {
        let inner = self.lock.read().unwrap();
        match inner.page_table.get(&page_id) {
            Some(&frame_id) => Ok(&self.frames[frame_id]),
            None => {
                unlock(inner);

                let mut inner = self.lock.write().unwrap();
                let frame_id = self.get_free_frame(inner.deref_mut()).await?;
                let page_ = &self.frames[frame_id];

                // SAFETY: We're sure nobody else is accessing this Page,
                // because we just pulled it off the free list, and we're still holding the page
                // table lock.
                let page = unsafe { &mut *(page_ as *const Page as *mut Page) };
                page.id = page_id;
                *page.dirty.get_mut() = false;
                *page.pin_count.get_mut() = 1;

                // TODO: Should we still hold the lock while we're doing IO?
                // A: Yes, but maybe a different one? (we shouldn't block reading existing tables,
                // but we don't want to read the same page twice)
                inner.disk_manager.read_page(page_id, page.data.get_mut().unwrap()).await?;

                inner.page_table.insert(page_id, frame_id);

                // I believe this is necessary to stretch the lifetime of the lock guard after we stop
                // using `page`.
                unlock(inner);

                Ok(page)
            }
        }
    }

    // TODO: decopypaste - get_page
    pub async fn allocate_page(&self) -> io::Result<&Page> {
        let mut inner = self.lock.write().unwrap();
        let frame_id = self.get_free_frame(inner.deref_mut()).await?;
        let page_ = &self.frames[frame_id];

        let page_id = inner.disk_manager.allocate_page().await?;

        // SAFETY: We're sure nobody else is accessing this Page,
        // because we just pulled it off the free list, and we're still holding the page
        // table lock.
        let page = unsafe { &mut *(page_ as *const Page as *mut Page) };
        page.id = page_id;
        *page.dirty.get_mut() = false;
        *page.pin_count.get_mut() = 1;

        inner.page_table.insert(page_id, frame_id);

        // I believe this is necessary to stretch the lifetime of the lock guard after we stop
        // using `page`.
        unlock(inner);

        Ok(page)
    }

    async fn get_free_frame(&self, inner: &mut BufferPoolInner) -> io::Result<FrameId> {
        match inner.free_frames.pop() {
            Some(frame_id) => Ok(frame_id),
            None => {
                panic!("No free frames");
            }
        }
    }
}

fn unlock<T>(lock: T) {}

#[cfg(test)]
mod tests {
    use super::*;

    async fn with_temp_db<R, RF: Future<Output = io::Result<R>>, F: FnOnce(DiskManager) -> RF>(f: F) -> io::Result<R> {
        let filename = "test.db";
        let mut disk_manager = DiskManager::open(filename).await?;
        let result = f(disk_manager).await;
        fs::remove_file(filename).await?;
        result
    }

    #[tokio::test]
    async fn test_allocate_and_read_one_page() {
        with_temp_db(async move |disk_manager| {
            let mut buffer_pool = BufferPool::new(disk_manager, 1);
            let page = buffer_pool.allocate_page().await?;
            assert_eq!(page.id, PageId(0));
            page.data.write().unwrap()[0] = 5;

            let page = buffer_pool.get_page(PageId(0)).await?;
            assert_eq!(page.data.read().unwrap()[0], 5);

            Ok(())
        }).await.unwrap()
    }
}
