pub mod disk_manager;

#[macro_use] extern crate bitvec;

extern crate rand;

use tokio::fs;
use tokio::prelude::*;
use std::path::Path;
use std::collections::{HashMap};
use tokio::sync::{RwLock};
use crate::disk_manager::*;
use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};
use std::ops::{Deref, DerefMut};
use std::future::Future;
use bitvec::vec::BitVec;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    IOError(io::Error),
    NoFreeFrames,
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::IOError(err)
    }
}

pub struct Page {
    pub id: PageId,
    dirty: AtomicBool,
    pin_count: AtomicUsize,
    pub data: RwLock<PageData>,
}

impl Page {
    pub fn dirty(&self) {
        self.dirty.store(true, Ordering::SeqCst);
    }
}

impl std::fmt::Debug for Page {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("Page")
            .field("id", &self.id)
            .finish()
    }
}

pub type FrameId = usize;

pub struct BufferPool {
    capacity: usize,
    frames: Box<[Page]>,
    lock: RwLock<BufferPoolInner>,
}

struct BufferPoolInner {
    disk_manager: DiskManager,
    page_table: HashMap<PageId, FrameId>,
    free_frames: Vec<FrameId>,
    ref_flag: BitVec,
    clock_hand: usize,
}

#[derive(Debug)]
pub struct PinnedPage<'a> {
    page: &'a Page,
}

impl<'a> Drop for PinnedPage<'a> {
    fn drop(&mut self) {
        self.page.pin_count.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Deref for PinnedPage<'_> {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        self.page
    }
}

impl BufferPool {
    pub fn new(disk_manager: DiskManager, capacity: usize) -> BufferPool {
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
                ref_flag: bitvec![0; capacity],
                clock_hand: 0,
            }),
        }
    }

    pub async fn get_page<'a>(&'a self, page_id: PageId) -> Result<PinnedPage<'a>> {
        let inner = self.lock.read().await;
        match inner.page_table.get(&page_id) {
            Some(&frame_id) => {
                drop(inner);
                self.pin_existing_page(frame_id).await
            }
            None => {
                drop(inner);

                let mut inner = self.lock.write().await;

                // Somebody else may have fetched the same page before we got the writer lock,
                // in which case we're done.
                if let Some(&frame_id) = inner.page_table.get(&page_id) {
                    drop(inner);
                    return self.pin_existing_page(frame_id).await;
                }

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
                inner.disk_manager.read_page(page_id, page.data.get_mut()).await?;

                inner.page_table.insert(page_id, frame_id);

                inner.ref_flag.set(frame_id, true);

                // I believe this is necessary to stretch the lifetime of the lock guard after we stop
                // using `page`.
                // TODO: actually check this - does Rust guarantee lifetime until end of block?
                drop(inner);

                Ok(PinnedPage { page: page })
            }
        }
    }

    pub async fn is_page_in_memory(&self, page_id: PageId) -> bool {
        let inner = self.lock.read().await;
        inner.page_table.contains_key(&page_id)
    }

    // May lock `inner` in write mode.
    async fn pin_existing_page<'a>(&'a self, frame_id: FrameId) -> Result<PinnedPage<'a>> {
        let page = &self.frames[frame_id];
        let old_pin_count = page.pin_count.fetch_add(1, Ordering::SeqCst);

        if old_pin_count == 0 {
            let mut inner = self.lock.write().await;

            // Do we really need to take the writer lock here? Seems insane!
            // TODO: Measure if we can get some improvement if we have a different lock, or use a
            // lock-free bit vector
            inner.ref_flag.set(frame_id, true);
        }

        Ok(PinnedPage { page: page })
    }

    // TODO: decopypaste - get_page
    pub async fn allocate_page<'a>(&'a self) -> Result<PinnedPage<'a>> {
        let mut inner = self.lock.write().await;
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
        
        // Zero-fill the newly created page
        let data = page.data.get_mut();
        for i in data.iter_mut() {
            *i = 0;
        }

        inner.page_table.insert(page_id, frame_id);

        // I believe this is necessary to stretch the lifetime of the lock guard after we stop
        // using `page`.
        drop(inner);

        Ok(PinnedPage { page: page })
    }

    async fn get_free_frame(&self, inner: &mut BufferPoolInner) -> Result<FrameId> {
        match inner.free_frames.pop() {
            Some(frame_id) => Ok(frame_id),
            None => {
                let frame_id = self.find_victim(inner)?;

                println!("evict {}", frame_id);

                // SAFETY: We're sure nobody else is accessing this Page,
                // because:
                // - we observed its pin_count at 0, and
                // - nobody could have increased it, because pin_count only increases while holding
                // writer lock on `inner`.
                let page = unsafe { &mut *(&self.frames[frame_id] as *const Page as *mut Page) };

                inner.page_table.remove(&page.id);

                if *page.dirty.get_mut() {
                    inner.disk_manager.write_page(page.id, page.data.get_mut()).await?;
                    *page.dirty.get_mut() = false;
                }

                page.id = PageId(std::usize::MAX);
                *page.pin_count.get_mut() = 0;

                Ok(frame_id)
            }
        }
    }

    fn find_victim(&self, inner: &mut BufferPoolInner) -> Result<FrameId> {
        // Note [Two passes]
        // In the first pass we may not get a page, since all unpinned pages will be also references.
        // On the second pass we're guaranteed to get a page, since we unrefed them all in the first pass.
        let mut i = 0;
        while i < self.capacity * 2 {
            if self.frames[inner.clock_hand].pin_count.load(Ordering::SeqCst) == 0 {
                if inner.ref_flag.get(inner.clock_hand) == Some(&true) {
                    println!("find_victim: unref {}", inner.clock_hand);
                    inner.ref_flag.set(inner.clock_hand, false);
                } else {
                    return Ok(inner.clock_hand);
                }
            } else {
                println!("find_victim: skip {}", inner.clock_hand);
            }
            i += 1;
            inner.clock_hand = (inner.clock_hand + 1) % self.capacity;
        }
        
        Err(Error::NoFreeFrames)
    }
}
