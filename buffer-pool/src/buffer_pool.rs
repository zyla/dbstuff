use crate::disk_manager::*;
use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::ops::DerefMut;
use crate::sync::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::RwLock;
use std::io;

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
    id: UnsafeCell<PageId>,
    dirty: AtomicBool,
    pin_count: AtomicUsize,
    data: RwLock<PageData>,
}

// SAFETY: We're protecting the UnsafeCell inside Page by the combination of
// buffer pool lock and pin count.
// TODO: Check whether we really satisfy the guarantees of Send, in particular if nothing is
// screwed up across awaits
unsafe impl Send for Page {}
unsafe impl Sync for Page {}

impl std::fmt::Debug for Page {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("Page").field("id", &self.id).finish()
    }
}

pub type FrameId = usize;

pub struct BufferPool {
    capacity: usize,
    frames: Box<[Page]>,
    lock: RwLock<BufferPoolInner>,
}

struct BufferPoolInner {
    disk_manager: Box<dyn DiskManager + Send>,
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

impl PinnedPage<'_> {
    pub fn id(&self) -> PageId {
        // SAFETY: The page is pinned, so the buffer pool is not switching it to a different one
        unsafe { *self.page.id.get() }
    }

    pub fn dirty(&self) {
        self.page.dirty.store(true, Ordering::SeqCst);
    }

    pub fn pin_count(&self) -> usize {
        self.page.pin_count.load(Ordering::SeqCst)
    }

    pub fn data(&self) -> &RwLock<PageData> {
        &self.page.data
    }
}

impl BufferPool {
    pub fn new(disk_manager: Box<dyn DiskManager + Send>, capacity: usize) -> BufferPool {
        let mut frames = Vec::with_capacity(capacity);
        let mut free_frames = Vec::with_capacity(capacity);
        for i in 0..capacity {
            frames.push(Page {
                id: UnsafeCell::new(PageId(std::usize::MAX)),
                dirty: AtomicBool::default(),
                pin_count: AtomicUsize::default(),
                data: RwLock::new([0; PAGE_SIZE]),
            });
            free_frames.push(i);
        }
        BufferPool {
            capacity,
            frames: frames.into_boxed_slice(),
            lock: RwLock::new(BufferPoolInner {
                disk_manager,
                page_table: HashMap::with_capacity(capacity),
                free_frames,
                ref_flag: bitvec![0; capacity],
                clock_hand: 0,
            }),
        }
    }

    pub async fn get_page(&self, page_id: PageId) -> Result<PinnedPage<'_>> {
        let inner = self.lock.read().await;
        match inner.page_table.get(&page_id) {
            Some(&frame_id) => {
                let page = &self.frames[frame_id];
                let old_pin_count = page.pin_count.fetch_add(1, Ordering::SeqCst);

                if old_pin_count == 0 {
                    drop(inner);
                    let mut inner = self.lock.write().await;

                    // Do we really need to take the writer lock here? Seems insane!
                    // TODO: Measure if we can get some improvement if we have a different lock, or use a
                    // lock-free bit vector
                    inner.ref_flag.set(frame_id, true);
                }

                Ok(PinnedPage { page })
            }
            None => {
                drop(inner);

                let mut inner = self.lock.write().await;

                // Somebody else may have fetched the same page before we got the writer lock,
                // in which case we're done.
                if let Some(&frame_id) = inner.page_table.get(&page_id) {
                    let page = &self.frames[frame_id];
                    let old_pin_count = page.pin_count.fetch_add(1, Ordering::SeqCst);

                    if old_pin_count == 0 {
                        inner.ref_flag.set(frame_id, true);
                    }

                    return Ok(PinnedPage { page });
                }

                let frame_id = self.get_free_frame(inner.deref_mut()).await?;

                let page = &self.frames[frame_id];
                // SAFETY: We're sure nobody else is accessing this Page,
                // because we just pulled it off the free list, and we're still holding the page
                // table lock.
                unsafe {
                    page.id.get().write(page_id);
                }
                page.dirty.store(false, Ordering::SeqCst);
                page.pin_count.store(1, Ordering::SeqCst);

                // TODO: Should we still hold the lock while we're doing IO?
                // A: Yes, but maybe a different one? (we shouldn't block reading existing tables,
                // but we don't want to read the same page twice)
                let mut data = page.data.write().await; // FIXME: we have exclusive access, shouldn't have to lock
                inner.disk_manager.read_page(page_id, &mut data).await?;

                inner.page_table.insert(page_id, frame_id);

                inner.ref_flag.set(frame_id, true);

                Ok(PinnedPage { page })
            }
        }
    }

    pub async fn is_page_in_memory(&self, page_id: PageId) -> bool {
        let inner = self.lock.read().await;
        inner.page_table.contains_key(&page_id)
    }

    // TODO: decopypaste - get_page
    pub async fn allocate_page(&self) -> Result<PinnedPage<'_>> {
        let mut inner = self.lock.write().await;
        let frame_id = self.get_free_frame(inner.deref_mut()).await?;

        let page_id = inner.disk_manager.allocate_page().await?;

        let page = &self.frames[frame_id];
        // SAFETY: We're sure nobody else is accessing this Page,
        // because we just pulled it off the free list, and we're still holding the page
        // table lock.
        unsafe {
            page.id.get().write(page_id);
        }
        page.dirty.store(false, Ordering::SeqCst);
        page.pin_count.store(1, Ordering::SeqCst);

        // Zero-fill the newly created page
        let mut data = page.data.write().await; // FIXME: we have exclusive access, shouldn't have to lock
        for i in data.iter_mut() {
            *i = 0;
        }

        inner.page_table.insert(page_id, frame_id);

        Ok(PinnedPage { page })
    }

    async fn get_free_frame(&self, inner: &mut BufferPoolInner) -> Result<FrameId> {
        match inner.free_frames.pop() {
            Some(frame_id) => Ok(frame_id),
            None => {
                let frame_id = self.find_victim(inner)?;
                let page = &self.frames[frame_id];
                // SAFETY:
                // - find_victim returns only frames with pin_count == 0
                // - we're still holding buffer pool lock
                let page_id = unsafe { *page.id.get() };
                unsafe { page.id.get().write(PageId(std::usize::MAX)) }

                inner.page_table.remove(&page_id);

                if page.dirty.load(Ordering::SeqCst) {
                    let data = page.data.read().await; // FIXME: we have exclusive access, shouldn't have to lock
                    inner.disk_manager.write_page(page_id, &data).await?;
                    page.dirty.store(false, Ordering::SeqCst);
                }

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
            if self.frames[inner.clock_hand]
                .pin_count
                .load(Ordering::SeqCst)
                == 0
            {
                if inner.ref_flag.get(inner.clock_hand) == Some(&true) {
                    //                    println!("find_victim: unref {}", inner.clock_hand);
                    inner.ref_flag.set(inner.clock_hand, false);
                } else {
                    return Ok(inner.clock_hand);
                }
            } else {
                //                println!("find_victim: skip {}", inner.clock_hand);
            }
            i += 1;
            inner.clock_hand = (inner.clock_hand + 1) % self.capacity;
        }

        Err(Error::NoFreeFrames)
    }
}
