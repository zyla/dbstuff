use buffer_pool::disk_manager::{PageData, PageId, INVALID_PAGE_ID, PAGE_SIZE};
use std::ops::{Deref, DerefMut};

pub struct TablePage<T> {
    data: T,
}

//
// Slotted page format:
// ---------------------------------------------------------
// | HEADER | ... FREE SPACE ... | ... INSERTED TUPLES ... |
// ---------------------------------------------------------
//                               ^
//                               free space pointer
//
// Header format (size in bytes):
// ----------------------------------------------------------------------------
// | PageId (4)| LSN (4)| PrevPageId (4)| NextPageId (4)| FreeSpacePointer(4) |
// ----------------------------------------------------------------------------
// ----------------------------------------------------------------
// | TupleCount (4) | Tuple_1 offset (4) | Tuple_1 size (4) | ... |
// ----------------------------------------------------------------
//

const OFFSET_NEXT_PAGE_ID: usize = 0x0c;
const OFFSET_FREE_SPACE_PTR: usize = 0x10;
const OFFSET_TUPLE_COUNT: usize = 0x14;
const OFFSET_TUPLE_DESCRIPTORS: usize = 0x18;
const SIZE_TUPLE_DESCRIPTOR: usize = 8;

// offsets inside tuple descriptor
const OFFSET_TUPLE_OFFSET: usize = 0;
const OFFSET_TUPLE_SIZE: usize = 4;

#[derive(PartialEq, Eq, Debug)]
pub enum Error {
    PageFull,
}

pub use Error::*;

pub type Result<T> = std::result::Result<T, Error>;

pub type SlotIndex = usize;

impl<T: Deref<Target = PageData>> TablePage<T> {
    pub fn from_existing(data: T) -> Self {
        TablePage { data }
    }

    pub fn free_space(&self) -> usize {
        self.get_free_space_ptr()
            - OFFSET_TUPLE_DESCRIPTORS
            - self.get_tuple_count() * SIZE_TUPLE_DESCRIPTOR
    }

    fn get_free_space_ptr(&self) -> usize {
        self.read_u32(OFFSET_FREE_SPACE_PTR) as usize
    }

    pub fn get_tuple_count(&self) -> usize {
        self.read_u32(OFFSET_TUPLE_COUNT) as usize
    }

    pub fn get_next_page_id(&self) -> PageId {
        PageId(self.read_u32(OFFSET_NEXT_PAGE_ID) as usize)
    }

    #[allow(clippy::cast_ptr_alignment)]
    fn read_u32(&self, offset: usize) -> u32 {
        let ptr = self.data[offset..].as_ptr() as *const u32;
        unsafe { ptr.read_unaligned() }
    }

    #[cfg(test)]
    pub(crate) fn data(&self) -> &PageData {
        &self.data
    }
}

impl<T: DerefMut<Target = PageData>> TablePage<T> {
    /// Initialize a new page in the given storage.
    pub fn new(data: T) -> Self {
        let mut page = TablePage { data };
        page.set_free_space_ptr(PAGE_SIZE as u32);
        page.set_tuple_count(0);
        page.set_next_page_id(INVALID_PAGE_ID);
        page
    }

    pub fn alloc_tuple(&mut self, size: usize) -> Result<(SlotIndex, &mut [u8])> {
        if self.free_space() < size + SIZE_TUPLE_DESCRIPTOR {
            return Err(Error::PageFull);
        }
        let end = self.get_free_space_ptr();
        let start = end - size;
        let index = self.get_tuple_count();
        self.set_free_space_ptr(start as u32);
        self.set_tuple_count((index + 1) as u32);
        self.set_tuple_descriptor(index, start as u32, (end - start) as u32);
        Ok((index, &mut self.data[start..end]))
    }

    pub fn insert_tuple<'a>(&mut self, tuple: &'a [u8]) -> Result<SlotIndex> {
        let (slot_index, new_tuple) = self.alloc_tuple(tuple.len())?;
        new_tuple.copy_from_slice(tuple);
        Ok(slot_index)
    }

    pub fn get_tuple(&self, index: SlotIndex) -> Option<&[u8]> {
        let (offset, size) = self.get_tuple_descriptor(index);
        if offset == 0 {
            return None;
        }
        Some(&self.data[offset..offset + size])
    }

    pub fn get_tuple_mut(&mut self, index: SlotIndex) -> Option<&mut [u8]> {
        let (offset, size) = self.get_tuple_descriptor(index);
        if offset == 0 {
            return None;
        }
        Some(&mut self.data[offset..offset + size])
    }

    fn set_free_space_ptr(&mut self, value: u32) {
        self.write_u32(OFFSET_FREE_SPACE_PTR, value);
    }

    fn set_tuple_count(&mut self, value: u32) {
        self.write_u32(OFFSET_TUPLE_COUNT, value);
    }

    pub fn set_next_page_id(&mut self, value: PageId) {
        self.write_u32(OFFSET_NEXT_PAGE_ID, value.0 as u32)
    }

    fn set_tuple_descriptor(&mut self, index: usize, offset: u32, size: u32) {
        self.write_u32(
            OFFSET_TUPLE_DESCRIPTORS + SIZE_TUPLE_DESCRIPTOR * index + OFFSET_TUPLE_OFFSET,
            offset,
        );
        self.write_u32(
            OFFSET_TUPLE_DESCRIPTORS + SIZE_TUPLE_DESCRIPTOR * index + OFFSET_TUPLE_SIZE,
            size,
        );
    }

    fn get_tuple_descriptor(&self, index: usize) -> (usize, usize) {
        (
            self.read_u32(
                OFFSET_TUPLE_DESCRIPTORS + SIZE_TUPLE_DESCRIPTOR * index + OFFSET_TUPLE_OFFSET,
            ) as usize,
            self.read_u32(
                OFFSET_TUPLE_DESCRIPTORS + SIZE_TUPLE_DESCRIPTOR * index + OFFSET_TUPLE_SIZE,
            ) as usize,
        )
    }

    #[allow(clippy::cast_ptr_alignment)]
    fn write_u32(&mut self, offset: usize, value: u32) {
        let ptr = self.data[offset..].as_mut_ptr() as *mut u32;
        unsafe {
            ptr.write_unaligned(value);
        }
    }
}
