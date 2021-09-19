use buffer_pool::disk_manager::{PageData, PageId, PAGE_SIZE};
use std::ops::{Deref, DerefMut};

pub struct InternalPage<T> {
    data: T,
}

const OFFSET_KEY_COUNT: usize = 0x00;
const OFFSET_FREE_SPACE_PTR: usize = 0x02;
const OFFSET_ENTRY_DESCRIPTORS: usize = 0x04;
const SIZE_ENTRY_DESCRIPTOR: usize = 8;

// offsets inside entry descriptor
const OFFSET_CHILD_POINTER: usize = 0;
const OFFSET_KEY_OFFSET: usize = 4;
const OFFSET_KEY_SIZE: usize = 6;

#[derive(PartialEq, Eq, Debug)]
pub enum Error {
    PageFull,
}

pub use Error::*;

pub type Result<T> = std::result::Result<T, Error>;

pub type SlotIndex = usize;

impl<T: Deref<Target = PageData>> InternalPage<T> {
    pub fn from_existing(data: T) -> Self {
        InternalPage { data }
    }

    pub fn unwrap(&self) -> &T {
        &self.data
    }

    pub fn free_space(&self) -> usize {
        self.get_free_space_ptr()
            - OFFSET_ENTRY_DESCRIPTORS
            - self.get_key_count() * SIZE_ENTRY_DESCRIPTOR
    }

    fn get_free_space_ptr(&self) -> usize {
        self.read_u16(OFFSET_FREE_SPACE_PTR) as usize
    }

    pub fn get_key_count(&self) -> usize {
        self.read_u16(OFFSET_KEY_COUNT) as usize
    }

    pub fn get_key(&self, index: SlotIndex) -> &[u8] {
        let (offset, size) = self.get_key_descriptor(index);
        &self.data[offset..offset + size]
    }

    pub fn get_child_pointer(&self, index: SlotIndex) -> PageId {
        PageId(self.read_u32(
            OFFSET_ENTRY_DESCRIPTORS + SIZE_ENTRY_DESCRIPTOR * index + OFFSET_CHILD_POINTER,
        ))
    }

    fn get_key_descriptor(&self, index: usize) -> (usize, usize) {
        (
            self.read_u16(
                OFFSET_ENTRY_DESCRIPTORS + SIZE_ENTRY_DESCRIPTOR * index + OFFSET_KEY_OFFSET,
            ) as usize,
            self.read_u16(
                OFFSET_ENTRY_DESCRIPTORS + SIZE_ENTRY_DESCRIPTOR * index + OFFSET_KEY_SIZE,
            ) as usize,
        )
    }

    #[allow(clippy::cast_ptr_alignment)]
    fn read_u32(&self, offset: usize) -> u32 {
        let ptr = self.data[offset..].as_ptr() as *const u32;
        unsafe { ptr.read_unaligned() }
    }

    #[allow(clippy::cast_ptr_alignment)]
    fn read_u16(&self, offset: usize) -> u16 {
        let ptr = self.data[offset..].as_ptr() as *const u16;
        unsafe { ptr.read_unaligned() }
    }

    #[cfg(test)]
    pub(crate) fn data(&self) -> &PageData {
        &self.data
    }
}

impl<T: DerefMut<Target = PageData>> InternalPage<T> {
    /// Initialize a new page in the given storage.
    pub fn new(data: T) -> Self {
        let mut page = InternalPage { data };
        page.set_free_space_ptr(PAGE_SIZE as u16);
        page.set_key_count(0);
        page
    }

    fn set_free_space_ptr(&mut self, value: u16) {
        self.write_u16(OFFSET_FREE_SPACE_PTR, value);
    }

    fn set_key_count(&mut self, value: u16) {
        self.write_u16(OFFSET_KEY_COUNT, value);
    }

    fn set_key_descriptor(&mut self, index: usize, offset: u16, size: u16) {
        self.write_u16(
            OFFSET_ENTRY_DESCRIPTORS + SIZE_ENTRY_DESCRIPTOR * index + OFFSET_KEY_OFFSET,
            offset,
        );
        self.write_u16(
            OFFSET_ENTRY_DESCRIPTORS + SIZE_ENTRY_DESCRIPTOR * index + OFFSET_KEY_SIZE,
            size,
        );
    }

    fn set_child_pointer(&mut self, index: usize, value: PageId) {
        self.write_u32(
            OFFSET_ENTRY_DESCRIPTORS + SIZE_ENTRY_DESCRIPTOR * index + OFFSET_CHILD_POINTER,
            value.0,
        );
    }

    #[allow(clippy::cast_ptr_alignment)]
    fn write_u32(&mut self, offset: usize, value: u32) {
        let ptr = self.data[offset..].as_mut_ptr() as *mut u32;
        unsafe {
            ptr.write_unaligned(value);
        }
    }

    #[allow(clippy::cast_ptr_alignment)]
    fn write_u16(&mut self, offset: usize, value: u16) {
        let ptr = self.data[offset..].as_mut_ptr() as *mut u16;
        unsafe {
            ptr.write_unaligned(value);
        }
    }
}
