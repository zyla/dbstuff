/// A generic page storing some metadata (opaque sequence of bytes, specific to page type) and a sequence of tuples (opaque byte sequences).
use buffer_pool::disk_manager::{PageData, PAGE_SIZE};
use std::mem;
use std::ops::{Deref, DerefMut};

pub struct TupleBlockPage<T, Meta = ()> {
    data: T,
    _phantom: std::marker::PhantomData<Meta>,
}

// Page format:
// --------------------------------------------------------
// | lsn (4) | metadata_size (2) | free_space_pointer (2) |
// -------------------------------------------------------------------
// | tuple_count (2) | tuple_offset[0] (2) | tuple_size[0] (2) | ... |
// -------------------------------------------------------------------
// | tuple_offset[tuple_count-1] (2) | tuple_size[tuple_count-1] (2) |
// -------------------------------------------------------------------
// | FREE SPACE | TUPLES | metadata (metadata_size) |
// --------------------------------------------------
//              ^ free_space_pointer
//

pub struct PageHeader {
    lsn: u32,
    metadata_size: u16,
    free_space_pointer: u16,
    tuple_count: u16,
    // next: a sequence of TupleDescriptor structs
}

struct TupleDescriptor {
    offset: u16,
    size: u16,
}

#[derive(PartialEq, Eq, Debug)]
pub enum Error {
    PageFull,
}

pub use Error::*;

pub type Result<T> = std::result::Result<T, Error>;

pub type SlotIndex = usize;

impl<T: Deref<Target = PageData>, Meta> TupleBlockPage<T, Meta> {
    pub fn from_existing(data: T) -> Self {
        let page = Self {
            data,
            _phantom: std::marker::PhantomData,
        };
        assert!(mem::size_of::<PageHeader>() < PAGE_SIZE);
        assert!(page.header().metadata_size == mem::size_of::<Meta>() as u16);
        page
    }

    pub fn unwrap(&self) -> &T {
        &self.data
    }

    pub fn header(&self) -> &PageHeader {
        unsafe { mem::transmute(self.data.as_ptr()) }
    }

    pub fn metadata(&self) -> &Meta {
        unsafe { mem::transmute(self.data[PAGE_SIZE - mem::size_of::<Meta>()..].as_ptr()) }
    }

    pub fn tuple_count(&self) -> usize {
        self.header().tuple_count as usize
    }

    pub fn free_space(&self) -> usize {
        let header = self.header();
        (header.free_space_pointer as usize)
            - mem::size_of::<PageHeader>()
            - self.tuple_count() * mem::size_of::<TupleDescriptor>()
    }

    pub fn get_tuple(&self, index: SlotIndex) -> Option<&[u8]> {
        let descriptor = self.get_tuple_descriptor(index);
        if descriptor.offset == 0 {
            return None;
        }
        Some(
            &self.data
                [descriptor.offset as usize..descriptor.offset as usize + descriptor.size as usize],
        )
    }

    fn get_tuple_descriptor(&self, index: usize) -> TupleDescriptor {
        assert!(index < self.tuple_count());
        let offset = mem::size_of::<PageHeader>() + index * mem::size_of::<TupleDescriptor>();
        unsafe { (self.data[offset..].as_ptr() as *const TupleDescriptor).read() }
    }

    #[cfg(test)]
    pub(crate) fn data(&self) -> &PageData {
        &self.data
    }
}

impl<T: DerefMut<Target = PageData>, Meta: Copy> TupleBlockPage<T, Meta> {
    /// Initialize a new page in the given storage.
    pub fn new(data: T, metadata: &Meta) -> Self {
        let mut page = Self {
            data,
            _phantom: std::marker::PhantomData,
        };
        let metadata_size = mem::size_of::<Meta>() as u16;
        *unsafe { page.header_mut() } = PageHeader {
            lsn: 0,
            metadata_size,
            free_space_pointer: PAGE_SIZE as u16 - metadata_size,
            tuple_count: 0,
        };
        *page.metadata_mut() = *metadata;
        page
    }

    /// Returns a mutable reference to the header.
    ///
    /// Unsafe because by messing with `tuple_count` or `free_space_pointer` one can cause accesses beyond the page
    /// boundary.
    pub unsafe fn header_mut(&mut self) -> &mut PageHeader {
        unsafe { mem::transmute(self.data.as_ptr()) }
    }

    pub fn metadata_mut(&mut self) -> &mut Meta {
        unsafe { mem::transmute(self.data[PAGE_SIZE - mem::size_of::<Meta>()..].as_ptr()) }
    }

    pub fn alloc_tuple(&mut self, size: usize) -> Result<(SlotIndex, &mut [u8])> {
        if self.free_space() < size + mem::size_of::<TupleDescriptor>() {
            return Err(Error::PageFull);
        }
        let end = self.header().free_space_pointer;
        let start = end - (size as u16);
        let index = self.header().tuple_count;
        unsafe { self.header_mut() }.free_space_pointer = start;
        unsafe { self.header_mut() }.tuple_count = index + 1;
        self.set_tuple_descriptor(
            index as usize,
            TupleDescriptor {
                offset: start,
                size: (end - start),
            },
        );
        Ok((
            index as SlotIndex,
            &mut self.data[start as usize..end as usize],
        ))
    }

    fn set_tuple_descriptor(&mut self, index: usize, descriptor: TupleDescriptor) {
        assert!(index < self.tuple_count());
        let offset = mem::size_of::<PageHeader>() + index * mem::size_of::<TupleDescriptor>();
        unsafe { (self.data[offset..].as_mut_ptr() as *mut TupleDescriptor).write(descriptor) }
    }

    pub fn insert_tuple<'a>(&mut self, tuple: &'a [u8]) -> Result<SlotIndex> {
        let (slot_index, new_tuple) = self.alloc_tuple(tuple.len())?;
        new_tuple.copy_from_slice(tuple);
        Ok(slot_index)
    }

    pub fn get_tuple_mut(&mut self, index: SlotIndex) -> Option<&mut [u8]> {
        let TupleDescriptor { offset, size } = self.get_tuple_descriptor(index);
        if offset == 0 {
            return None;
        }
        Some(&mut self.data[offset as usize..(offset + size) as usize])
    }
}
