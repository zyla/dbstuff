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
    #[allow(dead_code)]
    lsn: u32,
    metadata_size: u16,
    free_space_pointer: u16,
    // TODO: instead of pub(crate), expose a function to truncate the tuple vector
    pub(crate) tuple_count: u16,
    // next: a sequence of TupleDescriptor structs
}

#[derive(Debug)]
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

impl<T, Meta> Deref for TupleBlockPage<T, Meta> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

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

    pub fn free_space_after_compaction(&self) -> usize {
        PAGE_SIZE
            - mem::size_of::<PageHeader>()
            - mem::size_of::<Meta>()
            - self.tuple_count() * mem::size_of::<TupleDescriptor>()
            - self.total_tuple_size()
    }

    pub fn total_tuple_size(&self) -> usize {
        let mut tuple_total = 0;
        for index in 0..self.tuple_count() {
            tuple_total += self.get_tuple_descriptor(index).size as usize;
        }
        tuple_total
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

    #[cfg(test)]
    pub(crate) fn dump_tuples(&self) -> Vec<Vec<u8>> {
        (0..self.tuple_count())
            .map(|index| self.get_tuple(index).unwrap().to_vec())
            .collect()
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
        mem::transmute(self.data.as_ptr())
    }

    pub fn metadata_mut(&mut self) -> &mut Meta {
        unsafe { mem::transmute(self.data[PAGE_SIZE - mem::size_of::<Meta>()..].as_ptr()) }
    }

    pub fn compact(&mut self) {
        let mut data_copy = Box::new(*self.data);
        let copy = TupleBlockPage::<&mut PageData, Meta>::from_existing(&mut data_copy);
        unsafe { self.header_mut() }.tuple_count = 0;
        unsafe { self.header_mut() }.free_space_pointer =
            (PAGE_SIZE as u16) - self.header().metadata_size;
        for index in 0..copy.tuple_count() {
            println!("free space; {:?}", self.free_space());
            println!(
                "compact:inserting tuple {:?}",
                copy.get_tuple_descriptor(index)
            );
            self.insert_tuple_at(index, copy.get_tuple(index).expect("dead tuple unhandled"))
                .expect("page should not be full during compaction");
        }
    }

    /// Delete a tuple, shifting the tuples after it one slot left.
    pub fn delete_tuple(&mut self, index: SlotIndex) {
        let tuple_count = self.tuple_count();
        assert!(index < tuple_count);
        self.data.copy_within(
            Self::tuple_descriptor_offset(index + 1)..Self::tuple_descriptor_offset(tuple_count),
            Self::tuple_descriptor_offset(index),
        );
        unsafe { self.header_mut() }.tuple_count = (tuple_count - 1) as u16;
    }

    pub fn alloc_tuple_at(&mut self, index: SlotIndex, size: usize) -> Result<&mut [u8]> {
        let space_needed = size + mem::size_of::<TupleDescriptor>();
        if self.free_space() < space_needed && self.free_space_after_compaction() >= space_needed {
            self.compact();
        }
        if self.free_space() < space_needed {
            return Err(Error::PageFull);
        }
        let end = self.header().free_space_pointer;
        let start = end - (size as u16);
        // move tuple descriptors after insertion point one index to the right
        let tuple_count = self.tuple_count();
        self.data.copy_within(
            Self::tuple_descriptor_offset(index)..Self::tuple_descriptor_offset(tuple_count),
            Self::tuple_descriptor_offset(index + 1),
        );

        unsafe { self.header_mut() }.free_space_pointer = start;
        unsafe { self.header_mut() }.tuple_count = self.header().tuple_count + 1;

        // write the new tuple descriptor
        self.set_tuple_descriptor(
            index as usize,
            TupleDescriptor {
                offset: start,
                size: (end - start),
            },
        );
        Ok(&mut self.data[start as usize..end as usize])
    }

    fn set_tuple_descriptor(&mut self, index: usize, descriptor: TupleDescriptor) {
        assert!(index < self.tuple_count());
        let offset = Self::tuple_descriptor_offset(index);
        unsafe { (self.data[offset..].as_mut_ptr() as *mut TupleDescriptor).write(descriptor) }
    }

    fn tuple_descriptor_offset(index: usize) -> usize {
        mem::size_of::<PageHeader>() + index * mem::size_of::<TupleDescriptor>()
    }

    pub fn insert_tuple<'a>(&mut self, tuple: &'a [u8]) -> Result<SlotIndex> {
        let index = self.tuple_count();
        self.insert_tuple_at(index, tuple)?;
        Ok(index)
    }

    pub fn insert_tuple_at<'a>(&mut self, index: SlotIndex, tuple: &'a [u8]) -> Result<()> {
        let new_tuple = self.alloc_tuple_at(index, tuple.len())?;
        new_tuple.copy_from_slice(tuple);
        Ok(())
    }

    pub fn get_tuple_mut(&mut self, index: SlotIndex) -> Option<&mut [u8]> {
        let TupleDescriptor { offset, size } = self.get_tuple_descriptor(index);
        if offset == 0 {
            return None;
        }
        Some(&mut self.data[offset as usize..(offset + size) as usize])
    }
}
