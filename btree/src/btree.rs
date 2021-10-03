#![allow(dead_code)]

use crate::page;
use crate::page::TupleBlockPage;
#[cfg(test)]
use async_recursion::async_recursion;
use buffer_pool::buffer_pool::{BufferPool, PinnedPageReadGuard, PinnedPageWriteGuard, Result};
use buffer_pool::disk_manager::{PageData, PageId};
use std::cmp::Ordering;
use std::mem;
use std::ops::{Deref, DerefMut};

struct TreeMetadata {
    root_page_id: PageId,
}

pub struct BTree<'a> {
    buffer_pool: &'a BufferPool,
    meta_page_id: PageId,
}

impl<'a> BTree<'a> {
    pub async fn new(buffer_pool: &'a BufferPool) -> Result<BTree<'a>> {
        let meta_page = buffer_pool.allocate_page().await?;
        let root_page = buffer_pool.allocate_page().await?;
        let meta_page_data = meta_page.data().write().await;
        let initial_metadata = TreeMetadata {
            root_page_id: root_page.id(),
        };
        unsafe { (meta_page_data.as_ptr() as *mut TreeMetadata).write(initial_metadata) }
        meta_page.dirty();

        let root_page_data = root_page.data().write().await;
        NodePage::new_leaf(root_page_data);
        root_page.dirty();

        Ok(Self {
            buffer_pool,
            meta_page_id: meta_page.id(),
        })
    }

    /// Inserts the given key into the tree.
    /// When already there, overwrites the value.
    pub async fn insert(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let (meta, mut page) = self.get_root_page_write().await?;
        let parent = Parent::MetaPage(meta);

        if !page.metadata().is_leaf() {
            unimplemented!("Only leaf search implemented for now");
        }

        match page.binary_search(key) {
            SearchResult::Found(_) => {
                unimplemented!("replacing existing key");
            }
            SearchResult::NotFound(insert_index) => {
                let tuple_size = leaf_tuple::size(key, value);
                match page.alloc_tuple_at(insert_index, tuple_size) {
                    Ok(tuple) => {
                        leaf_tuple::write(tuple, key, value);
                        page.dirty();
                        Ok(())
                    }
                    Err(page::Error::PageFull) => {
                        // FIXME: crash in case the tuple is too big to fit in any page - we don't
                        // support overflow
                        let new_sibling_page =
                            self.buffer_pool.allocate_page().await?.write().await;
                        let mut new_sibling = NodePage::new_leaf(new_sibling_page);
                        let split_index = page.get_split_index(insert_index, tuple_size);
                        let num_tuples = page.tuple_count() + 1;

                        for target_index in split_index..num_tuples {
                            if target_index == insert_index {
                                // this is the new tuple
                                match new_sibling
                                    .alloc_tuple_at(insert_index - split_index, tuple_size)
                                {
                                    Ok(tuple) => {
                                        leaf_tuple::write(tuple, key, value);
                                    }
                                    Err(page::Error::PageFull) => {
                                        panic!("new tuple does not fit after split")
                                    }
                                }
                            } else {
                                let source_index = if target_index > insert_index {
                                    target_index - 1
                                } else {
                                    target_index
                                };
                                new_sibling
                                    .insert_tuple(page.get_tuple(source_index).expect("dead tuple"))
                                    .expect("old tuple does not fit after split");
                            }
                        }

                        if insert_index < split_index {
                            // Inserted tuple lands on the old page.
                            unsafe { page.header_mut() }.tuple_count = (split_index - 1) as u16;
                            let tuple = page
                                .alloc_tuple_at(insert_index, tuple_size)
                                .expect("new tuple does not fit after page split");
                            leaf_tuple::write(tuple, key, value);
                        } else {
                            unsafe { page.header_mut() }.tuple_count = split_index as u16;
                        }

                        new_sibling.page.dirty();

                        match parent {
                            Parent::InternalPage => {
                                // TODO: insert into the parent
                                unimplemented!("splitting non-root page")
                            }
                            Parent::MetaPage(mut meta_page) => {
                                // We are splitting the root page. Create a new internal page to
                                // replace the root.
                                let new_root_page =
                                    self.buffer_pool.allocate_page().await?.write().await;
                                let mut new_root = NodePage::new_internal(
                                    new_root_page,
                                    page.metadata().level + 1,
                                    page.id(),
                                );
                                let split_key = new_sibling.get_tuple_key(0);
                                let sibling_pointer_tuple = new_root
                                    .alloc_tuple_at(1, pivot_tuple::size(split_key))
                                    .expect("no space for key in new root");
                                pivot_tuple::write(
                                    sibling_pointer_tuple,
                                    new_sibling.id(),
                                    split_key,
                                );
                                new_root.page.dirty();

                                meta_page.metadata_mut().root_page_id = new_root.id();
                                meta_page.data.dirty();
                            }
                        }

                        Ok(())
                    }
                }
            }
        }
    }

    async fn get_root_page(&self) -> Result<NodePage<PinnedPageReadGuard<'a>>> {
        let meta_page = self.buffer_pool.get_page(self.meta_page_id).await?;
        let meta_page_data = meta_page.data().read().await;
        let meta: &TreeMetadata = unsafe { slice_to_struct(&meta_page_data[0..]) };
        Ok(self.get_node_page(meta.root_page_id).await?)
    }

    async fn get_node_page(&self, page_id: PageId) -> Result<NodePage<PinnedPageReadGuard<'a>>> {
        Ok(NodePage::from_existing(
            self.buffer_pool.get_page(page_id).await?.read().await,
        ))
    }

    async fn get_root_page_write(
        &self,
    ) -> Result<(
        MetaPage<PinnedPageWriteGuard<'a>>,
        NodePage<PinnedPageWriteGuard<'a>>,
    )> {
        let meta_page_ = self.buffer_pool.get_page(self.meta_page_id).await?;
        let meta_page = MetaPage::from_existing(meta_page_.write().await);
        let root_page_data = self
            .buffer_pool
            .get_page(meta_page.metadata().root_page_id)
            .await?
            .write()
            .await;
        Ok((meta_page, NodePage::from_existing(root_page_data)))
    }

    #[cfg(test)]
    pub async fn dump_tree(&self) -> Result<NodeDump> {
        self.dump_node(self.get_root_page().await?).await
    }

    #[cfg(test)]
    #[async_recursion]
    async fn dump_node(&self, page: NodePage<PinnedPageReadGuard<'a>>) -> Result<NodeDump> {
        if page.metadata().is_leaf() {
            Ok(NodeDump::Leaf(
                page.dump_tuples()
                    .iter()
                    .map(|tuple| {
                        (
                            leaf_tuple::get_key(tuple).to_vec(),
                            leaf_tuple::get_value(tuple).to_vec(),
                        )
                    })
                    .collect(),
            ))
        } else {
            let mut result = vec![];
            for tuple in page.dump_tuples() {
                let child = self
                    .get_node_page(pivot_tuple::get_header(&tuple).downlink_pointer)
                    .await?;
                result.push((
                    pivot_tuple::get_key(&tuple).to_vec(),
                    self.dump_node(child).await?,
                ));
            }
            Ok(NodeDump::Internal(result))
        }
    }
}

enum Parent<T> {
    MetaPage(MetaPage<T>),
    InternalPage, // TODO
}

#[cfg(test)]
#[derive(Debug, PartialEq, Eq)]
pub enum NodeDump {
    Internal(Vec<(Vec<u8>, NodeDump)>),
    Leaf(Vec<(Vec<u8>, Vec<u8>)>),
}

struct MetaPage<T> {
    data: T,
}

impl<T: Deref<Target = PageData>> MetaPage<T> {
    pub fn from_existing(data: T) -> Self {
        Self { data }
    }

    pub fn metadata(&self) -> &TreeMetadata {
        unsafe { slice_to_struct(&self.data[0..]) }
    }
}

impl<T: DerefMut<Target = PageData>> MetaPage<T> {
    pub fn metadata_mut(&mut self) -> &mut TreeMetadata {
        unsafe { slice_to_struct_mut(&mut self.data[0..]) }
    }
}

struct NodePage<T> {
    page: TupleBlockPage<T, NodeMetadata>,
}

impl<T> Deref for NodePage<T> {
    type Target = TupleBlockPage<T, NodeMetadata>;

    fn deref(&self) -> &Self::Target {
        &self.page
    }
}

impl<T> DerefMut for NodePage<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.page
    }
}

impl<T: Deref<Target = PageData>> NodePage<T> {
    fn from_existing(data: T) -> Self {
        Self {
            page: TupleBlockPage::from_existing(data),
        }
    }

    fn metadata(&self) -> &NodeMetadata {
        self.page.metadata()
    }

    fn binary_search(&self, key: &[u8]) -> SearchResult {
        let mut start = 0;
        let mut end = self.page.tuple_count();

        while start < end {
            let mid = (start + end) / 2;
            let tuple_key = self.get_tuple_key(mid);
            match tuple_key.cmp(key) {
                Ordering::Greater => {
                    end = mid;
                }
                Ordering::Less => {
                    start = mid + 1;
                }
                Ordering::Equal => {
                    return SearchResult::Found(mid);
                }
            }
        }

        SearchResult::NotFound(start)
    }

    fn get_tuple_key(&self, index: usize) -> &[u8] {
        let tuple = self.page.get_tuple(index).expect("found null tuple");
        if self.metadata().is_leaf() {
            leaf_tuple::get_key(tuple)
        } else {
            pivot_tuple::get_key(tuple)
        }
    }

    /// Compute tuple index at which to split the page. Tuples >= this index will go to the new
    /// page. The tuple bytes will be divided evenly (as much as possible) between the pages.
    ///
    /// During calculation a new tuple to be inserted at `insert_index` is taken into account.
    /// The returned index is shifted after `insert_index`.
    ///
    /// TODO: in some cases this will result in the new tuple not fitting on the page. Handle these
    /// cases.
    pub fn get_split_index(&self, insert_index: usize, tuple_size: usize) -> usize {
        let total_size = self.page.total_tuple_size() + tuple_size;
        let split_at_byte = total_size / 2;
        let mut bytes_so_far = 0;
        // After insert there will be one more tuple
        let num_tuples = self.page.tuple_count() + 1;
        for target_index in 0..num_tuples {
            if target_index == insert_index {
                // this is the new tuple
                bytes_so_far += tuple_size;
            } else {
                // If we're after the inserted tuple, be sure to read tuple size from the original
                // offset, not the one after insert
                let source_index = if target_index > insert_index {
                    target_index - 1
                } else {
                    target_index
                };
                bytes_so_far += self
                    .page
                    .get_tuple(source_index)
                    .expect("expected no dead tuples")
                    .len();
            }
            if bytes_so_far > split_at_byte {
                return target_index;
            }
        }
        panic!("get_split_index didn't reach split_at_byte");
    }
}

#[cfg(test)]
mod get_split_index_tests {
    use super::*;
    use buffer_pool::disk_manager::PAGE_SIZE;

    fn make_page(tuples: &[usize]) -> NodePage<Box<PageData>> {
        let data = Box::new([0; PAGE_SIZE]);
        let mut page = NodePage::new_leaf(data);
        for (index, tuple_size) in tuples.iter().enumerate() {
            page.alloc_tuple_at(index, *tuple_size).unwrap();
        }
        page
    }

    #[test]
    fn split_before_insert() {
        assert_eq!(make_page(&[1]).get_split_index(1, 1), 1);
    }
    #[test]
    fn split_after_insert() {
        assert_eq!(make_page(&[1]).get_split_index(0, 1), 1);
    }

    // In both cases below we can't fit both tuples on a single page.
    #[test]
    fn split_large_tuple_1() {
        assert_eq!(
            make_page(&[PAGE_SIZE / 2]).get_split_index(0, PAGE_SIZE / 2 - 100),
            1
        );
    }
    #[test]
    #[ignore = "case not handled yet"]
    fn split_large_tuple_2() {
        assert_eq!(
            make_page(&[PAGE_SIZE / 2 - 100]).get_split_index(0, PAGE_SIZE / 2),
            1
        );
    }
}

enum SearchResult {
    Found(usize),
    NotFound(usize),
}

impl<T: DerefMut<Target = PageData>> NodePage<T> {
    fn new_leaf(data: T) -> Self {
        Self {
            page: TupleBlockPage::new(data, &NodeMetadata { level: 0 }),
        }
    }

    fn new_internal(data: T, level: u8, first_child: PageId) -> Self {
        let mut page = TupleBlockPage::new(data, &NodeMetadata { level });
        let tuple = page
            .alloc_tuple_at(0, pivot_tuple::size(&[]))
            .expect("no space for -inf tuple");
        pivot_tuple::write(tuple, first_child, &[]);
        Self { page }
    }
}

/// Interpret the slice bytes as a data structure.
unsafe fn slice_to_struct<T>(buffer: &[u8]) -> &T {
    assert!(buffer.len() >= mem::size_of::<T>());
    mem::transmute(buffer.as_ptr())
}

/// Interpret the slice bytes as a data structure.
unsafe fn slice_to_struct_mut<T>(buffer: &mut [u8]) -> &mut T {
    assert!(buffer.len() >= mem::size_of::<T>());
    mem::transmute(buffer.as_ptr())
}

mod leaf_tuple {
    use super::*;

    #[derive(Debug, Clone, Copy)]
    pub struct Header {
        pub key_size: u16,
    }

    impl Header {
        pub const SIZE: usize = mem::size_of::<Header>();
    }

    pub fn size(key: &[u8], value: &[u8]) -> usize {
        Header::SIZE + key.len() + value.len()
    }

    /// Write a leaf tuple into the provided slice.
    /// The slice must have size at least that returned by `size()`.
    pub fn write(tuple: &mut [u8], key: &[u8], value: &[u8]) {
        *unsafe { slice_to_struct_mut(tuple) } = Header {
            key_size: key.len() as u16,
        };
        tuple[Header::SIZE..Header::SIZE + key.len()].copy_from_slice(key);
        tuple[Header::SIZE + key.len()..].copy_from_slice(value);
    }

    pub fn get_header(tuple: &[u8]) -> &Header {
        unsafe { slice_to_struct(tuple) }
    }

    pub fn get_key(tuple: &[u8]) -> &[u8] {
        let key_len = get_header(tuple).key_size as usize;
        &tuple[Header::SIZE..Header::SIZE + key_len]
    }

    pub fn get_value(tuple: &[u8]) -> &[u8] {
        let key_len = get_header(tuple).key_size as usize;
        &tuple[Header::SIZE + key_len..]
    }
}

mod pivot_tuple {
    use super::*;

    #[derive(Debug, Clone, Copy)]
    pub struct Header {
        pub downlink_pointer: PageId,
    }

    impl Header {
        pub const SIZE: usize = mem::size_of::<Header>();
    }

    pub fn size(key: &[u8]) -> usize {
        Header::SIZE + key.len()
    }

    /// Write a leaf tuple into the provided slice.
    /// The slice must have size at least that returned by `size()`.
    pub fn write(tuple: &mut [u8], downlink_pointer: PageId, key: &[u8]) {
        *unsafe { slice_to_struct_mut(tuple) } = Header { downlink_pointer };
        tuple[Header::SIZE..].copy_from_slice(key);
    }

    pub fn get_header(tuple: &[u8]) -> &Header {
        unsafe { slice_to_struct(tuple) }
    }

    pub fn get_key(tuple: &[u8]) -> &[u8] {
        &tuple[Header::SIZE..]
    }
}

#[derive(Debug, Clone, Copy)]
struct NodeMetadata {
    level: u8,
}

impl NodeMetadata {
    fn is_leaf(&self) -> bool {
        self.level == 0
    }
}
