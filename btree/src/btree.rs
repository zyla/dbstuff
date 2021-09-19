use crate::page::TupleBlockPage;
use buffer_pool::buffer_pool::{BufferPool, Result, PinnedPageReadGuard, PinnedPageWriteGuard};
use buffer_pool::disk_manager::{PageData, PageId, PAGE_SIZE};
use std::ops::{Deref, DerefMut};
use std::cmp::Ordering;
use std::mem;

struct TreeMetadata {
    root_page_id: PageId,
}

struct BTree<'a> {
    buffer_pool: &'a BufferPool,
    meta_page_id: PageId,
}

impl<'a> BTree<'a> {
    pub async fn new(buffer_pool: &'a BufferPool) -> Result<BTree<'a>> {
        let meta_page = buffer_pool.allocate_page().await?;
        let root_page = buffer_pool.allocate_page().await?;
        let meta_page_data = meta_page.data().write().await;
        unsafe{(meta_page_data.as_ptr() as *mut TreeMetadata).write(TreeMetadata{ root_page_id: root_page.id() })}
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
        let page = self.get_root_page_write().await?;

        if !page.metadata().is_leaf() {
            unimplemented!("Only leaf search implemented for now");
        }

        match page.binary_search(key) {
            SearchResult::Found(_) => {
                unimplemented!("replacing existing key");
            }
            SearchResult::NotFound(insert_index) => {
                // FIXME: allocate at insert_index
                match page.alloc_tuple(LeafTupleHeader::SIZE + key.len() + value.len()) {
                    Ok((index, tuple)) => {
                        *slice_to_struct_mut(tuple) = LeafTupleHeader { key_size: key.len() as u8 };
                        tuple[LeafTupleHeader::SIZE..LeafTupleHeader::SIZE + key.len()].copy_from_slice(key);
                        tuple[LeafTupleHeader::SIZE+key.len()..].copy_from_slice(value);
                        page.dirty();
                        Ok(())
                    },
                    Err(PageFull) => {
                        unimplemented!("page split")
                    }
                }
            }
        }
    }

    async fn get_root_page(&self) -> Result<NodePage<PinnedPageReadGuard<'a>>> {
        let meta_page = self.buffer_pool.get_page(self.meta_page_id).await?;
        let meta_page_data = meta_page.data().read().await;
        let meta: &TreeMetadata = unsafe{slice_to_struct(&meta_page_data[0..])};
        let root_page_data = self.buffer_pool.get_page(meta.root_page_id).await?.read().await;
        Ok(NodePage::from_existing(root_page_data))
    }

    async fn get_root_page_write(&self) -> Result<NodePage<PinnedPageWriteGuard<'a>>> {
        let meta_page = self.buffer_pool.get_page(self.meta_page_id).await?;
        let meta_page_data = meta_page.data().read().await;
        let meta: &TreeMetadata = unsafe{slice_to_struct(&meta_page_data[0..])};
        let root_page_data = self.buffer_pool.get_page(meta.root_page_id).await?.write().await;
        Ok(NodePage::from_existing(root_page_data))
    }
}

struct NodePage<T> {
    page: TupleBlockPage<T, NodeMetadata>
}

impl<T> Deref for NodePage<T> {
    type Target = TupleBlockPage<T, NodeMetadata>;

    fn deref(&self) -> &Self::Target {
        &self.page
    }
}

impl<T: Deref<Target=PageData>> NodePage<T> {
    fn from_existing(data: T) -> Self {
        Self { page: TupleBlockPage::from_existing(data) }
    }

    fn metadata(&self) -> &NodeMetadata {
        self.page.metadata()
    }
}

enum SearchResult {
    Found(usize),
    NotFound(usize),
}

impl<T: DerefMut<Target=PageData>> NodePage<T> {
    fn new_leaf(data: T) -> Self {
        Self { page: TupleBlockPage::new(data, &NodeMetadata { level: 0 }) }
    }

    fn binary_search(&self, key: &[u8]) -> SearchResult {
        if !self.metadata().is_leaf() {
            unimplemented!("Only leaf search implemented for now");
        }

        let mut start = 0;
        let mut end = self.page.tuple_count();

        while start < end {
            let mid = (start + end) / 2;
            let tuple_key = self.get_tuple_key(mid);
            match tuple_key.cmp(key) {
                Ordering::Less => {
                    end = mid;
                },
                Ordering::Greater => {
                    start = mid + 1;
                },
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
            let header = unsafe{slice_to_struct::<LeafTupleHeader>(tuple)};
            &tuple[mem::size_of::<LeafTupleHeader>()..mem::size_of::<LeafTupleHeader>()+(header.key_size as usize)]
        } else {
            unimplemented!("only leaf tuples implemented");
        }
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

#[derive(Debug, Clone, Copy)]
struct LeafTupleHeader {
    key_size: u8,
}

impl LeafTupleHeader {
    const SIZE: usize = mem::size_of::<LeafTupleHeader>();
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
