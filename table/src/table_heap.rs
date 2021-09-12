use crate::table_page;
use buffer_pool::buffer_pool::{BufferPool, PinnedPageReadGuard, Result};
use buffer_pool::disk_manager::PageId;
use std::ops::Deref;
use table_page::{SlotIndex, TablePage};

pub type TupleId = (PageId, SlotIndex);

pub struct TableHeap<'b> {
    buffer_pool: &'b BufferPool,
    first_page_id: PageId,
}

impl<'b> TableHeap<'b> {
    pub fn from_existing(buffer_pool: &'b BufferPool, first_page_id: PageId) -> Self {
        TableHeap {
            buffer_pool,
            first_page_id,
        }
    }

    /// Initialize a new table heap in the given storage.
    pub async fn new(buffer_pool: &'b BufferPool) -> Result<TableHeap<'b>> {
        let page = buffer_pool.allocate_page().await?;
        TablePage::new(page.data().write().await);
        page.dirty();
        Ok(TableHeap {
            buffer_pool,
            first_page_id: page.id(),
        })
    }

    pub async fn insert_tuple<'a>(&self, tuple: &'a [u8]) -> Result<TupleId> {
        let mut page_id = self.first_page_id;
        loop {
            let page = self.buffer_pool.get_page(page_id).await?;
            let mut table_page = TablePage::from_existing(page.data().write().await);
            match table_page.insert_tuple(tuple) {
                Ok(slot_index) => {
                    page.dirty();
                    return Ok((page.id(), slot_index));
                }
                Err(table_page::PageFull) => {
                    page_id = table_page.get_next_page_id();

                    if !page_id.is_valid() {
                        let new_page = self.buffer_pool.allocate_page().await?;
                        table_page.set_next_page_id(new_page.id());
                        page.dirty();
                        let new_page_data = new_page.data().write().await;

                        // Note: we can only unlock the previous page after locking the next one -
                        // otherwise we would be publishing a pointer to an uninitialized page.
                        drop(table_page);
                        drop(page);

                        let mut new_table_page = TablePage::new(new_page_data);
                        let slot_index = new_table_page
                            .insert_tuple(tuple)
                            .expect("Tuple too big to fit on a new page");
                        new_page.dirty();
                        return Ok((new_page.id(), slot_index));
                    }
                }
            }
        }
    }

    pub async fn iter(&self) -> Result<TableIter<'_>> {
        self.iter_at((self.first_page_id, 0)).await
    }

    pub async fn get_tuple(&self, tid: TupleId) -> Result<TupleReadGuard<'_>> {
        Ok(TupleReadGuard {
            iter: self.iter_at(tid).await?,
        })
    }

    async fn iter_at(&self, tid: TupleId) -> Result<TableIter<'_>> {
        Ok(TableIter {
            table: self,
            slot_index: tid.1,
            page: self.read_page(tid.0).await?,
        })
    }

    async fn read_page(&self, page_id: PageId) -> Result<TablePage<PinnedPageReadGuard<'b>>> {
        let page = self.buffer_pool.get_page(page_id).await?;
        Ok(TablePage::from_existing(page.read().await))
    }
}

pub struct TableIter<'b> {
    table: &'b TableHeap<'b>,
    slot_index: SlotIndex,
    page: TablePage<PinnedPageReadGuard<'b>>,
}

impl<'b> TableIter<'b> {
    pub async fn next<'t>(&'t mut self) -> Result<Option<(TupleId, &'t [u8])>> {
        while self.slot_index >= self.page.get_tuple_count() {
            let next = self.page.get_next_page_id();
            if !next.is_valid() {
                return Ok(None);
            }
            self.page = self.table.read_page(next).await?;
            self.slot_index = 0;
        }
        let slot_index = self.slot_index;
        self.slot_index += 1;
        Ok(Some((
            (self.page.unwrap().id(), slot_index),
            self.page.get_tuple(slot_index).expect("invalid slot index"),
        )))
    }
}

pub struct TupleReadGuard<'b> {
    iter: TableIter<'b>,
}

impl Deref for TupleReadGuard<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.iter
            .page
            .get_tuple(self.iter.slot_index)
            .expect("invalid slot index")
    }
}
