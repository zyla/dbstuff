use crate::table_page;
use buffer_pool::buffer_pool::{BufferPool, Result};
use buffer_pool::disk_manager::PageId;
use table_page::{SlotIndex, TablePage};

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

    pub async fn insert_tuple<'a>(&self, tuple: &'a [u8]) -> Result<(PageId, SlotIndex)> {
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
                        drop(table_page);
                        drop(page);

                        let mut new_table_page = TablePage::new(new_page.data().write().await);
                        let slot_index = new_table_page
                            .insert_tuple(tuple)
                            .expect("Freshly allocated table is full");
                        new_page.dirty();
                        return Ok((new_page.id(), slot_index));
                    }
                }
            }
        }
    }
}
