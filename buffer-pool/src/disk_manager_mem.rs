use crate::disk_manager::{DiskManager, PageData, PageId, PAGE_SIZE};
use async_trait::async_trait;
use std::io;
use std::convert::TryInto;

pub struct DiskManagerMem {
    pages: Vec<PageData>,
}

impl DiskManagerMem {
    pub fn new() -> Self {
        DiskManagerMem { pages: vec![] }
    }
}

impl Default for DiskManagerMem {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DiskManager for DiskManagerMem {
    async fn write_page(&mut self, page_id: PageId, data: &PageData) -> io::Result<()> {
        self.pages[page_id.0 as usize] = *data;
        Ok(())
    }

    async fn read_page(&mut self, page_id: PageId, data: &mut PageData) -> io::Result<()> {
        *data = self.pages[page_id.0 as usize];
        Ok(())
    }

    async fn allocate_page(&mut self) -> io::Result<PageId> {
        let id = PageId(self.pages.len().try_into().expect("PageId overflow"));
        self.pages.push([0; PAGE_SIZE]);
        Ok(id)
    }
}
