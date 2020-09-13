use async_trait::async_trait;
use std::io;

pub const PAGE_SIZE: usize = 4096;

#[derive(PartialEq, Eq, Hash, Clone, Copy, Debug)]
pub struct PageId(pub u32);

pub const INVALID_PAGE_ID: PageId = PageId(0xffff_ffff);

impl PageId {
    pub fn invalid() -> Self {
        INVALID_PAGE_ID
    }

    pub fn is_valid(self) -> bool {
        self != INVALID_PAGE_ID
    }
}

pub type PageData = [u8; PAGE_SIZE];

#[async_trait]
pub trait DiskManager: Send + Sync {
    async fn write_page(&mut self, page_id: PageId, data: &PageData) -> io::Result<()>;
    async fn read_page(&mut self, page_id: PageId, data: &mut PageData) -> io::Result<()>;
    async fn allocate_page(&mut self) -> io::Result<PageId>;
}
