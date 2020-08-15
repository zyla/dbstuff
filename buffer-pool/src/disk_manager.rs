#![cfg(not(loom))]

use std::path::Path;
use tokio::fs;
use tokio::prelude::*;

pub struct DiskManager {
    file: fs::File,
    num_pages: usize,
}

pub const PAGE_SIZE: usize = 4096;

#[derive(PartialEq, Eq, Hash, Clone, Copy, Debug)]
pub struct PageId(pub usize);

pub type PageData = [u8; PAGE_SIZE];

impl DiskManager {
    pub async fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .await?;
        let meta = file.metadata().await?;
        Ok(DiskManager {
            file,
            num_pages: meta.len() as usize / PAGE_SIZE,
        })
    }

    pub async fn write_page(&mut self, page_id: PageId, data: &PageData) -> io::Result<()> {
        self.file
            .seek(std::io::SeekFrom::Start((page_id.0 * PAGE_SIZE) as u64))
            .await?;
        self.file.write_all(data).await
    }

    pub async fn read_page(&mut self, page_id: PageId, data: &mut PageData) -> io::Result<()> {
        self.file
            .seek(std::io::SeekFrom::Start((page_id.0 * PAGE_SIZE) as u64))
            .await?;
        self.file.read_exact(data).await?;
        Ok(())
    }

    pub async fn allocate_page(&mut self) -> io::Result<PageId> {
        let id = PageId(self.num_pages);
        self.num_pages += 1;
        Ok(id)
    }
}
