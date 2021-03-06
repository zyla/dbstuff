#![cfg(not(loom))]

use crate::disk_manager::{DiskManager, PageData, PageId, PAGE_SIZE};
use async_trait::async_trait;
use std::convert::TryInto;
use std::path::Path;
use tokio::fs;
use tokio::prelude::*;

pub struct DiskManagerFile {
    file: fs::File,
    num_pages: usize,
}

impl DiskManagerFile {
    pub async fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .await?;
        let meta = file.metadata().await?;
        Ok(DiskManagerFile {
            file,
            num_pages: meta.len() as usize / PAGE_SIZE,
        })
    }
}

#[async_trait]
impl DiskManager for DiskManagerFile {
    async fn write_page(&mut self, page_id: PageId, data: &PageData) -> io::Result<()> {
        self.file
            .seek(std::io::SeekFrom::Start(
                (page_id.0 as usize * PAGE_SIZE) as u64,
            ))
            .await?;
        self.file.write_all(data).await
    }

    async fn read_page(&mut self, page_id: PageId, data: &mut PageData) -> io::Result<()> {
        self.file
            .seek(std::io::SeekFrom::Start(
                (page_id.0 as usize * PAGE_SIZE) as u64,
            ))
            .await?;
        self.file.read_exact(data).await?;
        Ok(())
    }

    async fn allocate_page(&mut self) -> io::Result<PageId> {
        let id = PageId(self.num_pages.try_into().expect("PageId overflow"));
        self.num_pages += 1;
        Ok(id)
    }
}
