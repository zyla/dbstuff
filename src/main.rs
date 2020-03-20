mod disk_manager;

use tokio::prelude::*;
use std::path::Path;
use std::collections::{HashMap};
use std::sync::RwLock;

struct Page {
    dirty: bool,
    pin_count: usize,
    data: PageData,
}

struct BufferPool {
    disk: DiskManager,
    capacity: usize,
    page_table: HashMap<PageId, Box<Page>>,
}

impl BufferPool {
    fn new(disk: DiskManager, capacity: usize) -> BufferPool {
        BufferPool {
            disk: disk,
            capacity: capacity,
            page_table: HashMap::with_capacity(capacity),
        }
    }

    async fn allocate_page(&mut self) -> io::Result<&mut Page> {
        let frame_id = self.get_free_frame().await?;
        let page = &mut self.frames[frame_id.0];
        page.id = self.disk.allocate_page().await?;
        self.page_table.insert(page.id, frame_id);
        Ok(page)
    }

    async fn get_free_frame(&mut self) -> io::Result<FrameId> {
        if let Some(frame_id) = self.free_frames.pop() {
            return Ok(frame_id)
        }
        loop {}
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let mut disk = DiskManager::open("test.db").await?;
    let mut buf = [0u8; 4096];
    buf[0..3].copy_from_slice(b"Bar");
    disk.write_page(PageId(0), &buf).await?;

    let mut bp = BufferPool::new(disk, 10);
    let page1 = bp.allocate_page().await?;
    let page2 = bp.allocate_page().await?;

    println!("{:?}", &page1.data[..]);
    println!("{:?}", &page2.data[..]);

    Ok(())
}
