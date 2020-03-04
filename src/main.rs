use tokio::fs;
use tokio::prelude::*;
use std::path::Path;
use std::collections::{HashMap};

struct DiskManager {
    file: fs::File,
    num_pages: usize,
}

const PAGE_SIZE: usize = 4096;

#[derive(PartialEq, Eq, Hash, Clone, Copy)]
struct PageId(usize);

type PageData = [u8; PAGE_SIZE];

impl DiskManager {
    async fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = fs::OpenOptions::new().read(true).write(true).create(true).open(path).await?;
        let meta = file.metadata().await?;
        Ok(DiskManager {
            file: file,
            num_pages: meta.len() as usize / PAGE_SIZE,
        })
    }

    async fn write_page(&mut self, page_id: PageId, data: &PageData) -> io::Result<()> {
        self.file.seek(std::io::SeekFrom::Start((page_id.0 * PAGE_SIZE) as u64)).await?;
        self.file.write_all(data).await
    }

    async fn read_page(&mut self, page_id: PageId, data: &mut PageData) -> io::Result<()> {
        self.file.seek(std::io::SeekFrom::Start((page_id.0 * PAGE_SIZE) as u64)).await?;
        self.file.read_exact(data).await?;
        Ok(())
    }

    async fn allocate_page(&mut self) -> io::Result<PageId> {
        let id = PageId(self.num_pages);
        self.num_pages += 1;
        Ok(id)
    }
}

struct FrameId(usize);

struct Page {
    id: PageId,
    dirty: bool,
    pin_count: usize,
    data: PageData,
}

struct BufferPool {
    disk: DiskManager,
    page_table: HashMap<PageId, FrameId>,
    frames: Vec<Page>,
    free_frames: Vec<FrameId>,
}

impl BufferPool {
    fn new(disk: DiskManager, capacity: usize) -> BufferPool {
        let mut frames = Vec::with_capacity(capacity);
        let mut free_frames = Vec::with_capacity(capacity);
        for i in 0..capacity {
            frames.push(Page {
                id: PageId(std::usize::MAX),
                dirty: false,
                pin_count: 0,
                data: [0; PAGE_SIZE]
            });
            free_frames.push(FrameId(i));
        }
        BufferPool {
            disk: disk,
            page_table: HashMap::with_capacity(capacity),
            frames: frames,
            free_frames: free_frames,
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
