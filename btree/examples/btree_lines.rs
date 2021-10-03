use btree::btree::BTree;

use buffer_pool::buffer_pool::{BufferPool, Result};
use buffer_pool::disk_manager_mem::DiskManagerMem;

use futures::stream::TryStreamExt;
use tokio::io;
use tokio::io::AsyncBufReadExt;

#[tokio::main]
async fn main() -> Result<()> {
    let buffer_pool = BufferPool::new(Box::new(DiskManagerMem::new()), 20);
    let btree = BTree::new(&buffer_pool).await?;
    let mut lines = io::BufReader::new(io::stdin()).lines();
    while let Some(line) = lines.try_next().await? {
        btree.insert(line.as_bytes(), &[]).await?;
    }
    println!("{:#?}", btree.dump_tree().await?);
    Ok(())
}
