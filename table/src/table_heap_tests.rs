use crate::hexdump::pretty_hex;
use crate::table_page;
use crate::table_heap::{TableHeap};
use buffer_pool::{disk_manager_mem::DiskManagerMem, disk_manager::PageId, buffer_pool::{Result, BufferPool}};

#[tokio::test]
async fn insert_and_get() -> Result<()> {
    let buffer_pool = BufferPool::new(Box::new(DiskManagerMem::new()), 10);
    let table = TableHeap::new(&buffer_pool).await?;
    let tid = table.insert_tuple(b"Hello World").await?;
    assert_eq!(&*table.get_tuple(tid).await?, b"Hello World");
    Ok(())
}

#[tokio::test]
async fn insert_many_and_iter() -> Result<()> {
    let buffer_pool = BufferPool::new(Box::new(DiskManagerMem::new()), 5);
    let table = TableHeap::new(&buffer_pool).await?;
    const N_PAGES: usize = 10;
    let mut n_tuples: usize = 0;
    loop {
        let tid = table.insert_tuple(format!("Tuple tuple {}", n_tuples).as_bytes()).await?;
        n_tuples += 1;
        if tid.1 >= N_PAGES {
            break;
        }
    }
    let mut iter = table.iter().await?;
    for i in 0..n_tuples {
        let tuple = iter.next().await?.expect("Expected tuple, got nothin'");
        assert_eq!(tuple.1, format!("Tuple tuple {}", i).as_bytes());
    }
    Ok(())
}
