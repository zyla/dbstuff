use crate::btree::{BTree, NodeDump};

use buffer_pool::buffer_pool::{BufferPool, Result};
use buffer_pool::disk_manager::PAGE_SIZE;
use buffer_pool::disk_manager_mem::DiskManagerMem;

#[tokio::test]
async fn test_new() -> Result<()> {
    let buffer_pool = BufferPool::new(Box::new(DiskManagerMem::new()), 20);
    let btree = BTree::new(&buffer_pool).await?;
    assert_debug_snapshot!(btree.dump_tree().await?, @r###"
    Leaf(
        [],
    )
    "###);
    Ok(())
}

#[tokio::test]
async fn test_insert_one() -> Result<()> {
    let buffer_pool = BufferPool::new(Box::new(DiskManagerMem::new()), 20);
    let btree = BTree::new(&buffer_pool).await?;
    btree.insert(&[1], &[100]).await?;
    assert_debug_snapshot!(btree.dump_tree().await?, @r###"
    Leaf(
        [
            (
                [
                    1,
                ],
                [
                    100,
                ],
            ),
        ],
    )
    "###);
    Ok(())
}

#[tokio::test]
async fn test_insert_many_should_sort() -> Result<()> {
    let buffer_pool = BufferPool::new(Box::new(DiskManagerMem::new()), 20);
    let btree = BTree::new(&buffer_pool).await?;
    btree.insert(&[1], &[101]).await?;
    btree.insert(&[3], &[103]).await?;
    btree.insert(&[2], &[102]).await?;
    btree.insert(&[0], &[100]).await?;
    assert_debug_snapshot!(btree.dump_tree().await?, @r###"
    Leaf(
        [
            (
                [
                    0,
                ],
                [
                    100,
                ],
            ),
            (
                [
                    1,
                ],
                [
                    101,
                ],
            ),
            (
                [
                    2,
                ],
                [
                    102,
                ],
            ),
            (
                [
                    3,
                ],
                [
                    103,
                ],
            ),
        ],
    )
    "###);
    Ok(())
}

#[tokio::test]
async fn test_page_split() -> Result<()> {
    let buffer_pool = BufferPool::new(Box::new(DiskManagerMem::new()), 20);
    let btree = BTree::new(&buffer_pool).await?;
    btree.insert(&[1; PAGE_SIZE / 2], &[101]).await?;
    btree.insert(&[2; PAGE_SIZE / 2], &[102]).await?;
    assert_eq!(
        btree.dump_tree().await?,
        NodeDump::Internal(vec![
            (
                vec![],
                NodeDump::Leaf(vec![(vec![1; PAGE_SIZE / 2], vec![101])])
            ),
            (
                vec![2; PAGE_SIZE / 2],
                NodeDump::Leaf(vec![(vec![2; PAGE_SIZE / 2], vec![102])])
            )
        ])
    );
    Ok(())
}
