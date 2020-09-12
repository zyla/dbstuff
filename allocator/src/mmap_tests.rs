use std::ptr;
use mmap::{MemoryMap, MapOption::*};

/// Returns RSS of current process in bytes.
/// Assumes current page size is 4k.
fn get_rss_bytes() -> usize {
    procinfo::pid::statm_self().unwrap().resident * PAGE_SIZE
}

const BIG: usize = 0x1_0000_0000;
const PAGE_SIZE: usize = 4096;

#[test]
fn alloc_big_chunk_no_commit() {
    let m = MemoryMap::new(BIG, &[MapReadable, MapWritable]).unwrap();
    assert!(get_rss_bytes() < BIG);
    drop(m);
}

#[test]
fn commit_some() {
    let m = MemoryMap::new(BIG, &[MapReadable, MapWritable]).unwrap();
    const N: usize = 16 * 1024 * 1024;
    for i in 0..(N/PAGE_SIZE) {
        unsafe {
            ptr::write(m.data().offset((i * PAGE_SIZE) as isize), 1);
        }
    }
    assert!(get_rss_bytes() >= N);
    drop(m);
}

#[test]
fn read_does_not_commit_and_returns_0() {
    let m = MemoryMap::new(BIG, &[MapReadable, MapWritable]).unwrap();
    const N: usize = 10 * 1024 * 1024;
    for i in 0..(N/PAGE_SIZE) {
        unsafe {
            assert_eq!(ptr::read(m.data().offset((i * PAGE_SIZE) as isize)), 0);
        }
    }
    assert!(get_rss_bytes() < N);
    drop(m);
}
