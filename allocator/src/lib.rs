#[cfg(test)]
mod mmap_tests;

use std::mem;
use std::ptr;
use mmap::{MapOption::*, MemoryMap};

struct Allocator {
    mem: MemoryMap,
    head: Option<Box<Chunk>>,
}

impl Allocator {
    fn new(len: usize) -> std::io::Result<Self> {
        Ok(Self {
            mem: MemoryMap::new(len, &[MapReadable, MapWritable]).map_err(to_io_err)?,
            head: Some(Box::new(Chunk { offset: 0, len, next: None })),
        })
    }

    fn alloc(&mut self, size: usize) -> Option<*mut u8> {
        let mut chunk_ptr: &mut Option<Box<Chunk>> = &mut self.head;
        while let Some(mut chunk) = chunk_ptr.as_deref_mut() {
            if chunk.len >= size {
                let start = chunk.offset;
                chunk.offset += size;
                chunk.len -= size;
                if chunk.len == 0 {
                    let next = mem::take(&mut chunk.next);
                    drop(chunk);
                    unsafe {
                        ptr::write(chunk_ptr as *mut Option<Box<Chunk>>, next);
                    }
                }
                return Some(unsafe { self.mem.data().offset(start as isize) })
            }
            chunk_ptr = &mut chunk.next;
        }
        None
    }
}

struct Chunk {
    offset: usize,
    len: usize,
    next: Option<Box<Chunk>>,
}

fn to_io_err(_e: mmap::MapError) -> std::io::Error {
    // TODO: figure out the real error mapping
    std::io::ErrorKind::Other.into()
}
