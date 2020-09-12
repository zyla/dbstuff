#[cfg(test)]
mod mmap_tests;

use mmap::{MapOption::*, MemoryMap};
use std::mem;
use std::ptr;

struct Allocator {
    mem: MemoryMap,
    head: Option<Box<Chunk>>,
}

impl Allocator {
    fn new(len: usize) -> std::io::Result<Self> {
        Ok(Self {
            mem: MemoryMap::new(len, &[MapReadable, MapWritable]).map_err(to_io_err)?,
            head: Some(Box::new(Chunk {
                offset: 0,
                len,
                next: None,
            })),
        })
    }

    fn alloc(&mut self, size: usize) -> Option<*mut u8> {
        let mut chunk_ptr: &mut Option<Box<Chunk>> = &mut self.head;
        let (data, next) = loop {
            match chunk_ptr.as_deref_mut() {
                Some(mut chunk) => {
                    if chunk.len < size {
                        chunk_ptr = &mut chunk.next;
                        continue;
                    }
                    let data = unsafe { self.mem.data().offset(chunk.offset as isize) };
                    chunk.offset += size;
                    chunk.len -= size;
                    if chunk.len == 0 {
                        break (data, Some(mem::take(&mut chunk.next)));
                    } else {
                        break (data, None);
                    }
                }
                None => return None,
            };
        };
        if let Some(replacement) = next {
            *chunk_ptr = replacement;
        }
        return Some(data);
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
