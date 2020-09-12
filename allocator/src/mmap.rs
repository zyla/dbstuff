use std::ptr;
use std::io::Error;

/// A wrapper around libc mmap.
pub fn mmap(len: usize) -> std::io::Result<*mut u8> {
    let ptr = unsafe {
        libc::mmap(
            ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            -1,
            0,
        )
    } as *mut u8;

    if ptr == (-1isize) as *mut u8 {
        Err(Error::last_os_error())
    } else {
        Ok(ptr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Returns RSS of current process in bytes.
    /// Assumes current page size is 4k.
    fn get_rss_bytes() -> usize {
        procinfo::pid::statm_self().unwrap().resident * PAGE_SIZE
    }

    const BIG: usize = 0x1_0000_0000;
    const PAGE_SIZE: usize = 4096;

    struct Mapping(*mut u8);

    impl Drop for Mapping {
        fn drop(&mut self) {
            unsafe { libc::munmap(self.0 as *mut libc::c_void, BIG) };
        }
    }

    #[test]
    fn alloc_big_chunk_no_commit() {
        let m = Mapping(mmap(BIG).unwrap());
        assert!(get_rss_bytes() < BIG);
        drop(m);
    }

    #[test]
    fn commit_some() {
        let m = Mapping(mmap(BIG).unwrap());
        const N: usize = 16 * 1024 * 1024;
        for i in 0..(N/PAGE_SIZE) {
            unsafe {
                ptr::write(m.0.offset((i * PAGE_SIZE) as isize), 1);
            }
        }
        assert!(get_rss_bytes() >= N);
        drop(m);
    }

    #[test]
    fn read_does_not_commit_and_returns_0() {
        let m = Mapping(mmap(BIG).unwrap());
        const N: usize = 10 * 1024 * 1024;
        for i in 0..(N/PAGE_SIZE) {
            unsafe {
                assert_eq!(ptr::read(m.0.offset((i * PAGE_SIZE) as isize)), 0);
            }
        }
        assert!(get_rss_bytes() < N);
        drop(m);
    }
}
