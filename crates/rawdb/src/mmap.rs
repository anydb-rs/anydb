use std::fs::File;

use memmap2::{MmapMut, MmapOptions};

use crate::Result;

/// Creates a mutable memory map for the given file.
#[inline]
pub fn create_mmap(file: &File) -> Result<MmapMut> {
    Ok(unsafe { MmapOptions::new().map_mut(file)? })
}

/// Writes data to a memory-mapped region.
///
/// # Panics
/// Panics if `offset + data.len()` exceeds `mmap.len()` or if the addition overflows.
///
/// # Safety Requirements
/// - No other references to the written range should exist during this call
#[inline]
pub fn write_to_mmap(mmap: &MmapMut, offset: usize, data: &[u8]) {
    let end = offset
        .checked_add(data.len())
        .expect("offset + data.len() overflow");
    assert!(end <= mmap.len(), "write beyond mmap bounds: end={end}, mmap.len()={}", mmap.len());

    // SAFETY: MmapMut guarantees the memory is valid and writable.
    // We've verified the bounds above.
    unsafe {
        let ptr = mmap.as_ptr() as *mut u8;
        std::ptr::copy_nonoverlapping(data.as_ptr(), ptr.add(offset), data.len());
    }
}
