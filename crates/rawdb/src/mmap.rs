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
/// # Safety
/// The caller must ensure:
/// - `offset + data.len()` does not exceed `mmap.len()`
/// - No other references to the written range exist during this call
#[inline]
pub fn write_to_mmap(mmap: &MmapMut, offset: usize, data: &[u8]) {
    let end = offset + data.len();
    debug_assert!(end <= mmap.len(), "write beyond mmap bounds");

    // SAFETY: MmapMut guarantees the memory is valid and writable.
    // We're creating a mutable view for the write operation.
    unsafe {
        let ptr = mmap.as_ptr() as *mut u8;
        std::ptr::copy_nonoverlapping(data.as_ptr(), ptr.add(offset), data.len());
    }
}
