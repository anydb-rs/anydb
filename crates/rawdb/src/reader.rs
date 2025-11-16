use memmap2::MmapMut;
use parking_lot::RwLockReadGuard;

use crate::RegionMetadata;

/// Zero-copy reader for accessing region data from memory-mapped storage.
///
/// Holds locks on the memory map and region metadata during its lifetime,
/// preventing concurrent modifications. Should be dropped as soon as reading
/// is complete to avoid blocking writes.
#[derive(Debug)]
pub struct Reader<'a> {
    mmap: RwLockReadGuard<'a, MmapMut>,
    meta: RwLockReadGuard<'a, RegionMetadata>,
}

impl<'a> Reader<'a> {
    #[inline]
    pub fn new(
        mmap: RwLockReadGuard<'a, MmapMut>,
        meta: RwLockReadGuard<'a, RegionMetadata>,
    ) -> Self {
        Self { mmap, meta }
    }

    /// Reads data from the region without bounds checking.
    ///
    /// # Safety
    /// The caller must ensure `offset + len` is within the region's length.
    /// Reading beyond the region's bounds is undefined behavior.
    #[inline(always)]
    pub fn unchecked_read(&self, offset: usize, len: usize) -> &[u8] {
        let start = self.meta.start() + offset;
        let end = start + len;
        &self.mmap[start..end]
    }

    /// Reads a slice of data from the region at the given offset.
    ///
    /// # Panics
    /// Panics if `offset + len` exceeds the region's length.
    #[inline(always)]
    pub fn read(&self, offset: usize, len: usize) -> &[u8] {
        assert!(offset + len <= self.meta.len());
        self.unchecked_read(offset, len)
    }

    /// Returns a slice containing all data in the region.
    #[inline(always)]
    pub fn read_all(&self) -> &[u8] {
        self.read(0, self.meta.len())
    }

    /// Returns a slice from the offset to the end of the mmap.
    ///
    /// This allows reading beyond the region boundary for performance-critical
    /// sequential access patterns, but the offset must still be within the region.
    ///
    /// # Panics
    /// Panics if the offset exceeds the region's length.
    #[inline(always)]
    pub fn prefixed(&self, offset: usize) -> &[u8] {
        assert!(offset <= self.meta.len(), "Offset {} exceeds region length {}", offset, self.meta.len());
        let start = self.meta.start() + offset;
        &self.mmap[start..]
    }
}
