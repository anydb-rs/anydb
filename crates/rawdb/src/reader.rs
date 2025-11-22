use memmap2::MmapMut;
use parking_lot::RwLockReadGuard;

use crate::{Database, Region, RegionMetadata};

/// Zero-copy reader for accessing region data from memory-mapped storage.
///
/// Holds locks on the memory map and region metadata during its lifetime,
/// preventing concurrent modifications. Should be dropped as soon as reading
/// is complete to avoid blocking writes.
///
/// The Reader owns references to the Database and Region to ensure the
/// underlying data structures remain valid for the lifetime of the guards.
#[must_use = "Reader holds locks and should be used for reading"]
pub struct Reader {
    // These fields keep the Arc-wrapped structures alive, ensuring the guards remain valid.
    // They must be declared AFTER the guards so they are dropped AFTER the guards.
    // (Rust drops fields in declaration order)
    mmap: RwLockReadGuard<'static, MmapMut>,
    meta: RwLockReadGuard<'static, RegionMetadata>,
    _db: Database,
    _region: Region,
}

impl Reader {
    /// Creates a new Reader for the given region.
    ///
    /// # Safety
    /// This uses transmute to extend guard lifetimes to 'static. This is safe because:
    /// - The guards borrow from RwLocks inside Arc-wrapped structures
    /// - Reader owns clones of those Arcs (_db and _region fields)
    /// - The Arcs are dropped AFTER the guards (field declaration order)
    /// - Therefore the RwLocks remain valid for the guards' entire lifetime
    #[inline]
    pub(crate) fn new(region: &Region) -> Self {
        let db = region.db();
        let region = region.clone();

        // SAFETY: The guards borrow from RwLocks inside the Arc-wrapped Database and Region.
        // We store clones of these Arcs in the Reader struct, and Rust drops fields in
        // declaration order, so the guards are dropped before the Arcs. This guarantees
        // the RwLocks remain valid for the entire lifetime of the guards.
        let mmap: RwLockReadGuard<'static, MmapMut> = unsafe { std::mem::transmute(db.mmap()) };
        let meta: RwLockReadGuard<'static, RegionMetadata> =
            unsafe { std::mem::transmute(region.meta()) };

        Self {
            mmap,
            meta,
            _db: db,
            _region: region,
        }
    }

    /// Reads data from the region without bounds checking.
    ///
    /// # Safety
    /// The caller must ensure `offset + len` is within the region's length.
    /// Reading beyond the region's bounds is undefined behavior.
    #[inline(always)]
    pub fn unchecked_read(&self, offset: usize, len: usize) -> &[u8] {
        let start = self.start() + offset;
        let end = start + len;
        &self.mmap[start..end]
    }

    /// Reads a slice of data from the region at the given offset.
    ///
    /// # Panics
    /// Panics if `offset + len` exceeds the region's length.
    #[inline(always)]
    pub fn read(&self, offset: usize, len: usize) -> &[u8] {
        assert!(offset + len <= self.len());
        self.unchecked_read(offset, len)
    }

    /// Returns the starting offset of this region in the database file.
    #[inline(always)]
    fn start(&self) -> usize {
        self.meta.start()
    }

    /// Returns the length of data in the region.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.meta.len()
    }

    /// Returns true if the region is empty.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns a slice containing all data in the region.
    #[inline(always)]
    pub fn read_all(&self) -> &[u8] {
        self.read(0, self.len())
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
        assert!(
            offset <= self.len(),
            "Offset {} exceeds region length {}",
            offset,
            self.len()
        );
        let start = self.start() + offset;
        &self.mmap[start..]
    }
}
