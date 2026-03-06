use memmap2::MmapMut;
use parking_lot::RwLockReadGuard;

use crate::{Database, Region};

/// Zero-copy reader for accessing region data from memory-mapped storage.
///
/// Holds a lock on the memory map and a snapshot of region start/len.
/// The snapshot is taken at reader creation time, providing snapshot isolation
/// and avoiding lock ordering deadlocks with writers.
///
/// # Important: Lock Duration
///
/// **Drop this reader as soon as possible.** While held, this blocks:
/// - `set_min_len` (file growth)
/// - `compact` / `punch_holes` (final sync phase)
///
/// Long-lived readers can cause other operations to hang waiting for the lock.
/// If you need to keep data around, copy it out of the reader first.
///
/// The Reader owns references to the Database and Region to ensure the
/// underlying data structures remain valid for the lifetime of the guards.
#[must_use = "Reader holds locks and should be used for reading"]
pub struct Reader {
    // SAFETY: Field order is critical! Rust drops struct fields in declaration order
    // (first declared = first dropped). The guard MUST be declared BEFORE the Arcs
    // so it is dropped FIRST — releasing the lock while the RwLock is still alive.
    // DO NOT REORDER these fields without understanding the safety implications.
    mmap: RwLockReadGuard<'static, MmapMut>,
    start: usize,
    len: usize,
    _region: Region,
    _db: Database,
}

impl Reader {
    /// Creates a new Reader for the given region.
    ///
    /// Snapshots region start/len (two usizes, no heap allocation) to provide
    /// snapshot isolation and avoid lock ordering deadlocks with writers.
    ///
    /// # Safety
    /// This uses transmute to extend the mmap guard lifetime to 'static. This is safe because:
    /// - The guard borrows from a RwLock inside an Arc-wrapped Database
    /// - Reader owns a clone of that Arc (_db field)
    /// - The Arc is dropped AFTER the guard (field declaration order)
    /// - Therefore the RwLock remains valid for the guard's entire lifetime
    #[inline]
    pub(crate) fn new(region: &Region) -> Self {
        let db = region.db();
        let region = region.clone();

        // Snapshot start/len under a brief read lock (no heap allocation).
        // This avoids lock ordering deadlocks with writers who need region.meta().write()
        // while holding pages.write().
        let meta = region.meta();
        let start = meta.start();
        let len = meta.len();
        drop(meta);

        // SAFETY: The guard borrows from a RwLock inside the Arc-wrapped Database.
        // We store a clone of this Arc in the Reader struct. Rust drops fields in
        // declaration order (first declared = first dropped). Since mmap is declared
        // BEFORE _db, mmap drops first — while the Arc (_db) is still alive.
        // This guarantees the RwLock remains valid for the guard's entire lifetime.
        let mmap: RwLockReadGuard<'static, MmapMut> = unsafe { std::mem::transmute(db.mmap()) };

        Self {
            _db: db,
            _region: region,
            start,
            len,
            mmap,
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
        self.start
    }

    /// Returns the length of data in the region.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len
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
