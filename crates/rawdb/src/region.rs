use std::{fs::File, mem, sync::Arc};

use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::{Database, Error, Reader, RegionMetadata, Result, WeakDatabase};

/// Named region within a database providing isolated storage space.
///
/// Regions grow dynamically as data is written and can be moved within the
/// database file to optimize space usage. Each region has a unique ID for lookup.
#[derive(Debug, Clone)]
#[must_use = "Region should be stored to access the data"]
pub struct Region(Arc<RegionInner>);

#[derive(Debug)]
pub struct RegionInner {
    db: WeakDatabase,
    index: usize,
    meta: RwLock<RegionMetadata>,
    /// Dirty ranges (start_offset, end_offset) relative to region start.
    /// Merged at flush time to reduce syscalls.
    /// Separate from meta to allow flush without blocking iterators.
    dirty_ranges: Mutex<Vec<(usize, usize)>>,
}

impl Region {
    pub fn new(
        db: &Database,
        id: String,
        index: usize,
        start: usize,
        len: usize,
        reserved: usize,
    ) -> Self {
        Self(Arc::new(RegionInner {
            db: db.weak_clone(),
            index,
            meta: RwLock::new(RegionMetadata::new(id, start, len, reserved)),
            dirty_ranges: Mutex::new(Vec::new()),
        }))
    }

    pub fn from(db: &Database, index: usize, meta: RegionMetadata) -> Self {
        Self(Arc::new(RegionInner {
            db: db.weak_clone(),
            index,
            meta: RwLock::new(meta),
            dirty_ranges: Mutex::new(Vec::new()),
        }))
    }

    /// Creates a reader for zero-copy access to this region's data.
    ///
    /// The Reader holds read locks on both the memory map and region metadata,
    /// blocking writes until dropped. Drop the reader as soon as you're done
    /// reading to avoid blocking other operations.
    #[inline]
    pub fn create_reader(&self) -> Reader {
        Reader::new(self)
    }

    pub fn open_db_read_only_file(&self) -> Result<File> {
        self.db().open_read_only_file()
    }

    /// Appends data to the end of the region.
    ///
    /// The region will automatically grow and relocate if needed.
    /// Data is written to the mmap but not durable until `flush()` is called.
    #[inline]
    pub fn write(&self, data: &[u8]) -> Result<()> {
        self.write_with(data, None, false)
    }

    /// Writes data at a specific offset within the region.
    ///
    /// The offset must be within the current region length.
    /// Data written past the current end will extend the length.
    /// Data is written to the mmap but not durable until `flush()` is called.
    #[inline]
    pub fn write_at(&self, data: &[u8], at: usize) -> Result<()> {
        self.write_with(data, Some(at), false)
    }

    /// Writes values directly to the mmap with dirty range tracking.
    ///
    /// All writes must be within the current region length (no extension).
    /// Tracks dirty ranges to avoid flushing unchanged data.
    ///
    /// - `iter`: Iterator yielding (offset, value) pairs where offset is relative to region start
    /// - `value_len`: The byte size of each value
    /// - `write_fn`: Called for each (value, slice) to serialize the value into the slice
    #[inline]
    pub fn batch_write_each<T, F>(
        &self,
        iter: impl Iterator<Item = (usize, T)>,
        value_len: usize,
        mut write_fn: F,
    ) where
        F: FnMut(&T, &mut [u8]),
    {
        let region_start = self.meta().start();
        let db = self.db();
        let mmap = db.mmap();
        let ptr = mmap.as_ptr() as *mut u8;

        let mut ranges = self.0.dirty_ranges.lock();

        for (offset, value) in iter {
            let abs_offset = region_start + offset;
            let slice = unsafe { std::slice::from_raw_parts_mut(ptr.add(abs_offset), value_len) };
            write_fn(&value, slice);
            ranges.push((offset, offset + value_len));
        }
    }

    /// Truncates the region to the specified length.
    ///
    /// This reduces the logical length but doesn't modify existing data bytes.
    /// The truncated data becomes inaccessible even though the bytes remain in the mmap.
    /// Changes are not durable until `flush()` is called.
    pub fn truncate(&self, from: usize) -> Result<()> {
        // Check current length first (quick read, guard dropped immediately)
        let len = self.meta().len();
        if from == len {
            return Ok(());
        } else if from > len {
            return Err(Error::TruncateInvalid {
                from,
                current_len: len,
            });
        }

        let db = self.db();
        // Lock order: regions -> metadata (top-to-bottom)
        let regions = db.regions();
        let mut meta = self.meta_mut();
        meta.set_len(from);
        meta.write_if_dirty(self.index(), &regions);
        Ok(())
    }

    /// Truncates the region to a specific offset and writes data there.
    ///
    /// This is an atomic truncate + write operation. The final length will be
    /// exactly `at + data.len()` regardless of the previous length.
    /// Changes are not durable until `flush()` is called.
    #[inline]
    pub fn truncate_write(&self, at: usize, data: &[u8]) -> Result<()> {
        self.write_with(data, Some(at), true)
    }

    fn write_with(&self, data: &[u8], at: Option<usize>, truncate: bool) -> Result<()> {
        let db = self.db();
        let index = self.index();
        let meta = self.meta();
        let start = meta.start();
        let reserved = meta.reserved();
        let len = meta.len();
        drop(meta);

        let data_len = data.len();

        // Validate write position if specified
        // Note: checking `at > len` is sufficient since `len <= reserved` is always true
        // Therefore if `at <= len`, then `at <= reserved` must also be true
        if let Some(at_val) = at
            && at_val > len
        {
            return Err(Error::WriteOutOfBounds {
                position: at_val,
                region_len: len,
            });
        }

        let new_len = at.map_or(len + data_len, |at| {
            let new_len = at + data_len;
            if truncate { new_len } else { new_len.max(len) }
        });
        let write_start = start + at.unwrap_or(len);

        // Write to reserved space if possible
        if new_len <= reserved {
            // Write before acquiring meta to avoid deadlock with punch_holes.
            // Lock order: mmap (via db.write) must come before meta.
            db.write(write_start, data);

            // Lock order: regions → meta
            let regions = db.regions();
            let mut meta = self.meta_mut();

            self.mark_dirty_abs(start, write_start, data_len);
            meta.set_len(new_len);
            meta.write_if_dirty(index, &regions);

            return Ok(());
        }

        assert!(new_len > reserved);
        if reserved == 0 {
            panic!(
                "reserved is 0 which would cause infinite loop! start={start}, len={len}, index={index}, new_len={new_len}"
            );
        }
        let mut new_reserved = reserved;
        while new_len > new_reserved {
            new_reserved = new_reserved
                .checked_mul(2)
                .expect("Region size would overflow usize");
        }
        assert!(new_len <= new_reserved);
        let added_reserve = new_reserved - reserved;

        let mut layout = db.layout_mut();

        // If is last continue writing
        if layout.is_last_anything(self) {
            // Release layout BEFORE calling set_min_len to avoid deadlock.
            // set_min_len needs mmap_mut, and another thread may hold mmap read
            // while waiting for layout_mut, causing deadlock if we hold layout here.
            let target_len = start + new_reserved;
            drop(layout);

            db.set_min_len(target_len)?;

            // Re-acquire layout and verify we're still last
            let layout = db.layout();
            if !layout.is_last_anything(self) {
                // Another region was appended while we didn't hold the lock.
                // Fall through to the other code paths by restarting.
                drop(layout);
                return self.write_with(data, at, truncate);
            }
            drop(layout);

            let mut meta = self.meta_mut();
            meta.set_reserved(new_reserved);
            drop(meta);

            db.write(write_start, data);

            self.mark_dirty_abs(start, write_start, data_len);
            // Acquire regions READ lock BEFORE metadata WRITE lock to prevent deadlock.
            let regions = db.regions();
            let mut meta = self.meta_mut();
            meta.set_len(new_len);
            meta.write_if_dirty(index, &regions);

            return Ok(());
        }

        // Expand region to the right if gap is wide enough
        let hole_start = start + reserved;
        if layout
            .get_hole(hole_start)
            .is_some_and(|gap| gap >= added_reserve)
        {
            layout.remove_or_compress_hole(hole_start, added_reserve)?;
            let mut meta = self.meta_mut();
            meta.set_reserved(new_reserved);
            drop(meta);
            drop(layout);

            db.write(write_start, data);

            self.mark_dirty_abs(start, write_start, data_len);
            // Acquire regions READ lock BEFORE metadata WRITE lock to prevent deadlock.
            let regions = db.regions();
            let mut meta = self.meta_mut();
            meta.set_len(new_len);
            meta.write_if_dirty(index, &regions);

            return Ok(());
        }

        // Find hole big enough to move the region
        if let Some(hole_start) = layout.find_smallest_adequate_hole(new_reserved) {
            layout.remove_or_compress_hole(hole_start, new_reserved)?;
            layout.reserve(hole_start, new_reserved);
            drop(layout);

            db.copy(start, hole_start, write_start - start);
            db.write(hole_start + at.unwrap_or(len), data);

            let mut layout = db.layout_mut();
            layout.move_region(hole_start, self)?;
            assert!(layout.take_reserved(hole_start) == Some(new_reserved));

            // Region moved, mark all data as dirty (relative to new start)
            self.mark_dirty(0, new_len);
            // Lock order: layout (held) → regions → meta
            let regions = db.regions();
            let mut meta = self.meta_mut();
            meta.set_start(hole_start);
            meta.set_reserved(new_reserved);
            meta.set_len(new_len);
            meta.write_if_dirty(index, &regions);

            return Ok(());
        }

        // Allocate at end of file
        let new_start = layout.len();
        let target_len = new_start + new_reserved;
        // Release layout BEFORE calling set_min_len to avoid deadlock.
        drop(layout);

        db.set_min_len(target_len)?;

        // Re-acquire layout and reserve space
        let mut layout = db.layout_mut();
        // Verify new_start is still valid (another thread may have appended)
        let current_len = layout.len();
        if current_len != new_start {
            // State changed, restart to pick the right path
            drop(layout);
            return self.write_with(data, at, truncate);
        }
        layout.reserve(new_start, new_reserved);
        drop(layout);

        db.copy(start, new_start, write_start - start);
        db.write(new_start + at.unwrap_or(len), data);

        let mut layout = db.layout_mut();
        layout.move_region(new_start, self)?;
        assert!(layout.take_reserved(new_start) == Some(new_reserved));

        // Region moved, mark all data as dirty (relative to new start)
        self.mark_dirty(0, new_len);
        // Lock order: layout (held) → regions → meta
        let regions = db.regions();
        let mut meta = self.meta_mut();
        meta.set_start(new_start);
        meta.set_reserved(new_reserved);
        meta.set_len(new_len);
        meta.write_if_dirty(index, &regions);

        Ok(())
    }

    /// Renames the region to a new ID.
    ///
    /// The new ID must not already be in use.
    /// Changes are not durable until `flush()` is called.
    pub fn rename(&self, new_id: &str) -> Result<()> {
        let old_id = self.meta().id().to_string();
        let db = self.db();
        let mut regions = db.regions_mut();
        let mut meta = self.meta_mut();
        let index = self.index();
        regions.rename(&old_id, new_id)?;
        meta.set_id(new_id.to_string());
        meta.write_if_dirty(index, &regions);
        Ok(())
    }

    /// Removes the region from the database.
    ///
    /// The space is marked as a pending hole that will become reusable after
    /// the next `flush()`. This consumes the region to prevent use-after-free.
    pub fn remove(self) -> Result<()> {
        let db = self.db();
        // Lock order: layout → regions
        let mut layout = db.layout_mut();
        let mut regions = db.regions_mut();
        layout.remove_region(&self)?;
        regions.remove(&self)?;
        Ok(())
    }

    /// Flushes this region's dirty data and metadata to disk.
    ///
    /// Flushes if any data writes or metadata-only changes (truncate, rename) were made.
    /// Returns `Ok(true)` if anything was flushed, `Ok(false)` if nothing was dirty.
    pub fn flush(&self) -> Result<bool> {
        let db = self.db();
        let dirty_ranges = self.take_dirty_ranges();

        let data_flushed = if !dirty_ranges.is_empty() {
            // Lock order: mmap before meta, so release meta before acquiring mmap
            let region_start = self.meta().start();
            let mmap = db.mmap();
            for (dirty_start, dirty_end) in &dirty_ranges {
                mmap.flush_range(region_start + dirty_start, dirty_end - dirty_start)?;
            }
            true
        } else {
            false
        };

        // Lock order: regions → meta
        let regions = db.regions();
        let meta = self.meta();
        let meta_flushed = meta.flush(self.index(), &regions)?;

        Ok(data_flushed || meta_flushed)
    }

    #[inline(always)]
    pub fn arc(&self) -> &Arc<RegionInner> {
        &self.0
    }

    #[inline(always)]
    pub fn index(&self) -> usize {
        self.0.index
    }

    #[inline(always)]
    pub fn meta(&self) -> RwLockReadGuard<'_, RegionMetadata> {
        self.0.meta.read()
    }

    #[inline(always)]
    fn meta_mut(&self) -> RwLockWriteGuard<'_, RegionMetadata> {
        self.0.meta.write()
    }

    #[inline(always)]
    pub fn db(&self) -> Database {
        self.0.db.upgrade()
    }

    /// Marks a range as dirty (needing flush).
    /// `offset` is relative to region start.
    #[inline]
    pub fn mark_dirty(&self, offset: usize, len: usize) {
        self.0.dirty_ranges.lock().push((offset, offset + len));
    }

    /// Marks a range as dirty using absolute file positions.
    /// Converts to relative offsets internally.
    #[inline]
    fn mark_dirty_abs(&self, region_start: usize, abs_start: usize, len: usize) {
        let offset = abs_start - region_start;
        self.mark_dirty(offset, len);
    }

    /// Takes and returns dirty ranges, clearing the internal Vec and releasing memory.
    /// Concurrent writes after this call will create new ranges for next flush.
    #[inline]
    pub(crate) fn take_dirty_ranges(&self) -> Vec<(usize, usize)> {
        mem::take(&mut *self.0.dirty_ranges.lock())
    }
}
