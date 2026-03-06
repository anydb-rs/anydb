use std::{fs::File, mem, sync::Arc};

use log::{debug, trace};
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
pub(crate) struct RegionInner {
    db: WeakDatabase,
    index: usize,
    meta: RwLock<RegionMetadata>,
    /// Dirty bounding box (min_offset, max_offset) relative to region start.
    /// (usize::MAX, 0) means clean (no dirty data).
    /// Separate from meta to allow flush without blocking iterators.
    dirty_bounds: Mutex<(usize, usize)>,
}

impl Region {

    pub(crate) fn new(
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
            dirty_bounds: Mutex::new((usize::MAX, 0)),
        }))
    }

    pub(crate) fn from(db: &Database, index: usize, meta: RegionMetadata) -> Self {
        Self(Arc::new(RegionInner {
            db: db.weak_clone(),
            index,
            meta: RwLock::new(meta),
            dirty_bounds: Mutex::new((usize::MAX, 0)),
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
    ///
    /// # Panics
    /// Panics if any write would exceed the region length or mmap bounds.
    #[inline]
    pub fn batch_write_each<T, F>(
        &self,
        iter: impl Iterator<Item = (usize, T)>,
        value_len: usize,
        mut write_fn: F,
    ) where
        F: FnMut(&T, &mut [u8]),
    {
        let meta = self.meta();
        let region_start = meta.start();
        let region_len = meta.len();
        drop(meta);

        let db = self.db();
        let mmap = db.mmap();
        let ptr = mmap.as_ptr() as *mut u8;

        // Track bounding box instead of per-value ranges
        let mut dirty_start = usize::MAX;
        let mut dirty_end = 0usize;

        for (offset, value) in iter {
            let end_offset = offset
                .checked_add(value_len)
                .expect("offset + value_len overflow");
            assert!(
                end_offset <= region_len,
                "batch_write_each: write at offset {offset} with len {value_len} exceeds region length {region_len}"
            );

            // SAFETY: region_start + offset + value_len <= region_start + region_len <= mmap_len
            // (region fits within mmap is an invariant maintained by write_with/set_min_len)
            let abs_offset = region_start + offset;
            let slice = unsafe { std::slice::from_raw_parts_mut(ptr.add(abs_offset), value_len) };
            write_fn(&value, slice);
            dirty_start = dirty_start.min(offset);
            dirty_end = dirty_end.max(end_offset);
        }

        if dirty_start < dirty_end {
            let mut bounds = self.0.dirty_bounds.lock();
            bounds.0 = bounds.0.min(dirty_start);
            bounds.1 = bounds.1.max(dirty_end);
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

    #[inline]
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

        // Offset within region where write starts (append position if at=None)
        let write_offset = at.unwrap_or(len);
        let new_len = at.map_or(len + data_len, |at| {
            let new_len = at + data_len;
            if truncate { new_len } else { new_len.max(len) }
        });
        let write_start = start + write_offset;

        // Write to reserved space if possible
        if new_len <= reserved {
            db.write(write_start, data);
            self.mark_dirty_abs(start, write_start, data_len);

            // Fast path: in-place update doesn't change length, skip metadata locks
            if new_len != len {
                // Lock order: regions → meta
                let regions = db.regions();
                let mut meta = self.meta_mut();
                meta.set_len(new_len);
                meta.write_if_dirty(index, &regions);
            }

            return Ok(());
        }

        // Need to grow: validate reserved is non-zero to avoid infinite loop
        if reserved == 0 {
            return Err(Error::InvariantViolation(format!(
                "reserved is 0 which would cause infinite loop! start={start}, len={len}, index={index}, new_len={new_len}"
            )));
        }

        // Double reserved until it fits new_len
        let mut new_reserved = reserved;
        while new_len > new_reserved {
            new_reserved = new_reserved
                .checked_mul(2)
                .ok_or(Error::RegionSizeOverflow {
                    current: new_reserved,
                    requested: new_len,
                })?;
        }
        let added_reserve = new_reserved - reserved;

        // Amount of existing data to copy when relocating:
        // - truncate=true: only copy up to write position (discard rest)
        // - truncate=false: copy all existing data to preserve bytes after write
        let copy_len = if truncate { write_offset } else { len };

        trace!(
            "{}: '{}' write_with acquiring layout_mut (need to grow)",
            db,
            self.meta().id()
        );
        let mut layout = db.layout_mut();

        // If is last, extend reserved in place (no relocation needed)
        if layout.is_last_anything(self) {
            let target_len = start + new_reserved;
            // Update reserved BEFORE dropping layout. Layout::len() reads
            // region.meta().reserved() for the last region, so other threads
            // see the expanded end immediately — no retry needed.
            // Lock order: layout (held) → meta — correct per ordering rules.
            {
                let mut meta = self.meta_mut();
                meta.set_reserved(new_reserved);
            }
            // Release layout BEFORE calling set_min_len to avoid deadlock.
            // set_min_len needs mmap_mut, and another thread may hold mmap read
            // while waiting for layout_mut, causing deadlock if we hold layout here.
            drop(layout);

            if let Err(e) = db.set_min_len(target_len) {
                let mut meta = self.meta_mut();
                meta.set_reserved(reserved);
                return Err(e);
            }

            db.write(write_start, data);

            self.mark_dirty_abs(start, write_start, data_len);
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

        // Relocate: find a hole or allocate at end of file
        let new_start = if let Some(hole_start) = layout.find_smallest_adequate_hole(new_reserved) {
            debug!(
                "{}: '{}' relocating to hole at {} (need {})",
                db,
                self.meta().id(),
                hole_start,
                new_reserved
            );
            layout.remove_or_compress_hole(hole_start, new_reserved)?;
            layout.reserve(hole_start, new_reserved);
            drop(layout);
            hole_start
        } else {
            let new_start = layout.len();
            let target_len = new_start + new_reserved;
            debug!(
                "{}: '{}' allocating at end {} (need {})",
                db,
                self.meta().id(),
                new_start,
                new_reserved
            );
            // Reserve space in the layout BEFORE dropping the lock.
            // This ensures other threads see the updated layout.len() immediately,
            // preventing thundering-herd retries when many regions grow at once.
            layout.reserve(new_start, new_reserved);
            // Release layout BEFORE calling set_min_len to avoid deadlock.
            // set_min_len needs mmap_mut, and another thread may hold mmap read
            // while waiting for layout_mut, causing deadlock if we hold layout here.
            drop(layout);

            if let Err(e) = db.set_min_len(target_len) {
                let mut layout = db.layout_mut();
                layout.take_reserved(new_start);
                return Err(e);
            }
            new_start
        };

        db.copy(start, new_start, copy_len)?;
        db.write(new_start + write_offset, data);

        trace!(
            "{}: '{}' write_with re-acquiring layout_mut (after relocation)",
            db,
            self.meta().id()
        );
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
        debug!("{}: rename '{}' -> '{}'", db, old_id, new_id);
        trace!(
            "{}: rename '{}' -> '{}' acquiring regions_mut",
            db, old_id, new_id
        );
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
        let id = self.meta().id().to_string();
        debug!("{}: '{}' remove", db, id);
        trace!("{}: '{}' remove acquiring layout_mut", db, id);
        // Lock order: layout → regions
        let mut layout = db.layout_mut();
        trace!("{}: '{}' remove acquiring regions_mut", db, id);
        let mut regions = db.regions_mut();
        trace!("{}: '{}' remove got locks", db, id);
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
        let dirty_bounds = self.take_dirty_bounds();

        // Lock order: regions → mmap → meta
        // Must acquire regions before mmap to avoid deadlock with punch_holes
        let regions = db.regions();

        let data_flushed = if let Some((min, max)) = dirty_bounds {
            let region_start = self.meta().start();
            let mmap = db.mmap();
            if let Err(e) = mmap.flush_async_range(region_start + min, max - min) {
                drop(mmap);
                self.restore_dirty_bounds(min, max);
                return Err(e.into());
            }
            true
        } else {
            false
        };

        let meta = self.meta();
        let meta_flushed = meta.flush(self.index(), &regions)?;

        // Data MUST be durable before metadata — if we crash after metadata sync
        // but before data sync, metadata could reference unwritten data.
        if data_flushed || meta_flushed {
            db.file().sync_data()?;
            regions.sync_data()?;
        }

        Ok(data_flushed || meta_flushed)
    }

    /// Returns true if two Region handles point to the same underlying region.
    #[inline(always)]
    pub fn ptr_eq(&self, other: &Region) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    #[inline(always)]
    pub(crate) fn arc(&self) -> &Arc<RegionInner> {
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
    pub(crate) fn meta_mut(&self) -> RwLockWriteGuard<'_, RegionMetadata> {
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
        let end = offset + len;
        let mut bounds = self.0.dirty_bounds.lock();
        bounds.0 = bounds.0.min(offset);
        bounds.1 = bounds.1.max(end);
    }

    /// Marks a range as dirty using absolute file positions.
    /// Converts to relative offsets internally.
    #[inline]
    fn mark_dirty_abs(&self, region_start: usize, abs_start: usize, len: usize) {
        let offset = abs_start - region_start;
        self.mark_dirty(offset, len);
    }

    /// Takes the dirty bounding box, resetting to clean.
    /// Returns `Some((min, max))` if dirty, `None` if clean.
    #[inline]
    pub(crate) fn take_dirty_bounds(&self) -> Option<(usize, usize)> {
        let mut bounds = self.0.dirty_bounds.lock();
        if bounds.0 < bounds.1 {
            Some(mem::replace(&mut *bounds, (usize::MAX, 0)))
        } else {
            None
        }
    }

    /// Restores dirty bounds (used when flush fails to allow retry).
    #[inline]
    pub(crate) fn restore_dirty_bounds(&self, min: usize, max: usize) {
        let mut bounds = self.0.dirty_bounds.lock();
        bounds.0 = bounds.0.min(min);
        bounds.1 = bounds.1.max(max);
    }
}
