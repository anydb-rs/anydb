use std::{fs::File, sync::Arc};

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
    /// Dirty range (start_offset, end_offset) relative to region start.
    /// Separate from meta to allow flush without blocking iterators.
    dirty_range: Mutex<Option<(usize, usize)>>,
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
            dirty_range: Mutex::new(None),
        }))
    }

    pub fn from(db: &Database, index: usize, meta: RegionMetadata) -> Self {
        Self(Arc::new(RegionInner {
            db: db.weak_clone(),
            index,
            meta: RwLock::new(meta),
            dirty_range: Mutex::new(None),
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

    /// Truncates the region to the specified length.
    ///
    /// This reduces the logical length but doesn't modify existing data bytes.
    /// The truncated data becomes inaccessible even though the bytes remain in the mmap.
    /// Changes are not durable until `flush()` is called.
    pub fn truncate(&self, from: usize) -> Result<()> {
        let mut meta = self.meta_mut();
        let len = meta.len();
        if from == len {
            return Ok(());
        } else if from > len {
            return Err(Error::TruncateInvalid {
                from,
                current_len: len,
            });
        }
        meta.set_len(from);
        meta.write(self.index(), &self.db().regions());
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
            // info!(
            //     "Write {data_len} bytes to {region_index} reserved space at {write_start} (start = {start}, at = {at:?}, len = {len})"
            // );

            // For appends (at.is_none()), write data before acquiring meta lock to reduce lock time.
            // For positioned writes (at.is_some()), acquire meta lock first to ensure atomicity.
            if at.is_none() {
                db.write(write_start, data);
            }

            let mut meta = self.meta_mut();

            if at.is_some() {
                db.write(write_start, data);
            }

            self.extend_dirty_abs(start, write_start, data_len);
            meta.set_len(new_len);
            meta.write(index, &db.regions());

            return Ok(());
        }

        assert!(new_len > reserved);
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
            // info!("{region_index} Append to file at {write_start}");

            db.set_min_len(start + new_reserved)?;
            let mut meta = self.meta_mut();
            meta.set_reserved(new_reserved);
            drop(meta);
            drop(layout);

            db.write(write_start, data);

            self.extend_dirty_abs(start, write_start, data_len);
            let mut meta = self.meta_mut();
            meta.set_len(new_len);
            meta.write(index, &db.regions());

            return Ok(());
        }

        // Expand region to the right if gap is wide enough
        let hole_start = start + reserved;
        if layout
            .get_hole(hole_start)
            .is_some_and(|gap| gap >= added_reserve)
        {
            // info!("Expand {region_index} to hole");

            layout.remove_or_compress_hole(hole_start, added_reserve)?;
            let mut meta = self.meta_mut();
            meta.set_reserved(new_reserved);
            drop(meta);
            drop(layout);

            db.write(write_start, data);

            self.extend_dirty_abs(start, write_start, data_len);
            let mut meta = self.meta_mut();
            meta.set_len(new_len);
            meta.write(index, &db.regions());

            return Ok(());
        }

        // Find hole big enough to move the region
        if let Some(hole_start) = layout.find_smallest_adequate_hole(new_reserved) {
            // info!("Move {region_index} to hole at {hole_start}");

            layout.remove_or_compress_hole(hole_start, new_reserved)?;
            drop(layout);

            db.copy(start, hole_start, write_start - start);
            db.write(hole_start + at.unwrap_or(len), data);

            let mut layout = db.layout_mut();
            layout.move_region(hole_start, self)?;

            // Region moved, mark all data as dirty (relative to new start)
            self.extend_dirty(0, new_len);
            let mut meta = self.meta_mut();
            meta.set_start(hole_start);
            meta.set_reserved(new_reserved);
            meta.set_len(new_len);
            meta.write(index, &db.regions());

            return Ok(());
        }

        let new_start = layout.len();
        // Write at the end
        // info!(
        //     "Move {region_index} to the end, from {start}..{} to {new_start}..{}",
        //     start + reserved,
        //     new_start + new_reserved
        // );
        db.set_min_len(new_start + new_reserved)?;
        layout.reserve(new_start, new_reserved);
        drop(layout);

        db.copy(start, new_start, write_start - start);
        db.write(new_start + at.unwrap_or(len), data);

        let mut layout = db.layout_mut();
        layout.move_region(new_start, self)?;
        assert!(layout.take_reserved(new_start) == Some(new_reserved));

        // Region moved, mark all data as dirty (relative to new start)
        self.extend_dirty(0, new_len);
        let mut meta = self.meta_mut();
        meta.set_start(new_start);
        meta.set_reserved(new_reserved);
        meta.set_len(new_len);
        meta.write(index, &db.regions());

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
        meta.write(index, &regions);
        Ok(())
    }

    /// Removes the region from the database.
    ///
    /// The space is marked as a pending hole that will become reusable after
    /// the next `flush()`. This consumes the region to prevent use-after-free.
    pub fn remove(self) -> Result<()> {
        let db = self.db();
        let mut regions = db.regions_mut();
        let mut layout = db.layout_mut();
        layout.remove_region(&self)?;
        regions.remove(&self)?;
        Ok(())
    }

    /// Flushes this region's dirty data and metadata to disk.
    ///
    /// Only flushes the dirty range if any writes have been made since the last flush.
    /// Returns `Ok(true)` if data was flushed, `Ok(false)` if nothing was dirty.
    pub fn flush(&self) -> Result<bool> {
        let Some((dirty_start, dirty_end)) = self.take_dirty_range() else {
            return Ok(false);
        };

        let db = self.db();
        let meta = self.meta();
        let start = meta.start();
        let abs_start = start + dirty_start;
        let dirty_len = dirty_end - dirty_start;

        db.mmap().flush_range(abs_start, dirty_len)?;
        meta.flush(self.index(), &db.regions())?;
        Ok(true)
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

    /// Extends the dirty range to include the given write.
    /// `offset` is relative to region start.
    #[inline]
    fn extend_dirty(&self, offset: usize, len: usize) {
        let end = offset + len;
        let mut dirty = self.0.dirty_range.lock();
        *dirty = Some(match *dirty {
            None => (offset, end),
            Some((s, e)) => (s.min(offset), e.max(end)),
        });
    }

    /// Extends the dirty range using absolute file positions.
    /// Converts to relative offsets internally.
    #[inline]
    fn extend_dirty_abs(&self, region_start: usize, abs_start: usize, len: usize) {
        let offset = abs_start - region_start;
        self.extend_dirty(offset, len);
    }

    /// Takes and clears the dirty range, returning it if any.
    #[inline]
    fn take_dirty_range(&self) -> Option<(usize, usize)> {
        self.0.dirty_range.lock().take()
    }
}
