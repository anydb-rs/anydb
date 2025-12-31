#![doc = include_str!("../README.md")]

use std::{
    collections::HashSet,
    fs::{self, File, OpenOptions},
    os::unix::io::AsRawFd,
    path::{Path, PathBuf},
    sync::{Arc, Weak},
};

use log::debug;
use memmap2::MmapMut;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

mod disk_usage;
pub mod error;
mod hints;
mod hole_punch;
mod layout;
mod mmap;
mod reader;
mod region;
mod region_metadata;
mod region_state;
mod regions;

pub use disk_usage::*;
pub use error::*;
pub use hints::*;
use hole_punch::*;
use layout::*;
use mmap::*;
use rayon::prelude::*;
pub use reader::*;
pub use region::*;
pub use region_metadata::*;
use region_state::*;
use regions::*;

pub const PAGE_SIZE: usize = 4096;
pub const PAGE_SIZE_MINUS_1: usize = PAGE_SIZE - 1;
/// One gibibyte (1024^3 bytes).
#[allow(non_upper_case_globals)]
pub const GiB: usize = 1024 * 1024 * 1024;

/// Memory-mapped database with dynamic space allocation and hole punching.
///
/// Provides efficient storage through memory mapping with automatic region management,
/// space reclamation via hole punching, and dynamic file growth as needed.
#[derive(Debug, Clone)]
#[must_use = "Database should be stored to keep the database open"]
pub struct Database(Arc<DatabaseInner>);

/// # Lock Ordering
///
/// To prevent deadlocks, locks must always be acquired in this order:
///
/// ```text
/// 1. layout     (Database-level: allocation and hole tracking)
/// 2. regions    (Database-level: region registry)
/// 3. mmap       (Database-level: memory-mapped file)
/// 4. file       (Database-level: file handle)
/// 5. meta       (Region-level: per-region metadata)
/// 6. dirty_ranges (Region-level: per-region dirty tracking)
/// ```
///
/// If you need multiple locks, acquire them top-to-bottom. Never hold a
/// lower lock while acquiring a higher one.
#[derive(Debug)]
struct DatabaseInner {
    path: PathBuf,
    // Lock order: layout → regions → mmap → file
    layout: RwLock<Layout>,
    regions: RwLock<Regions>,
    mmap: RwLock<MmapMut>,
    file: RwLock<File>,
}

impl Database {
    /// Opens or creates a database at the specified path.
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_with_min_len(path, 0)
    }

    /// Opens or creates a database with a minimum initial file size.
    pub fn open_with_min_len(path: &Path, min_len: usize) -> Result<Self> {
        fs::create_dir_all(path)?;

        let file = OpenOptions::new()
            .read(true)
            .create(true)
            .write(true)
            .truncate(false)
            .open(Self::data_path_from(path))?;
        debug!("File opened.");

        file.try_lock()?;
        debug!("File locked.");

        let file_len = file.metadata()?.len() as usize;
        if file_len < min_len {
            file.set_len(min_len as u64)?;
            debug!("File extended.");
            file.sync_all()?;
        }

        let regions = Regions::open(path)?;
        let mmap = create_mmap(&file)?;
        debug!("Mmap created.");

        let db = Self(Arc::new(DatabaseInner {
            path: path.to_owned(),
            layout: RwLock::new(Layout::default()),
            regions: RwLock::new(regions),
            mmap: RwLock::new(mmap),
            file: RwLock::new(file),
        }));

        db.regions_mut().fill(&db)?;
        debug!("Filled regions.");
        *db.layout_mut() = Layout::from(&*db.regions());
        debug!("Layout created.");

        Ok(db)
    }

    /// Returns the current length of the database file in bytes.
    pub fn file_len(&self) -> Result<usize> {
        Ok(self.file().metadata()?.len() as usize)
    }

    /// Ensures the database file is at least the specified length.
    ///
    /// This pre-allocates file space to avoid expensive growth during writes.
    /// The length is rounded up to the next page size multiple (4096 bytes).
    pub fn set_min_len(&self, len: usize) -> Result<()> {
        let len = Self::ceil_number_to_page_size_multiple(len);

        let file_len = self.file_len()?;
        if file_len >= len {
            return Ok(());
        }

        let mut mmap = self.mmap_mut();
        let file = self.file_mut();
        file.set_len(len as u64)?;
        *mmap = create_mmap(&file)?;
        Ok(())
    }

    /// Pre-allocates space for at least the specified number of regions.
    ///
    /// This avoids expensive reallocations when creating many regions.
    pub fn set_min_regions(&self, regions: usize) -> Result<()> {
        self.regions_mut()
            .set_min_len(regions * SIZE_OF_REGION_METADATA)?;
        self.set_min_len(regions * PAGE_SIZE)
    }

    /// Gets an existing region by ID.
    pub fn get_region(&self, id: &str) -> Option<Region> {
        self.regions().get_from_id(id).cloned()
    }

    /// Creates a region with the given ID, or returns it if it already exists.
    pub fn create_region_if_needed(&self, id: &str) -> Result<Region> {
        if let Some(region) = self.get_region(id) {
            return Ok(region);
        }

        // Pre-extend outside lock if needed (file I/O shouldn't block other threads)
        let layout = self.layout();
        if layout.find_smallest_adequate_hole(PAGE_SIZE).is_none() {
            let end = layout.len();
            drop(layout);
            self.set_min_len(end + PAGE_SIZE * 16)?;
        } else {
            drop(layout);
        }

        // Lock order: layout → regions
        let mut layout = self.layout_mut();
        let mut regions = self.regions_mut();

        // Double-check after lock (another thread may have created it)
        if let Some(region) = regions.get_from_id(id).cloned() {
            return Ok(region);
        }

        let start = if let Some(start) = layout.find_smallest_adequate_hole(PAGE_SIZE) {
            layout.remove_or_compress_hole(start, PAGE_SIZE)?;
            start
        } else {
            layout.len()
        };

        let region = regions.create(self, id.to_owned(), start)?;
        layout.insert_region(start, &region);
        Ok(region)
    }

    #[inline]
    pub(crate) fn write(&self, start: usize, data: &[u8]) {
        write_to_mmap(&self.mmap(), start, data);
    }

    /// Copy data within the mmap, chunked to avoid excessive memory pressure
    pub(crate) fn copy(&self, src: usize, dst: usize, len: usize) {
        const CHUNK_SIZE: usize = GiB; // 1GB chunks
        let mmap = self.mmap();
        for offset in (0..len).step_by(CHUNK_SIZE) {
            let chunk_len = (len - offset).min(CHUNK_SIZE);
            let src_start = src + offset;
            let dst_start = dst + offset;
            write_to_mmap(&mmap, dst_start, &mmap[src_start..src_start + chunk_len]);
        }
    }

    /// Removes a region by ID if it exists, otherwise does nothing.
    ///
    /// Returns `Ok(())` whether the region existed or not.
    pub fn remove_region_if_exists(&self, id: &str) -> Result<()> {
        match self.remove_region(id) {
            Ok(()) | Err(Error::RegionNotFound) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Removes a region by ID.
    ///
    /// Returns `Error::RegionNotFound` if the region doesn't exist.
    pub fn remove_region(&self, id: &str) -> Result<()> {
        let Some(region) = self.get_region(id) else {
            return Err(Error::RegionNotFound);
        };
        region.remove()
    }

    /// Removes all regions except those in the provided set.
    ///
    /// This is useful for garbage collection - keeping only regions that are
    /// still in use and removing all others.
    pub fn retain_regions(&self, mut ids: HashSet<String>) -> Result<()> {
        // Collect regions to remove first to avoid deadlock
        // (holding read lock while calling region.remove() which needs write lock)
        let regions_to_remove: Vec<_> = self
            .regions()
            .id_to_index()
            .keys()
            .filter(|id| !ids.remove(&**id))
            .filter_map(|id| self.get_region(id))
            .collect();

        // Now remove them (read lock is released)
        regions_to_remove
            .into_iter()
            .try_for_each(|region| region.remove())
    }

    /// Open a dedicated file handle for sequential reading
    /// This enables optimal kernel readahead for iteration
    #[inline]
    pub fn open_read_only_file(&self) -> Result<File> {
        File::open(self.data_path()).map_err(Error::from)
    }

    /// Returns the actual disk usage (accounting for sparse files and hole punching).
    ///
    /// On Unix systems, this uses `fstat` to get the number of blocks actually allocated.
    /// On Windows, this falls back to the logical file size (less accurate for sparse files).
    pub fn disk_usage(&self) -> Result<DiskUsage> {
        DiskUsage::from_file(&self.file())
    }

    /// Flushes all dirty data and metadata to disk.
    ///
    /// This ensures durability - all writes are persisted and will survive a crash.
    /// Also promotes pending holes so they can be reused by future allocations.
    /// Returns the number of regions that had dirty data or metadata.
    pub fn flush(&self) -> Result<usize> {
        // Collect dirty regions (take ranges, clearing them atomically)
        let dirty_regions: Vec<(Region, Vec<(usize, usize)>)> = self
            .regions()
            .index_to_region()
            .iter()
            .flatten()
            .filter_map(|r| {
                let ranges = r.take_dirty_ranges();
                if !ranges.is_empty() || r.meta().needs_flush() {
                    Some((r.clone(), ranges))
                } else {
                    None
                }
            })
            .collect();

        if dirty_regions.is_empty() {
            self.layout_mut().promote_pending_holes();
            return Ok(0);
        }

        // Collect all dirty ranges with their region info
        let data_regions: Vec<_> = dirty_regions
            .iter()
            .filter(|(_, ranges)| !ranges.is_empty())
            .collect();

        if !data_regions.is_empty() {
            // Flatten all ranges into absolute positions
            let mut all_ranges: Vec<_> = data_regions
                .iter()
                .flat_map(|(r, ranges)| {
                    let region_start = r.meta().start();
                    ranges
                        .iter()
                        .map(move |(s, e)| (region_start + s, region_start + e))
                })
                .collect();
            all_ranges.sort_unstable_by_key(|(s, _)| *s);

            // Merge adjacent ranges (gap < 64KB)
            const MERGE_GAP: usize = 64 * 1024;
            let merged: Vec<(usize, usize)> =
                all_ranges
                    .into_iter()
                    .fold(Vec::new(), |mut acc, (s, e)| {
                        if let Some((_, last_end)) = acc.last_mut()
                            && s <= *last_end + MERGE_GAP
                        {
                            *last_end = (*last_end).max(e);
                        } else {
                            acc.push((s, e));
                        }
                        acc
                    });

            let mmap = self.mmap();
            for (start, end) in merged {
                mmap.flush_range(start, end - start)?;
            }
        }

        // Sync metadata, then mark clean
        self.regions().flush()?;
        for (region, _) in &dirty_regions {
            region.meta().mark_clean();
        }

        self.layout_mut().promote_pending_holes();
        Ok(dirty_regions.len())
    }

    /// Compact the database by promoting pending holes and punching holes in the file.
    ///
    /// This flushes all dirty data first to ensure consistency.
    #[inline]
    pub fn compact(&self) -> Result<()> {
        use std::time::Instant;
        let i = Instant::now();
        self.flush()?;
        debug!("compact flush: {:?}", i.elapsed());
        let i = Instant::now();
        let r = self.punch_holes();
        debug!("compact punch_holes: {:?}", i.elapsed());
        r
    }

    fn punch_holes(&self) -> Result<()> {
        // Lock order: layout → regions → mmap → file
        let layout = self.layout();
        let regions = self.regions();
        let mut mmap = self.mmap_mut();
        let file = self.file_mut();

        let mut punched = regions
            .index_to_region()
            .par_iter()
            .flatten()
            .map(|region| -> Result<usize> {
                let region_meta = region.meta();
                let rstart = region_meta.start();
                let len = region_meta.len();
                let reserved = region_meta.reserved();
                let ceil_len = Self::ceil_number_to_page_size_multiple(len);
                if unlikely(ceil_len > reserved) {
                    return Err(Error::InvariantViolation(format!(
                        "ceil_len ({}) > reserved ({})",
                        ceil_len, reserved
                    )));
                } else if ceil_len < reserved {
                    let start = rstart + ceil_len;
                    let hole = reserved - ceil_len;
                    if Self::approx_has_punchable_data(&mmap, start, hole) {
                        HolePunch::punch(&file, start, hole)?;
                        return Ok(1);
                    }
                }
                Ok(0)
            })
            .sum::<Result<usize>>()?;

        punched += layout
            .start_to_hole()
            .par_iter()
            .map(|(&start, &hole)| -> Result<usize> {
                if Self::approx_has_punchable_data(&mmap, start, hole) {
                    HolePunch::punch(&file, start, hole)?;
                    return Ok(1);
                }
                Ok(0)
            })
            .sum::<Result<usize>>()?;

        if punched > 0 {
            unsafe {
                libc::fsync(file.as_raw_fd());
            }
            *mmap = create_mmap(&file)?;
        }

        Ok(())
    }

    fn approx_has_punchable_data(mmap: &MmapMut, start: usize, len: usize) -> bool {
        assert!(start.is_multiple_of(PAGE_SIZE));
        assert!(len.is_multiple_of(PAGE_SIZE));

        // Validate bounds before accessing mmap to prevent hangs from corrupted metadata
        if start + len > mmap.len() {
            debug!(
                "approx_has_punchable_data: out of bounds - start={}, len={}, mmap_len={}",
                start,
                len,
                mmap.len()
            );
            return false;
        }

        let min = start;
        let max = start + len;
        let check = |start, end| {
            assert!(start >= min);
            assert!(end < max);
            let start_is_some = mmap[start] != 0;
            let end_is_some = mmap[end] != 0;
            start_is_some || end_is_some
        };

        let first_page_start = start;
        let first_page_end = start + PAGE_SIZE - 1;
        if check(first_page_start, first_page_end) {
            return true;
        }

        let last_page_start = start + len - PAGE_SIZE;
        let last_page_end = start + len - 1;
        if check(last_page_start, last_page_end) {
            return true;
        }

        if len > GiB {
            let num_gb_checks = len / GiB;
            for i in 1..num_gb_checks {
                let gb_boundary = start + i * GiB;
                let page_start = gb_boundary;
                let page_end = gb_boundary + PAGE_SIZE - 1;

                if check(page_start, page_end) {
                    return true;
                }
            }
        }

        false
    }

    #[inline(always)]
    pub fn file(&self) -> RwLockReadGuard<'_, File> {
        self.0.file.read()
    }

    #[inline(always)]
    pub fn file_mut(&self) -> RwLockWriteGuard<'_, File> {
        self.0.file.write()
    }

    #[inline(always)]
    pub fn mmap(&self) -> RwLockReadGuard<'_, MmapMut> {
        self.0.mmap.read()
    }

    #[inline(always)]
    pub fn mmap_mut(&self) -> RwLockWriteGuard<'_, MmapMut> {
        self.0.mmap.write()
    }

    #[inline(always)]
    pub fn regions(&self) -> RwLockReadGuard<'_, Regions> {
        self.0.regions.read()
    }

    #[inline(always)]
    pub(crate) fn regions_mut(&self) -> RwLockWriteGuard<'_, Regions> {
        self.0.regions.write()
    }

    #[inline(always)]
    pub fn layout(&self) -> RwLockReadGuard<'_, Layout> {
        self.0.layout.read()
    }

    #[inline(always)]
    pub(crate) fn layout_mut(&self) -> RwLockWriteGuard<'_, Layout> {
        self.0.layout.write()
    }

    #[inline]
    fn ceil_number_to_page_size_multiple(num: usize) -> usize {
        (num + PAGE_SIZE_MINUS_1) & !PAGE_SIZE_MINUS_1
    }

    #[inline(always)]
    fn data_path(&self) -> PathBuf {
        Self::data_path_from(self.path())
    }
    #[inline(always)]
    fn data_path_from(path: &Path) -> PathBuf {
        path.join("data")
    }

    #[inline(always)]
    pub fn path(&self) -> &Path {
        &self.0.path
    }

    #[inline]
    pub fn weak_clone(&self) -> WeakDatabase {
        WeakDatabase(Arc::downgrade(&self.0))
    }
}

/// Weak reference to a Database that doesn't prevent it from being dropped.
///
/// Used internally by regions to avoid circular references while maintaining
/// access to the parent database.
#[derive(Debug, Clone)]
pub struct WeakDatabase(Weak<DatabaseInner>);

impl WeakDatabase {
    pub fn upgrade(&self) -> Database {
        Database(
            self.0
                .upgrade()
                .expect("Database was dropped while Region still exists"),
        )
    }
}
