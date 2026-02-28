#![doc = include_str!("../README.md")]

use std::{
    collections::HashSet,
    fmt,
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    sync::{Arc, Weak},
};

use log::{debug, trace};
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
    name: String,
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
        let name = path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
            .to_string();

        fs::create_dir_all(path)?;

        let file = OpenOptions::new()
            .read(true)
            .create(true)
            .write(true)
            .truncate(false)
            .open(Self::data_path_from(path))?;

        file.try_lock()?;

        let file_len = file.metadata()?.len() as usize;
        if file_len < min_len {
            file.set_len(min_len as u64)?;
            file.sync_all()?;
        }

        let regions = Regions::open(path)?;
        let mmap = create_mmap(&file)?;

        let db = Self(Arc::new(DatabaseInner {
            path: path.to_owned(),
            name,
            layout: RwLock::new(Layout::default()),
            regions: RwLock::new(regions),
            mmap: RwLock::new(mmap),
            file: RwLock::new(file),
        }));

        db.regions_mut().fill(&db)?;
        *db.layout_mut() = Layout::from(&*db.regions());

        debug!("{}: opened with {} regions", db, db.regions().len());

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

        // Quick check without lock
        let file_len = self.file_len()?;
        if file_len >= len {
            return Ok(());
        }

        debug!("{}: set_min_len({})", self, len);
        trace!("{}: set_min_len acquiring mmap_mut", self);
        let mut mmap = self.mmap_mut();
        trace!("{}: set_min_len acquiring file_mut", self);
        let file = self.file_mut();

        // Re-check after acquiring lock - another thread may have extended the file
        // while we were waiting. Without this check, we could TRUNCATE the file.
        let current_len = file.metadata()?.len() as usize;
        if current_len >= len {
            return Ok(());
        }

        trace!("{}: set_min_len extending file", self);
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
        debug!("{}: create_region_if_needed '{}'", self, id);
        trace!("{}: create_region_if_needed '{}' acquiring layout_mut", self, id);
        let mut layout = self.layout_mut();
        trace!("{}: create_region_if_needed '{}' acquiring regions_mut", self, id);
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

    /// Copy data within the mmap, chunked to avoid excessive memory pressure.
    ///
    /// Returns an error if source and destination ranges overlap.
    pub(crate) fn copy(&self, src: usize, dst: usize, len: usize) -> Result<()> {
        if len == 0 {
            return Ok(());
        }

        // Check for overlapping ranges
        let src_end = src + len;
        let dst_end = dst + len;
        if !(src_end <= dst || dst_end <= src) {
            return Err(Error::OverlappingCopyRanges {
                src,
                src_end,
                dst,
                dst_end,
            });
        }

        const CHUNK_SIZE: usize = GiB; // 1GB chunks
        let mmap = self.mmap();
        for offset in (0..len).step_by(CHUNK_SIZE) {
            let chunk_len = (len - offset).min(CHUNK_SIZE);
            let src_start = src + offset;
            let dst_start = dst + offset;
            write_to_mmap(&mmap, dst_start, &mmap[src_start..src_start + chunk_len]);
        }
        Ok(())
    }

    /// Flushes dirty ranges to disk.
    pub(crate) fn flush_ranges(&self, ranges: Vec<(usize, usize)>) -> Result<()> {
        let mmap = self.mmap();
        for (start, end) in ranges {
            mmap.flush_range(start, end - start)?;
        }
        Ok(())
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
        debug!(
            "{}: retain_regions called with {} ids to keep",
            self,
            ids.len()
        );

        // Collect regions to remove first to avoid deadlock
        // (holding read lock while calling region.remove() which needs write lock)
        let regions_to_remove: Vec<_> = self
            .regions()
            .id_to_index()
            .keys()
            .filter(|id| !ids.remove(&**id))
            .filter_map(|id| self.get_region(id))
            .collect();

        if !ids.is_empty() {
            debug!(
                "{}: retain_regions: {} ids in retain set not found in db: {:?}",
                self,
                ids.len(),
                ids
            );
        }

        if !regions_to_remove.is_empty() {
            debug!(
                "{}: retain_regions removing {} regions: {:?}",
                self,
                regions_to_remove.len(),
                regions_to_remove
                    .iter()
                    .map(|r| r.meta().id().to_string())
                    .collect::<Vec<_>>()
            );
        }

        // Now remove them (read lock is released)
        for region in regions_to_remove {
            let ref_count = std::sync::Arc::strong_count(region.arc());
            debug!(
                "{}: removing '{}' (arc count: {})",
                self,
                region.meta().id(),
                ref_count
            );
            region.remove()?;
        }
        Ok(())
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
            debug!("{}: flush (no dirty)", self);
            self.layout_mut().promote_pending_holes(self.name());
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

            if let Err(e) = self.flush_ranges(merged) {
                // Restore all original ranges to their regions
                for (region, ranges) in dirty_regions {
                    region.restore_dirty_ranges(ranges);
                }
                return Err(e);
            }
        }

        // Sync metadata, then mark clean
        self.regions().flush()?;
        for (region, _) in &dirty_regions {
            region.meta().mark_clean();
        }

        debug!("{}: flushed {} regions", self, dirty_regions.len());
        self.layout_mut().promote_pending_holes(self.name());
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
        let flush_time = i.elapsed();
        let i = Instant::now();
        let r = self.punch_holes();
        let punch_time = i.elapsed();
        debug!(
            "{}: compact in {:?} (flush: {:?}, punch_holes: {:?})",
            self,
            flush_time + punch_time,
            flush_time,
            punch_time
        );
        r
    }

    fn punch_holes(&self) -> Result<()> {
        // Hold layout READ throughout to prevent write_with from allocating in holes.
        trace!("{}: punch_holes acquiring layout", self);
        let layout = self.layout();

        // Collect regions that may have punchable reserved space
        let regions_to_check: Vec<Region> = {
            let regions = self.regions();
            regions
                .index_to_region()
                .iter()
                .flatten()
                .cloned()
                .collect()
        };

        // Collect layout holes (protected by layout READ - can punch in parallel)
        let layout_holes: Vec<(usize, usize)> = layout
            .start_to_hole()
            .iter()
            .map(|(&start, &hole)| (start, hole))
            .collect();

        let file = self.file();
        let mut punched = 0usize;

        // Punch region reserved space. We MUST hold meta WRITE before checking,
        // because write_with does db.write() BEFORE updating meta. If we only
        // acquire WRITE after checking, a concurrent write could be in progress.
        for region in &regions_to_check {
            let meta = region.meta_mut();
            let rstart = meta.start();
            let len = meta.len();
            let reserved = meta.reserved();
            let ceil_len = Self::ceil_number_to_page_size_multiple(len);

            if ceil_len < reserved {
                let start = rstart + ceil_len;
                let hole = reserved - ceil_len;
                if Self::approx_has_punchable_data(&file, start, hole) {
                    HolePunch::punch(&file, start, hole)?;
                    punched += 1;
                }
            }
        }

        // Punch layout holes in parallel (safe - layout READ prevents allocation)
        // No per-region lock needed since these are unallocated holes.
        let layout_punched: usize = layout_holes
            .par_iter()
            .filter_map(|&(start, hole)| {
                if Self::approx_has_punchable_data(&file, start, hole) {
                    HolePunch::punch(&file, start, hole).ok()?;
                    Some(1)
                } else {
                    None
                }
            })
            .sum();
        punched += layout_punched;

        drop(file);
        drop(layout);

        // Sync and recreate mmap if we punched anything
        if punched > 0 {
            debug!("{}: punch_holes syncing after {} punches", self, punched);
            let mut mmap = self.mmap_mut();
            let file = self.file_mut();
            // sync_all syncs both data and metadata (block allocation), which is
            // necessary for hole punching since it modifies sparse file metadata
            file.sync_all()?;
            *mmap = create_mmap(&file)?;
        }

        Ok(())
    }

    /// Check if a hole region likely contains non-zero data worth punching.
    /// Uses pread to avoid holding mmap lock.
    fn approx_has_punchable_data(file: &File, start: usize, len: usize) -> bool {
        use std::os::unix::io::AsRawFd;

        assert!(start.is_multiple_of(PAGE_SIZE));
        assert!(len.is_multiple_of(PAGE_SIZE));

        let fd = file.as_raw_fd();
        let mut buf = [0u8; 1];

        let mut check_page = |page_start: usize| -> bool {
            // Check first byte
            let n = unsafe {
                libc::pread(
                    fd,
                    buf.as_mut_ptr() as *mut libc::c_void,
                    1,
                    page_start as libc::off_t,
                )
            };
            if n == 1 && buf[0] != 0 {
                return true;
            }

            // Check last byte
            let page_end = page_start + PAGE_SIZE - 1;
            let n = unsafe {
                libc::pread(
                    fd,
                    buf.as_mut_ptr() as *mut libc::c_void,
                    1,
                    page_end as libc::off_t,
                )
            };
            n == 1 && buf[0] != 0
        };

        // Check first page
        if check_page(start) {
            return true;
        }

        // Check last page
        let last_page_start = start + len - PAGE_SIZE;
        if last_page_start != start && check_page(last_page_start) {
            return true;
        }

        // For very large holes, also check at GB boundaries
        if len > GiB {
            let num_gb_checks = len / GiB;
            for i in 1..num_gb_checks {
                let gb_boundary = start + i * GiB;
                if check_page(gb_boundary) {
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

    /// Returns the database name (last path component) for logging.
    #[inline]
    pub fn name(&self) -> &str {
        &self.0.name
    }
}

impl fmt::Display for Database {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
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
