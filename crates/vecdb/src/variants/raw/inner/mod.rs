use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    marker::PhantomData,
    mem,
    path::PathBuf,
};

use log::{debug, info};
use rawdb::{Database, Reader, Region, likely, unlikely};

use crate::{
    AnyStoredVec, AnyVec, BUFFER_SIZE, BaseVec, Bytes, BytesExt, Error, Format, GenericStoredVec,
    HEADER_OFFSET, Header, ImportOptions, RawIoSource, RawMmapSource, Result, SIZE_OF_U64,
    ScannableVec, Stamp, TypedVec, VecIndex, VecReader, VecValue, Version, WithPrev,
    short_type_name, vec_region_name_with,
};

mod strategy;

pub use strategy::*;

const VERSION: Version = Version::ONE;

/// Core implementation for raw storage vectors shared by BytesVec and ZeroCopyVec.
///
/// Parameterized by serialization strategy `S` to support different serialization approaches:
/// - `BytesStrategy`: Explicit little-endian serialization (portable)
/// - `ZeroCopyStrategy`: Native byte order via zerocopy (fast but not portable)
///
/// Provides holes (deleted indices) and updated values tracking for both vec types.
#[derive(Debug, Clone)]
#[must_use = "Vector should be stored to keep data accessible"]
pub struct RawVecInner<I, T, S> {
    pub(crate) base: BaseVec<I, T>,
    has_stored_holes: bool,
    holes: WithPrev<BTreeSet<usize>>,
    updated: WithPrev<BTreeMap<usize, T>>,
    _strategy: PhantomData<S>,
}

impl<I, T, S> RawVecInner<I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: RawStrategy<T>,
{
    /// The size of T in bytes as determined by the strategy.
    pub const SIZE_OF_T: usize = size_of::<T>();

    /// Imports the vector, automatically resetting it if format/version mismatches occur.
    ///
    /// # Warning
    ///
    /// This will DELETE all existing data on format/version errors. Use with caution.
    pub fn forced_import_with(mut options: ImportOptions, format: Format) -> Result<Self> {
        options.version = options.version + VERSION;
        let res = Self::import_with(options, format);
        match res {
            Err(Error::WrongEndian)
            | Err(Error::WrongLength { .. })
            | Err(Error::DifferentFormat { .. })
            | Err(Error::DifferentVersion { .. }) => {
                info!("Resetting {}...", options.name);
                options
                    .db
                    .remove_region_if_exists(&vec_region_name_with::<I>(options.name))?;
                Self::import_with(options, format)
            }
            _ => res,
        }
    }

    pub fn import_with(mut options: ImportOptions, format: Format) -> Result<Self> {
        options.version = options.version + VERSION;

        let db = options.db;
        let name = options.name;

        let base = BaseVec::import(options, format)?;

        // Raw format requires data to be aligned to SIZE_OF_T
        let region_len = base.region().meta().len();
        if region_len > HEADER_OFFSET
            && !(region_len - HEADER_OFFSET).is_multiple_of(Self::SIZE_OF_T)
        {
            return Err(Error::CorruptedRegion { region_len });
        }

        let holes = if let Some(region) = db.get_region(&Self::holes_region_name_with(name)) {
            Some(
                region
                    .create_reader()
                    .read_all()
                    .chunks(size_of::<usize>())
                    .map(usize::from_bytes)
                    .collect::<Result<BTreeSet<usize>>>()?,
            )
        } else {
            None
        };

        let mut this = Self {
            base,
            has_stored_holes: holes.is_some(),
            holes: WithPrev::new(holes.unwrap_or_default()),
            updated: WithPrev::default(),
            _strategy: PhantomData,
        };

        let len = this.real_stored_len();
        *this.base.mut_prev_stored_len() = len;
        this.base.update_stored_len(len);

        Ok(this)
    }

    fn write_header_if_needed(&mut self) -> Result<()> {
        if self.header().modified() {
            let r = self.region().clone();
            self.mut_header().write(&r)?;
        }
        Ok(())
    }

    /// Returns optimal buffer size for I/O operations, aligned to SIZE_OF_T boundary.
    #[inline]
    pub(crate) const fn aligned_buffer_size() -> usize {
        (BUFFER_SIZE / Self::SIZE_OF_T) * Self::SIZE_OF_T
    }

    /// Removes this vector and all associated regions (main region and holes) from the database.
    pub fn remove(self) -> Result<()> {
        let db = self.base.db();
        let holes_region_name = self.holes_region_name();
        let has_stored_holes = self.has_stored_holes;

        // Remove main region
        self.base.remove()?;

        // Remove holes region if it exists
        if has_stored_holes {
            db.remove_region(&holes_region_name)?;
        }

        Ok(())
    }

    fn holes_region_name(&self) -> String {
        Self::holes_region_name_with(self.name())
    }
    fn holes_region_name_with(name: &str) -> String {
        format!("{}_holes", vec_region_name_with::<I>(name))
    }

    #[inline(always)]
    pub fn holes(&self) -> &BTreeSet<usize> {
        self.holes.current()
    }

    #[inline(always)]
    pub fn prev_holes(&self) -> &BTreeSet<usize> {
        self.holes.previous()
    }

    #[inline(always)]
    pub fn mut_holes(&mut self) -> &mut BTreeSet<usize> {
        self.holes.current_mut()
    }

    #[inline(always)]
    pub fn updated(&self) -> &BTreeMap<usize, T> {
        self.updated.current()
    }

    #[inline(always)]
    pub fn mut_updated(&mut self) -> &mut BTreeMap<usize, T> {
        self.updated.current_mut()
    }

    #[inline(always)]
    pub fn prev_updated(&self) -> &BTreeMap<usize, T> {
        self.updated.previous()
    }

    // ── Point reads (all layers: holes, updated, pushed, stored) ────

    /// Gets a value checking all dirty-state layers.
    ///
    /// Checks in order: holes → pushed → updated → stored.
    /// Returns `None` if the index is a hole or beyond length.
    /// This is the most complete single-value read for raw vecs.
    #[inline]
    pub fn get_any_or_read(&self, index: I, reader: &Reader) -> Result<Option<T>> {
        self.get_any_or_read_at(index.to_usize(), reader)
    }

    /// Gets a value checking all dirty-state layers at a usize index.
    ///
    /// Checks: holes → pushed → updated → stored.
    /// Returns `None` if the index is a hole or beyond length.
    #[inline]
    pub fn get_any_or_read_at(&self, index: usize, reader: &Reader) -> Result<Option<T>> {
        // Check holes first
        if !self.holes().is_empty() && self.holes().contains(&index) {
            return Ok(None);
        }

        let stored_len = self.stored_len();

        // Check pushed (beyond stored length)
        if index >= stored_len {
            return Ok(self.pushed().get(index - stored_len).cloned());
        }

        // Check updated layer
        if !self.updated().is_empty()
            && let Some(updated_value) = self.updated().get(&index)
        {
            return Ok(Some(updated_value.clone()));
        }

        // Fall back to reading from storage
        Ok(Some(self.unchecked_read_at(index, reader)?))
    }

    /// Updates the value at the given index.
    #[inline]
    pub fn update(&mut self, index: I, value: T) -> Result<()> {
        self.update_at(index.to_usize(), value)
    }

    /// Updates the value at the given usize index.
    #[inline]
    pub fn update_at(&mut self, index: usize, value: T) -> Result<()> {
        let stored_len = self.stored_len();

        if index >= stored_len {
            if let Some(prev) = self.mut_pushed().get_mut(index - stored_len) {
                *prev = value;
                return Ok(());
            } else {
                return Err(Error::IndexTooHigh {
                    index,
                    len: stored_len,
                    name: self.name().to_string(),
                });
            }
        }

        if !self.holes().is_empty() {
            self.mut_holes().remove(&index);
        }

        self.mut_updated().insert(index, value);

        Ok(())
    }

    /// Returns the first empty index (either the first hole or the length).
    #[inline]
    pub fn get_first_empty_index(&self) -> I {
        self.holes()
            .first()
            .cloned()
            .unwrap_or_else(|| self.len_())
            .into()
    }

    /// Fills the first hole with the value, or pushes if there are no holes. Returns the index used.
    #[inline]
    pub fn fill_first_hole_or_push(&mut self, value: T) -> Result<I> {
        Ok(
            if let Some(hole) = self.mut_holes().pop_first().map(I::from) {
                self.update(hole, value)?;
                hole
            } else {
                self.push(value);
                I::from(self.len() - 1)
            },
        )
    }

    /// Takes (removes and returns) the value at the given index using provided reader.
    pub fn take(&mut self, index: I, reader: &Reader) -> Result<Option<T>> {
        self.take_at(index.to_usize(), reader)
    }

    /// Takes (removes and returns) the value at the given usize index using provided reader.
    pub fn take_at(&mut self, index: usize, reader: &Reader) -> Result<Option<T>> {
        let opt = self.get_any_or_read_at(index, reader)?;
        if opt.is_some() {
            self.unchecked_delete_at(index);
        }
        Ok(opt)
    }

    /// Deletes the value at the given index (marks it as a hole).
    #[inline]
    pub fn delete(&mut self, index: I) {
        self.delete_at(index.to_usize())
    }

    /// Deletes the value at the given usize index (marks it as a hole).
    #[inline]
    pub fn delete_at(&mut self, index: usize) {
        if index < self.len() {
            self.unchecked_delete_at(index);
        }
    }

    #[inline]
    #[doc(hidden)]
    pub fn unchecked_delete(&mut self, index: I) {
        self.unchecked_delete_at(index.to_usize())
    }

    #[inline]
    #[doc(hidden)]
    pub fn unchecked_delete_at(&mut self, index: usize) {
        if !self.updated().is_empty() {
            self.mut_updated().remove(&index);
        }
        self.mut_holes().insert(index);
    }

    /// Collects all values into a Vec, with None for holes.
    pub fn collect_holed(&self) -> Result<Vec<Option<T>>> {
        self.collect_holed_range(0, self.len())
    }

    /// Collects values in `[from, to)` into a Vec, with None for holes.
    pub fn collect_holed_range(&self, from: usize, to: usize) -> Result<Vec<Option<T>>> {
        let len = self.len();
        let from = from.min(len);
        let to = to.min(len);

        if from >= to {
            return Ok(vec![]);
        }

        let reader = self.create_reader();

        (from..to)
            .map(|i| self.get_any_or_read_at(i, &reader))
            .collect::<Result<Vec<_>>>()
    }

    #[inline]
    pub fn create_reader(&self) -> Reader {
        self.base.region().create_reader()
    }

    /// Reads a stored value at `index` without bounds checking.
    #[inline(always)]
    pub fn unchecked_read_at(&self, index: usize, reader: &Reader) -> Result<T> {
        let bytes = reader.prefixed((index * Self::SIZE_OF_T) + HEADER_OFFSET);
        S::read(&bytes[..Self::SIZE_OF_T])
    }

    /// Reads a stored value at `index` using the provided reader.
    #[inline(always)]
    pub fn read_at(&self, index: usize, reader: &Reader) -> Result<T> {
        let len = self.len_();
        if likely(index < len) {
            self.unchecked_read_at(index, reader)
        } else {
            Err(Error::IndexTooHigh {
                index,
                len,
                name: self.name().to_string(),
            })
        }
    }

    /// Reads a stored value at `index`, creating a temporary reader.
    #[inline]
    pub fn read_at_once(&self, index: usize) -> Result<T> {
        self.read_at(index, &self.create_reader())
    }

    /// Reads a stored value at the typed index, creating a temporary reader.
    #[inline]
    pub fn read_once(&self, index: I) -> Result<T> {
        self.read_at_once(index.to_usize())
    }

    #[inline]
    pub fn push(&mut self, value: T) {
        self.base.mut_pushed().push(value);
    }

    #[inline]
    pub fn reserve_pushed(&mut self, additional: usize) {
        self.base.reserve_pushed(additional);
    }

    #[inline]
    pub fn index_to_name(&self) -> String {
        vec_region_name_with::<I>(self.name())
    }

    /// Creates a `VecReader` for O(1) random access to stored values.
    #[inline]
    pub fn reader(&self) -> VecReader<I, T, S> {
        VecReader::new(self)
    }

    // ── Source helpers (internal, for ScannableVec) ──────────────────

    /// Fold over stored data using auto-selected source (mmap or IO).
    fn fold_source<B, F: FnMut(B, T) -> B>(&self, from: usize, to: usize, init: B, f: F) -> B {
        let range_bytes = to.saturating_sub(from) * size_of::<T>();
        if range_bytes > crate::MMAP_CROSSOVER_BYTES {
            RawIoSource::new(self, from, to).fold(init, f)
        } else {
            RawMmapSource::new(self, from, to).fold(init, f)
        }
    }

    /// Fallible fold over stored data using auto-selected source.
    fn try_fold_source<B, E, F: FnMut(B, T) -> std::result::Result<B, E>>(
        &self,
        from: usize,
        to: usize,
        init: B,
        f: F,
    ) -> std::result::Result<B, E> {
        let range_bytes = to.saturating_sub(from) * size_of::<T>();
        if range_bytes > crate::MMAP_CROSSOVER_BYTES {
            RawIoSource::new(self, from, to).try_fold(init, f)
        } else {
            RawMmapSource::new(self, from, to).try_fold(init, f)
        }
    }

    // ── Public source-specific folds (for benchmarking) ──────────────

    /// Fold over stored data using buffered file I/O.
    ///
    /// Only reads stored (persisted) values — ignores holes, updates, and pushed.
    /// Useful for benchmarking I/O strategy vs mmap.
    pub fn fold_stored_io<B, F: FnMut(B, T) -> B>(
        &self,
        from: usize,
        to: usize,
        init: B,
        f: F,
    ) -> B {
        let stored_len = self.stored_len();
        let from = from.min(stored_len);
        let to = to.min(stored_len);
        if from >= to {
            return init;
        }
        RawIoSource::new(self, from, to).fold(init, f)
    }

    /// Fold over stored data using memory-mapped access.
    ///
    /// Only reads stored (persisted) values — ignores holes, updates, and pushed.
    /// Useful for benchmarking mmap strategy vs I/O.
    pub fn fold_stored_mmap<B, F: FnMut(B, T) -> B>(
        &self,
        from: usize,
        to: usize,
        init: B,
        f: F,
    ) -> B {
        let stored_len = self.stored_len();
        let from = from.min(stored_len);
        let to = to.min(stored_len);
        if from >= to {
            return init;
        }
        RawMmapSource::new(self, from, to).fold(init, f)
    }

    /// Element-by-element fold handling dirty state (holes, updates, pushed).
    fn fold_dirty<B, F: FnMut(B, T) -> B>(&self, from: usize, to: usize, init: B, mut f: F) -> B {
        let stored_len = self.stored_len();
        let has_holes = !self.holes().is_empty();
        let has_updates = !self.updated().is_empty();
        let reader = self.create_reader();
        let mut acc = init;

        let stored_to = to.min(stored_len);
        for i in from..stored_to {
            if has_holes && self.holes().contains(&i) {
                continue;
            }
            let val = if has_updates {
                if let Some(v) = self.updated().get(&i) {
                    v.clone()
                } else {
                    self.unchecked_read_at(i, &reader).unwrap()
                }
            } else {
                self.unchecked_read_at(i, &reader).unwrap()
            };
            acc = f(acc, val);
        }

        let push_from = from.max(stored_len);
        for i in push_from..to {
            if has_holes && self.holes().contains(&i) {
                continue;
            }
            if let Some(v) = self.get_pushed_at(i, stored_len) {
                acc = f(acc, v.clone());
            }
        }

        acc
    }

    /// Element-by-element fallible fold handling dirty state.
    fn try_fold_dirty<B, E, F: FnMut(B, T) -> std::result::Result<B, E>>(
        &self,
        from: usize,
        to: usize,
        init: B,
        mut f: F,
    ) -> std::result::Result<B, E> {
        let stored_len = self.stored_len();
        let has_holes = !self.holes().is_empty();
        let has_updates = !self.updated().is_empty();
        let reader = self.create_reader();
        let mut acc = init;

        let stored_to = to.min(stored_len);
        for i in from..stored_to {
            if has_holes && self.holes().contains(&i) {
                continue;
            }
            let val = if has_updates {
                if let Some(v) = self.updated().get(&i) {
                    v.clone()
                } else {
                    self.unchecked_read_at(i, &reader).unwrap()
                }
            } else {
                self.unchecked_read_at(i, &reader).unwrap()
            };
            acc = f(acc, val)?;
        }

        let push_from = from.max(stored_len);
        for i in push_from..to {
            if has_holes && self.holes().contains(&i) {
                continue;
            }
            if let Some(v) = self.get_pushed_at(i, stored_len) {
                acc = f(acc, v.clone())?;
            }
        }

        Ok(acc)
    }
}

impl<I, T, S> AnyVec for RawVecInner<I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: RawStrategy<T>,
{
    #[inline]
    fn version(&self) -> Version {
        self.header().vec_version()
    }

    #[inline]
    fn name(&self) -> &str {
        self.base.name()
    }

    #[inline]
    fn len(&self) -> usize {
        self.len_()
    }

    #[inline]
    fn index_type_to_string(&self) -> &'static str {
        I::to_string()
    }

    #[inline]
    fn value_type_to_size_of(&self) -> usize {
        size_of::<T>()
    }

    #[inline]
    fn value_type_to_string(&self) -> &'static str {
        short_type_name::<T>()
    }

    #[inline]
    fn region_names(&self) -> Vec<String> {
        let mut names = vec![self.index_to_name()];
        if self.has_stored_holes {
            names.push(self.holes_region_name());
        }
        names
    }
}

impl<I, T, S> AnyStoredVec for RawVecInner<I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: RawStrategy<T>,
{
    #[inline]
    fn db_path(&self) -> PathBuf {
        self.base.db_path()
    }

    #[inline]
    fn header(&self) -> &Header {
        self.base.header()
    }

    #[inline]
    fn mut_header(&mut self) -> &mut Header {
        self.base.mut_header()
    }

    #[inline]
    fn saved_stamped_changes(&self) -> u16 {
        self.base.saved_stamped_changes()
    }

    fn db(&self) -> Database {
        self.region().db()
    }

    #[inline]
    fn real_stored_len(&self) -> usize {
        (self.region().meta().len() - HEADER_OFFSET) / Self::SIZE_OF_T
    }

    #[inline]
    fn stored_len(&self) -> usize {
        self.base.stored_len()
    }

    fn write(&mut self) -> Result<bool> {
        self.write_header_if_needed()?;

        let stored_len = self.stored_len();
        let pushed_len = self.pushed_len();
        let real_stored_len = self.real_stored_len();
        // After rollback, stored_len can be > real_stored_len (missing items are in updated map)
        let truncated = stored_len < real_stored_len;
        let expanded = stored_len > real_stored_len;
        let has_new_data = pushed_len != 0;
        let has_updated_data = !self.updated().is_empty();
        let has_holes = !self.holes().is_empty();
        let had_holes = self.has_stored_holes;

        if !truncated && !expanded && !has_new_data && !has_updated_data && !has_holes && !had_holes
        {
            return Ok(false);
        }

        let from = stored_len * Self::SIZE_OF_T + HEADER_OFFSET;

        if has_new_data {
            // Serialize pushed values using strategy
            let mut bytes = Vec::with_capacity(pushed_len * Self::SIZE_OF_T);
            for v in mem::take(self.mut_pushed()).iter() {
                S::write_to_vec(v, &mut bytes);
            }
            self.region().clone().truncate_write(from, &bytes)?;
            self.update_stored_len(stored_len + pushed_len);
        } else if truncated {
            self.region().truncate(from)?;
        }

        if has_updated_data {
            let updated = self.updated.take_current();
            let region = self.region();

            if unlikely(expanded) {
                // After rollback, updates may extend beyond current disk length.
                // Use write_at which handles extension (slower but necessary).
                let mut bytes = Vec::with_capacity(Self::SIZE_OF_T);
                for (index, value) in updated {
                    let offset = index * Self::SIZE_OF_T + HEADER_OFFSET;
                    bytes.clear();
                    S::write_to_vec(&value, &mut bytes);
                    region.write_at(&bytes, offset)?;
                }
            } else {
                // Normal case: write directly to mmap, no intermediate allocations
                region.batch_write_each(
                    updated
                        .into_iter()
                        .map(|(index, value)| (index * Self::SIZE_OF_T + HEADER_OFFSET, value)),
                    Self::SIZE_OF_T,
                    S::write_to_slice,
                );
            }
        }

        if has_holes {
            self.has_stored_holes = true;
            let holes_region = self
                .region()
                .db()
                .create_region_if_needed(&self.holes_region_name())?;
            let holes = self.holes();
            let mut bytes = Vec::with_capacity(holes.len() * size_of::<usize>());
            for i in holes.iter() {
                bytes.extend_from_slice(&i.to_ne_bytes());
            }
            holes_region.truncate_write(0, &bytes)?;
        } else if had_holes {
            self.has_stored_holes = false;
            let db = self.region().db();
            let holes_name = self.holes_region_name();
            debug!("{}: removing holes region '{}'", db, holes_name);
            db.remove_region(&holes_name)?;
        }

        Ok(true)
    }

    fn region(&self) -> &Region {
        self.base.region()
    }

    fn serialize_changes(&self) -> Result<Vec<u8>> {
        // Get base serialization (stamp, stored_len info, truncated, pushed)
        let mut bytes = self.default_serialize_changes()?;

        // Append RawVecInner-specific data: updated, prev_holes
        let reader = self.create_reader();

        let (modified_indexes, modified_values) = self
            .updated()
            .keys()
            .map(|&i| {
                // Prefer prev_updated values over disk values (for post-rollback state)
                let val = self
                    .prev_updated()
                    .get(&i)
                    .cloned()
                    .unwrap_or_else(|| self.unchecked_read_at(i, &reader).unwrap());
                (i, val)
            })
            .collect::<(Vec<_>, Vec<_>)>();
        bytes.extend(modified_indexes.len().to_bytes().as_ref());
        bytes.extend(modified_indexes.to_bytes());
        // Serialize values using strategy
        for v in &modified_values {
            S::write_to_vec(v, &mut bytes);
        }

        let prev_holes = self.prev_holes().iter().copied().collect::<Vec<_>>();
        bytes.extend(prev_holes.len().to_bytes());
        bytes.extend(prev_holes.to_bytes());

        Ok(bytes)
    }

    fn any_stamped_write_with_changes(&mut self, stamp: Stamp) -> Result<()> {
        <Self as GenericStoredVec<I, T>>::stamped_write_with_changes(self, stamp)
    }

    fn remove(self) -> Result<()> {
        Self::remove(self)
    }

    fn any_reset(&mut self) -> Result<()> {
        <Self as GenericStoredVec<I, T>>::reset(self)
    }
}

impl<I, T, S> GenericStoredVec<I, T> for RawVecInner<I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: RawStrategy<T>,
{
    fn collect_stored_range(&self, from: usize, to: usize) -> Result<Vec<T>> {
        // Must use reader directly — fold_source clamps to stored_len,
        // but this method may read truncated values still on disk.
        let reader = self.create_reader();
        (from..to)
            .map(|i| {
                if let Some(val) = self.prev_updated().get(&i) {
                    Ok(val.clone())
                } else {
                    self.unchecked_read_at(i, &reader)
                }
            })
            .collect()
    }

    #[inline(always)]
    fn read_value_from_bytes(&self, bytes: &[u8]) -> Result<T> {
        S::read(bytes)
    }

    #[inline(always)]
    fn write_value_to(&self, value: &T, buf: &mut Vec<u8>) {
        S::write_to_vec(value, buf);
    }

    #[inline(always)]
    fn pushed(&self) -> &[T] {
        self.base.pushed()
    }
    #[inline(always)]
    fn mut_pushed(&mut self) -> &mut Vec<T> {
        self.base.mut_pushed()
    }
    #[inline(always)]
    fn prev_pushed(&self) -> &[T] {
        self.base.prev_pushed()
    }
    #[inline(always)]
    fn mut_prev_pushed(&mut self) -> &mut Vec<T> {
        self.base.mut_prev_pushed()
    }

    #[inline(always)]
    fn prev_stored_len(&self) -> usize {
        self.base.prev_stored_len()
    }
    #[inline(always)]
    fn mut_prev_stored_len(&mut self) -> &mut usize {
        self.base.mut_prev_stored_len()
    }
    #[inline(always)]
    fn update_stored_len(&self, val: usize) {
        self.base.update_stored_len(val);
    }

    fn reset(&mut self) -> Result<()> {
        // Clear holes and updated data (specific to RawVecInner)
        self.holes.clear();
        self.updated.clear();

        // Use default reset for common cleanup
        self.default_reset()
    }

    fn restore_truncated_value(&mut self, index: usize, value: T) {
        // RawVecInner restores truncated values into the updated map instead of pushing
        self.mut_updated().insert(index, value);
    }

    fn truncate_if_needed_at(&mut self, index: usize) -> Result<()> {
        // Handle holes - clear any beyond index
        if self.holes().last().is_some_and(|&h| h >= index) {
            self.mut_holes().retain(|&i| i < index);
        }

        // Handle updated - clear any beyond index
        if self
            .updated()
            .last_key_value()
            .is_some_and(|(&k, _)| k >= index)
        {
            self.mut_updated().retain(|&i, _| i < index);
        }

        // Call default which handles pushed layer and stored_len
        if self.default_truncate_if_needed_at(index)? {
            self.update_stored_len(index);
        }

        Ok(())
    }

    fn reset_unsaved(&mut self) {
        self.default_reset_unsaved();
        self.holes.clear();
        self.updated.clear();
    }

    fn is_dirty(&self) -> bool {
        !self.is_pushed_empty() || !self.holes().is_empty() || !self.updated().is_empty()
    }

    fn stamped_write_with_changes(&mut self, stamp: Stamp) -> Result<()> {
        if self.saved_stamped_changes() == 0 {
            return self.stamped_write(stamp);
        }

        // Call default which handles file management, serialize, flush, and updates prev_stored_len/prev_pushed
        // serialize_changes() saves prev_holes, so we must call this BEFORE holes.save()
        self.default_stamped_write_with_changes(stamp)?;

        // Now update prev_ fields for next iteration
        self.holes.save();
        self.updated.clear_previous();

        Ok(())
    }

    fn rollback_before(&mut self, stamp: Stamp) -> Result<Stamp> {
        // Call default which handles the rollback loop and updates prev_stored_len/prev_pushed
        let result = self.default_rollback_before(stamp)?;

        // Update RawVecInner-specific prev_ fields
        self.holes.save();
        self.updated.save();

        Ok(result)
    }

    fn rollback(&mut self) -> Result<()> {
        let path = self
            .changes_path()
            .join(u64::from(self.stamp()).to_string());
        let bytes = fs::read(&path)?;
        self.deserialize_then_undo_changes(&bytes)
    }

    fn deserialize_then_undo_changes(&mut self, bytes: &[u8]) -> Result<()> {
        // Parse base data (stamp, stored_len, truncated, pushed)
        let mut pos = self.default_deserialize_then_undo_changes(bytes)?;
        let mut len = SIZE_OF_U64;

        // Parse RawVecInner-specific data: updated, prev_holes

        let modified_len = usize::from_bytes(&bytes[pos..pos + len])?;
        pos += len;
        len = SIZE_OF_U64 * modified_len;
        let indexes = bytes[pos..pos + len].chunks(SIZE_OF_U64);
        pos += len;
        len = Self::SIZE_OF_T * modified_len;
        let values = bytes[pos..pos + len].chunks(Self::SIZE_OF_T);
        let old_values_to_restore: BTreeMap<usize, T> = indexes
            .zip(values)
            .map(|(i, v)| {
                let idx = usize::from_bytes(i)?;
                let val = S::read(v)?;
                Ok((idx, val))
            })
            .collect::<Result<_>>()?;
        pos += len;

        len = SIZE_OF_U64;
        let prev_holes_len = usize::from_bytes(&bytes[pos..pos + len])?;
        pos += len;
        len = SIZE_OF_U64 * prev_holes_len;
        let prev_holes = bytes[pos..pos + len]
            .chunks(SIZE_OF_U64)
            .map(usize::from_bytes)
            .collect::<Result<BTreeSet<_>>>()?;

        if !self.holes().is_empty() || !self.prev_holes().is_empty() || !prev_holes.is_empty() {
            *self.holes.current_mut() = prev_holes.clone();
            *self.holes.previous_mut() = prev_holes;
        }

        // Restore old values to updated map
        for (i, v) in old_values_to_restore {
            self.update_at(i, v)?;
        }

        // Update prev_ fields
        self.updated.save();

        Ok(())
    }
}

impl<I, T, S> ScannableVec<I, T> for RawVecInner<I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: RawStrategy<T>,
{
    fn for_each_range_dyn(&self, from: usize, to: usize, f: &mut dyn FnMut(T)) {
        let len = self.len_();
        let from = from.min(len);
        let to = to.min(len);
        if from >= to {
            return;
        }

        let stored_len = self.stored_len();
        let is_clean =
            self.holes().is_empty() && self.updated().is_empty() && self.pushed().is_empty();

        if is_clean && to <= stored_len {
            self.fold_source(from, to, (), |(), v| f(v));
        } else {
            self.fold_dirty(from, to, (), |(), v| f(v));
        }
    }

    fn fold_range<B, F: FnMut(B, T) -> B>(&self, from: usize, to: usize, init: B, f: F) -> B
    where
        Self: Sized,
    {
        let len = self.len_();
        let from = from.min(len);
        let to = to.min(len);
        if from >= to {
            return init;
        }

        let stored_len = self.stored_len();
        let is_clean =
            self.holes().is_empty() && self.updated().is_empty() && self.pushed().is_empty();

        if is_clean && to <= stored_len {
            self.fold_source(from, to, init, f)
        } else {
            self.fold_dirty(from, to, init, f)
        }
    }

    fn try_fold_range<B, E, F: FnMut(B, T) -> std::result::Result<B, E>>(
        &self,
        from: usize,
        to: usize,
        init: B,
        f: F,
    ) -> std::result::Result<B, E>
    where
        Self: Sized,
    {
        let len = self.len_();
        let from = from.min(len);
        let to = to.min(len);
        if from >= to {
            return Ok(init);
        }

        let stored_len = self.stored_len();
        let is_clean =
            self.holes().is_empty() && self.updated().is_empty() && self.pushed().is_empty();

        if is_clean && to <= stored_len {
            self.try_fold_source(from, to, init, f)
        } else {
            self.try_fold_dirty(from, to, init, f)
        }
    }
}

impl<I, T, S> TypedVec for RawVecInner<I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: RawStrategy<T>,
{
    type I = I;
    type T = T;
}
