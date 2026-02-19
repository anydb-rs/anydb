use std::{
    collections::{BTreeMap, BTreeSet},
    marker::PhantomData,
    path::PathBuf,
};

use log::{debug, info};
use rawdb::{Database, Reader, Region, likely, unlikely};

use crate::{
    AnyStoredVec, AnyVec, BUFFER_SIZE, BaseVec, Bytes, Error, Format, HEADER_OFFSET,
    Header, ImportOptions, MMAP_CROSSOVER_BYTES, RawIoSource, RawMmapSource, ReadableVec, Result,
    SIZE_OF_U64, Stamp, TypedVec, VecIndex, VecReader, VecValue, Version, WithPrev, WritableVec,
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
    holes: WithPrev<BTreeSet<usize>>,
    updated: WithPrev<BTreeMap<usize, T>>,
    has_stored_holes: bool,
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

    #[inline(always)]
    pub fn pushed(&self) -> &[T] {
        self.base.pushed()
    }

    #[inline(always)]
    pub fn mut_pushed(&mut self) -> &mut Vec<T> {
        self.base.mut_pushed()
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
        if unlikely(!self.holes().is_empty()) && self.holes().contains(&index) {
            return Ok(None);
        }

        let stored_len = self.stored_len();

        // Check pushed (beyond stored length)
        if index >= stored_len {
            return Ok(self.base.pushed().get(index - stored_len).cloned());
        }

        // Check updated layer
        if unlikely(!self.updated().is_empty())
            && let Some(updated_value) = self.updated().get(&index)
        {
            return Ok(Some(updated_value.clone()));
        }

        // Fall back to reading from storage
        Ok(Some(self.unchecked_read_at(index, reader)))
    }

    // ── Point reads (clean: pushed + stored only) ──────────────────

    /// Gets a value from pushed or stored layers only. No holes/updates checks.
    ///
    /// Uses a `VecReader` with a cached mmap base pointer for O(1) stored reads.
    /// Use when you know the vec has no holes or updates (e.g. append-only vecs).
    #[inline(always)]
    pub fn get_pushed_or_read(&self, index: I, reader: &VecReader<I, T, S>) -> Option<T> {
        self.get_pushed_or_read_at(index.to_usize(), reader)
    }

    /// Gets a value from pushed or stored layers at a usize index. No holes/updates checks.
    #[inline(always)]
    pub fn get_pushed_or_read_at(&self, index: usize, reader: &VecReader<I, T, S>) -> Option<T> {
        let stored_len = self.stored_len();
        if index >= stored_len {
            return self.base.pushed().get(index - stored_len).cloned();
        }
        Some(reader.get(index))
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
            if let Some(prev) = self.base.mut_pushed().get_mut(index - stored_len) {
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
            .unwrap_or_else(|| self.base.len())
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
                self.base.mut_pushed().push(value);
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
    pub fn unchecked_read_at(&self, index: usize, reader: &Reader) -> T {
        let ptr = reader.prefixed(HEADER_OFFSET).as_ptr();
        unsafe { S::read_from_ptr(ptr, index * Self::SIZE_OF_T) }
    }

    /// Reads a stored value at `index` using the provided reader.
    #[inline(always)]
    pub fn read_at(&self, index: usize, reader: &Reader) -> Result<T> {
        let len = self.base.len();
        if likely(index < len) {
            Ok(self.unchecked_read_at(index, reader))
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
        self.base.index_to_name()
    }

    /// Creates a `VecReader` for O(1) random access to stored values.
    #[inline]
    pub fn reader(&self) -> VecReader<I, T, S> {
        VecReader::new(self)
    }

    // ── Strategy-specific methods (moved from trait) ─────────────────

    /// Collects stored values in `[from, to)` for serialization purposes.
    pub(crate) fn collect_stored_range(&self, from: usize, to: usize) -> Result<Vec<T>> {
        let reader = self.create_reader();
        Ok((from..to)
            .map(|i| {
                if let Some(val) = self.prev_updated().get(&i) {
                    val.clone()
                } else {
                    self.unchecked_read_at(i, &reader)
                }
            })
            .collect())
    }

    /// Restores a truncated value into the updated map.
    fn restore_truncated_value(&mut self, index: usize, value: T) {
        self.mut_updated().insert(index, value);
    }

    /// Deserializes change data and undoes those changes.
    fn deserialize_then_undo_changes(&mut self, bytes: &[u8]) -> Result<()> {
        // Parse base data
        let change = BaseVec::<I, T>::parse_change_data(bytes, Self::SIZE_OF_T, |b| S::read(b))?;
        let mut pos = change.bytes_consumed;
        let mut len = SIZE_OF_U64;

        // Type-specific truncation handling
        let current_stored_len = self.stored_len();
        if change.prev_stored_len < current_stored_len {
            // Truncate holes/updated beyond new stored_len
            if self
                .holes()
                .last()
                .is_some_and(|&h| h >= change.prev_stored_len)
            {
                self.mut_holes().retain(|&i| i < change.prev_stored_len);
            }
            if self
                .updated()
                .last_key_value()
                .is_some_and(|(&k, _)| k >= change.prev_stored_len)
            {
                self.mut_updated()
                    .retain(|&i, _| i < change.prev_stored_len);
            }
        }

        // Apply base rollback (updates stamp, stored_len, clears pushed, appends prev_pushed)
        self.base.apply_rollback(&change);

        // Restore truncated values into updated map
        for (i, val) in change.truncated_values.into_iter().enumerate() {
            self.restore_truncated_value(change.truncated_start + i, val);
        }

        // Parse RawVecInner-specific data: updated, prev_holes
        fn check_bounds(bytes: &[u8], pos: usize, len: usize) -> Result<()> {
            let end = pos.checked_add(len).ok_or(Error::Overflow)?;
            if end > bytes.len() {
                return Err(Error::WrongLength {
                    received: bytes.len(),
                    expected: end,
                });
            }
            Ok(())
        }

        check_bounds(bytes, pos, len)?;
        let modified_len = usize::from_bytes(&bytes[pos..pos + len])?;
        pos += len;

        // Parse indexes
        len = SIZE_OF_U64
            .checked_mul(modified_len)
            .ok_or(Error::Overflow)?;
        check_bounds(bytes, pos, len)?;
        let indexes_start = pos;
        pos += len;

        // Parse values and restore directly into updated map
        len = Self::SIZE_OF_T
            .checked_mul(modified_len)
            .ok_or(Error::Overflow)?;
        check_bounds(bytes, pos, len)?;
        let mut idx_pos = indexes_start;
        let mut val_pos = pos;
        for _ in 0..modified_len {
            let idx = usize::from_bytes(&bytes[idx_pos..idx_pos + SIZE_OF_U64])?;
            let val = S::read(&bytes[val_pos..val_pos + Self::SIZE_OF_T])?;
            self.update_at(idx, val)?;
            idx_pos += SIZE_OF_U64;
            val_pos += Self::SIZE_OF_T;
        }
        pos += len;

        len = SIZE_OF_U64;
        check_bounds(bytes, pos, len)?;
        let prev_holes_len = usize::from_bytes(&bytes[pos..pos + len])?;
        pos += len;
        len = SIZE_OF_U64
            .checked_mul(prev_holes_len)
            .ok_or(Error::Overflow)?;
        check_bounds(bytes, pos, len)?;

        if prev_holes_len > 0 || !self.holes().is_empty() || !self.prev_holes().is_empty() {
            let prev_holes = bytes[pos..pos + len]
                .chunks(SIZE_OF_U64)
                .map(usize::from_bytes)
                .collect::<Result<BTreeSet<_>>>()?;
            *self.holes.current_mut() = prev_holes;
            self.holes.save();
        }

        // Update prev_ fields
        self.updated.save();

        Ok(())
    }

    // ── Source helpers (internal, for ReadableVec) ──────────────────

    /// Fold over stored data using auto-selected source (mmap or IO).
    #[inline]
    fn fold_source<B, F: FnMut(B, T) -> B>(&self, from: usize, to: usize, init: B, f: F) -> B {
        let range_bytes = to.saturating_sub(from) * size_of::<T>();
        if range_bytes > MMAP_CROSSOVER_BYTES {
            RawIoSource::new(self, from, to).fold(init, f)
        } else {
            RawMmapSource::new(self, from, to).fold(init, f)
        }
    }

    /// Fallible fold over stored data using auto-selected source.
    #[inline]
    fn try_fold_source<B, E, F: FnMut(B, T) -> std::result::Result<B, E>>(
        &self,
        from: usize,
        to: usize,
        init: B,
        f: F,
    ) -> std::result::Result<B, E> {
        let range_bytes = to.saturating_sub(from) * size_of::<T>();
        if range_bytes > MMAP_CROSSOVER_BYTES {
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

    /// Whether stored data has dirty state (holes or updates) requiring element-by-element access.
    #[inline]
    fn has_dirty_stored(&self) -> bool {
        !self.holes().is_empty() || !self.updated().is_empty()
    }

    /// Element-by-element fold when stored data has holes or updates.
    /// Own implementation (not delegating to try_fold_dirty) so LLVM can vectorize without `?` penalty.
    fn fold_dirty<B, F: FnMut(B, T) -> B>(&self, from: usize, to: usize, init: B, mut f: F) -> B {
        let stored_len = self.stored_len();
        let reader = self.create_reader();
        let data_ptr = reader.prefixed(HEADER_OFFSET).as_ptr();
        let mut acc = init;

        let stored_to = to.min(stored_len);
        let mut hole_iter = self.holes().range(from..to).peekable();
        let mut update_iter = self.updated().range(from..stored_to).peekable();

        let mut byte_off = from * Self::SIZE_OF_T;
        for i in from..stored_to {
            if unlikely(hole_iter.peek() == Some(&&i)) {
                hole_iter.next();
                byte_off += Self::SIZE_OF_T;
                continue;
            }
            let val = if unlikely(update_iter.peek().is_some_and(|&(&k, _)| k == i)) {
                update_iter.next().unwrap().1.clone()
            } else {
                unsafe { S::read_from_ptr(data_ptr, byte_off) }
            };
            byte_off += Self::SIZE_OF_T;
            acc = f(acc, val);
        }

        let push_from = from.max(stored_len);
        if push_from < to {
            let pushed = self.base.pushed();
            for i in push_from..to {
                if unlikely(hole_iter.peek() == Some(&&i)) {
                    hole_iter.next();
                    continue;
                }
                if let Some(v) = pushed.get(i - stored_len) {
                    acc = f(acc, v.clone());
                }
            }
        }

        acc
    }

    /// Element-by-element fallible fold when stored data has holes or updates.
    /// Uses range iterators over holes/updates for O(1) amortized lookups.
    fn try_fold_dirty<B, E, F: FnMut(B, T) -> std::result::Result<B, E>>(
        &self,
        from: usize,
        to: usize,
        init: B,
        mut f: F,
    ) -> std::result::Result<B, E> {
        let stored_len = self.stored_len();
        let reader = self.create_reader();
        let data_ptr = reader.prefixed(HEADER_OFFSET).as_ptr();
        let mut acc = init;

        // Walk sorted holes/updates forward alongside index range — O(1) amortized per element.
        let stored_to = to.min(stored_len);
        let mut hole_iter = self.holes().range(from..to).peekable();
        let mut update_iter = self.updated().range(from..stored_to).peekable();

        let mut byte_off = from * Self::SIZE_OF_T;
        for i in from..stored_to {
            if unlikely(hole_iter.peek() == Some(&&i)) {
                hole_iter.next();
                byte_off += Self::SIZE_OF_T;
                continue;
            }
            let val = if unlikely(update_iter.peek().is_some_and(|&(&k, _)| k == i)) {
                update_iter.next().unwrap().1.clone()
            } else {
                // SAFETY: i < stored_len, reader holds mmap guard
                unsafe { S::read_from_ptr(data_ptr, byte_off) }
            };
            byte_off += Self::SIZE_OF_T;
            acc = f(acc, val)?;
        }

        let push_from = from.max(stored_len);
        if push_from < to {
            let pushed = self.base.pushed();
            for i in push_from..to {
                if unlikely(hole_iter.peek() == Some(&&i)) {
                    hole_iter.next();
                    continue;
                }
                if let Some(v) = pushed.get(i - stored_len) {
                    acc = f(acc, v.clone())?;
                }
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
        self.base.version()
    }

    #[inline]
    fn name(&self) -> &str {
        self.base.name()
    }

    #[inline]
    fn len(&self) -> usize {
        self.base.len()
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
        self.base.write_header_if_needed()?;

        let stored_len = self.stored_len();
        let pushed_len = self.base.pushed().len();
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
            if S::IS_NATIVE_LAYOUT {
                // Bulk write: memory layout matches serialized format, skip per-value
                // serialization entirely. Single memcpy from pushed buffer to mmap.
                let pushed = self.base.pushed();
                let bytes = unsafe {
                    std::slice::from_raw_parts(
                        pushed.as_ptr() as *const u8,
                        pushed.len() * Self::SIZE_OF_T,
                    )
                };
                self.region().truncate_write(from, bytes)?;
            } else {
                let mut bytes = Vec::with_capacity(pushed_len * Self::SIZE_OF_T);
                for v in self.base.pushed() {
                    S::write_to_vec(v, &mut bytes);
                }
                self.region().truncate_write(from, &bytes)?;
            }
            self.base.mut_pushed().clear();
            self.base.update_stored_len(stored_len + pushed_len);
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
                bytes.extend_from_slice(i.to_bytes().as_ref());
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
        let mut bytes = self.base.serialize_changes(
            Self::SIZE_OF_T,
            |from, to| self.collect_stored_range(from, to),
            |vals, buf| {
                for v in vals {
                    S::write_to_vec(v, buf);
                }
            },
        )?;

        // Append RawVecInner-specific data: updated indexes+values, prev_holes
        let reader = self.create_reader();
        let updated = self.updated();

        bytes.extend(updated.len().to_bytes().as_ref());
        // Write indexes directly
        for &i in updated.keys() {
            bytes.extend(i.to_bytes().as_ref());
        }
        // Write values directly
        for (&i, _) in updated.iter() {
            if let Some(v) = self.prev_updated().get(&i) {
                S::write_to_vec(v, &mut bytes);
            } else {
                S::write_to_vec(&self.unchecked_read_at(i, &reader), &mut bytes);
            }
        }

        let prev_holes = self.prev_holes();
        bytes.extend(prev_holes.len().to_bytes().as_ref());
        for &hole in prev_holes.iter() {
            bytes.extend(hole.to_bytes().as_ref());
        }

        Ok(bytes)
    }

    fn any_stamped_write_with_changes(&mut self, stamp: Stamp) -> Result<()> {
        <Self as WritableVec<I, T>>::stamped_write_with_changes(self, stamp)
    }

    fn remove(self) -> Result<()> {
        Self::remove(self)
    }

    fn any_reset(&mut self) -> Result<()> {
        <Self as WritableVec<I, T>>::reset(self)
    }
}

impl<I, T, S> WritableVec<I, T> for RawVecInner<I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: RawStrategy<T>,
{
    #[inline]
    fn push(&mut self, value: T) {
        self.base.mut_pushed().push(value);
    }

    #[inline]
    fn pushed(&self) -> &[T] {
        self.base.pushed()
    }

    fn truncate_if_needed_at(&mut self, index: usize) -> Result<()> {
        // Handle holes - drop any at or beyond index
        if self.holes().last().is_some_and(|&h| h >= index) {
            self.mut_holes().split_off(&index);
        }

        // Handle updated - drop any at or beyond index
        if self
            .updated()
            .last_key_value()
            .is_some_and(|(&k, _)| k >= index)
        {
            self.mut_updated().split_off(&index);
        }

        // Truncate pushed layer and update stored_len if needed
        if self.base.truncate_pushed(index) {
            self.base.update_stored_len(index);
        }

        Ok(())
    }

    fn reset(&mut self) -> Result<()> {
        self.holes.clear();
        self.updated.clear();
        self.truncate_if_needed_at(0)?;
        self.base.reset_base()
    }

    fn reset_unsaved(&mut self) {
        self.base.reset_unsaved_base();
        self.holes.clear();
        self.updated.clear();
    }

    fn is_dirty(&self) -> bool {
        !self.base.pushed().is_empty() || !self.holes().is_empty() || !self.updated().is_empty()
    }

    fn stamped_write_with_changes(&mut self, stamp: Stamp) -> Result<()> {
        if self.base.saved_stamped_changes() == 0 {
            return self.stamped_write(stamp);
        }

        // serialize_changes() reads prev_holes, so must happen BEFORE holes.save()
        let data = self.serialize_changes()?;
        self.base.save_change_file(stamp, &data)?;
        self.stamped_write(stamp)?;
        self.base.save_prev();
        self.holes.save();
        self.updated.clear_previous();

        Ok(())
    }

    fn rollback(&mut self) -> Result<()> {
        let bytes = self.base.read_current_change_file()?;
        self.deserialize_then_undo_changes(&bytes)
    }

    fn find_rollback_files(&self) -> Result<BTreeMap<Stamp, PathBuf>> {
        self.base.find_rollback_files()
    }

    fn save_rollback_state(&mut self) {
        self.base.save_prev_for_rollback();
        self.holes.save();
        self.updated.save();
    }
}

impl<I, T, S> ReadableVec<I, T> for RawVecInner<I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: RawStrategy<T>,
{
    #[inline]
    fn for_each_range_dyn(&self, from: usize, to: usize, f: &mut dyn FnMut(T)) {
        self.fold_range(from, to, (), |(), v| f(v));
    }

    #[inline]
    fn fold_range<B, F: FnMut(B, T) -> B>(&self, from: usize, to: usize, init: B, mut f: F) -> B
    where
        Self: Sized,
    {
        let len = self.base.len();
        let from = from.min(len);
        let to = to.min(len);
        if from >= to {
            return init;
        }

        if self.has_dirty_stored() {
            return self.fold_dirty(from, to, init, f);
        }

        let stored_len = self.stored_len();

        if to <= stored_len {
            return self.fold_source(from, to, init, f);
        }

        let mut acc = init;
        if from < stored_len {
            acc = self.fold_source(from, stored_len, acc, &mut f);
        }
        self.base.fold_pushed(from, to, acc, f)
    }

    #[inline]
    fn try_fold_range<B, E, F: FnMut(B, T) -> std::result::Result<B, E>>(
        &self,
        from: usize,
        to: usize,
        init: B,
        mut f: F,
    ) -> std::result::Result<B, E>
    where
        Self: Sized,
    {
        let len = self.base.len();
        let from = from.min(len);
        let to = to.min(len);
        if from >= to {
            return Ok(init);
        }

        if self.has_dirty_stored() {
            return self.try_fold_dirty(from, to, init, f);
        }

        let stored_len = self.stored_len();

        if to <= stored_len {
            return self.try_fold_source(from, to, init, f);
        }

        let mut acc = init;
        if from < stored_len {
            acc = self.try_fold_source(from, stored_len, acc, &mut f)?;
        }
        self.base.try_fold_pushed(from, to, acc, f)
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
