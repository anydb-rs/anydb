use std::{collections::BTreeMap, marker::PhantomData, path::PathBuf, sync::Arc};

use log::info;
use parking_lot::RwLock;
use rawdb::{Database, Reader, Region};

use crate::{
    AnyStoredVec, AnyVec, BaseVec, CompressedIoSource, CompressedMmapSource, Error, Format,
    HEADER_OFFSET, Header, ImportOptions, MMAP_CROSSOVER_BYTES, ReadableVec, Result, Stamp,
    TypedVec, VecIndex, VecValue, Version, WritableVec, likely, short_type_name, unlikely,
    vec_region_name_with,
};

mod page;
mod pages;
mod strategy;

pub use page::*;
pub use pages::*;
pub use strategy::*;

/// Maximum size in bytes of a single uncompressed page (16 KiB).
/// Smaller pages reduce memory overhead during decompression and improve
/// random access performance, while larger pages compress more efficiently.
/// 16 KiB balances these trade-offs for typical workloads.
pub const MAX_UNCOMPRESSED_PAGE_SIZE: usize = 16 * 1024;

const VERSION: Version = Version::new(3);

/// Inner implementation for compressed storage vectors.
/// Parameterized by compression strategy to support different compression algorithms.
#[derive(Debug, Clone)]
#[must_use = "Vector should be stored to keep data accessible"]
pub struct CompressedVecInner<I, T, S> {
    base: BaseVec<I, T>,
    pages: Arc<RwLock<Pages>>,
    _strategy: PhantomData<S>,
}

impl<I, T, S> CompressedVecInner<I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: CompressionStrategy<T>,
{
    const PER_PAGE: usize = MAX_UNCOMPRESSED_PAGE_SIZE / Self::SIZE_OF_T;

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
                options
                    .db
                    .remove_region_if_exists(&Self::pages_region_name_(options.name))?;
                Self::import_with(options, format)
            }
            _ => res,
        }
    }

    #[inline]
    pub fn import_with(mut options: ImportOptions, format: Format) -> Result<Self> {
        options.version = options.version + VERSION;
        let db = options.db;
        let name = options.name;

        let base = BaseVec::import(options, format)?;

        let pages = Pages::import(db, &Self::pages_region_name_(name))?;

        let mut this = Self {
            base,
            pages: Arc::new(RwLock::new(pages)),
            _strategy: PhantomData,
        };

        let len = this.real_stored_len();
        *this.base.mut_prev_stored_len() = len;
        this.base.update_stored_len(len);

        Ok(this)
    }

    /// Decodes a compressed page, returning the decompressed values.
    #[inline]
    pub fn decode_page(&self, page_index: usize, reader: &Reader) -> Result<Vec<T>> {
        Self::decode_page_(self.stored_len(), page_index, reader, &self.pages.read())
    }

    /// Static version of decode_page that takes pages directly.
    #[inline]
    pub(crate) fn decode_page_(
        stored_len: usize,
        page_index: usize,
        reader: &Reader,
        pages: &Pages,
    ) -> Result<Vec<T>> {
        let index = Self::page_index_to_index(page_index);

        if unlikely(index >= stored_len) {
            return Err(Error::IndexTooHigh {
                index,
                len: stored_len,
                name: "pcodec_page".to_string(),
            });
        } else if unlikely(page_index >= pages.len()) {
            return Err(Error::ExpectVecToHaveIndex);
        }

        // SAFETY: We checked page_index < pages.len() above
        let page = pages
            .get(page_index)
            .expect("page should exist after bounds check");
        let len = page.bytes as usize;
        let offset = page.start as usize;

        let compressed_data = reader.unchecked_read(offset, len);
        Self::decompress_bytes(compressed_data, page.values as usize)
    }

    /// Stateless: decompress raw bytes into Vec<T>
    #[inline]
    fn decompress_bytes(compressed_data: &[u8], expected_len: usize) -> Result<Vec<T>> {
        let vec = S::decompress(compressed_data, expected_len)?;

        if likely(vec.len() == expected_len) {
            return Ok(vec);
        }

        Err(Error::DecompressionMismatch {
            expected_len,
            actual_len: vec.len(),
        })
    }

    #[inline]
    fn compress_page(chunk: &[T]) -> Result<Vec<u8>> {
        debug_assert!(
            chunk.len() <= Self::PER_PAGE,
            "chunk length {} exceeds PER_PAGE {}",
            chunk.len(),
            Self::PER_PAGE
        );

        S::compress(chunk)
    }

    #[inline]
    pub(crate) fn index_to_page_index(index: usize) -> usize {
        index / Self::PER_PAGE
    }

    #[inline]
    pub(crate) fn page_index_to_index(page_index: usize) -> usize {
        page_index * Self::PER_PAGE
    }

    pub(crate) fn pages_region_name(&self) -> String {
        Self::pages_region_name_(self.name())
    }
    fn pages_region_name_(name: &str) -> String {
        format!("{}_pages", vec_region_name_with::<I>(name))
    }

    /// Removes this vector and all its associated regions from the database
    pub fn remove(self) -> Result<()> {
        // Remove main region
        self.base.remove()?;

        // Remove pages region
        let pages = Arc::try_unwrap(self.pages).map_err(|_| Error::PagesStillReferenced)?;
        pages.into_inner().remove()?;

        Ok(())
    }

    #[inline]
    pub fn reserve_pushed(&mut self, additional: usize) {
        self.base.reserve_pushed(additional);
    }

    #[inline]
    pub(crate) fn create_reader(&self) -> Reader {
        self.base.region().create_reader()
    }

    #[inline]
    pub(crate) fn pages(&self) -> &Arc<RwLock<Pages>> {
        &self.pages
    }

    // ── Strategy-specific methods (moved from trait) ─────────────────

    /// Collects stored values in `[from, to)` for serialization purposes.
    pub(crate) fn collect_stored_range(&self, from: usize, to: usize) -> Result<Vec<T>> {
        if from >= to {
            return Ok(vec![]);
        }

        let reader = self.create_reader();
        let pages = self.pages.read();
        let real_len = pages.stored_len(Self::PER_PAGE);
        let to = to.min(real_len);
        if from >= to {
            return Ok(vec![]);
        }

        let mut result = Vec::with_capacity(to - from);
        let start_page = from / Self::PER_PAGE;
        let end_page = (to - 1) / Self::PER_PAGE;

        for page_idx in start_page..=end_page {
            let page_start = page_idx * Self::PER_PAGE;
            let decoded = Self::decode_page_(real_len, page_idx, &reader, &pages)?;
            let local_from = from.saturating_sub(page_start);
            let local_to = (to - page_start).min(decoded.len());
            result.extend_from_slice(&decoded[local_from..local_to]);
        }

        Ok(result)
    }

    /// Deserializes change data and undoes those changes.
    fn deserialize_then_undo_changes(&mut self, bytes: &[u8]) -> Result<()> {
        let change = BaseVec::<I, T>::parse_change_data(bytes, Self::SIZE_OF_T, |b| S::read(b))?;

        // Type-specific truncation: handle pages
        let current_stored_len = self.stored_len();
        if change.prev_stored_len < current_stored_len {
            self.base.update_stored_len(change.prev_stored_len);
        }

        // Apply base rollback
        self.base.apply_rollback(&change);

        // Restore truncated values by pushing them back
        for val in change.truncated_values {
            self.base.mut_pushed().push(val);
        }

        Ok(())
    }

    // ── Source helpers ─────────────────────────────────────────────

    /// Fold over stored data using buffered file I/O (better for large scans).
    #[inline]
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
        CompressedIoSource::new(self, from, to).fold(init, f)
    }

    /// Fold over stored data using mmap (better for small/random reads).
    #[inline]
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
        CompressedMmapSource::new(self, from, to).fold(init, f)
    }

    /// Fold over stored data using auto-selected source (mmap or IO).
    #[inline]
    fn fold_source<B, F: FnMut(B, T) -> B>(&self, from: usize, to: usize, init: B, f: F) -> B {
        let range_bytes = to.saturating_sub(from) * size_of::<T>();
        if range_bytes > MMAP_CROSSOVER_BYTES {
            CompressedIoSource::new(self, from, to).fold(init, f)
        } else {
            CompressedMmapSource::new(self, from, to).fold(init, f)
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
            CompressedIoSource::new(self, from, to).try_fold(init, f)
        } else {
            CompressedMmapSource::new(self, from, to).try_fold(init, f)
        }
    }

}

impl<I, T, S> AnyVec for CompressedVecInner<I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: CompressionStrategy<T>,
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
        vec![self.base.index_to_name(), self.pages_region_name()]
    }
}

impl<I, T, S> AnyStoredVec for CompressedVecInner<I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: CompressionStrategy<T>,
{
    #[inline]
    fn db_path(&self) -> PathBuf {
        self.base.db_path()
    }

    #[inline]
    fn region(&self) -> &Region {
        self.base.region()
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

    #[inline]
    fn stored_len(&self) -> usize {
        self.base.stored_len()
    }

    #[inline]
    fn real_stored_len(&self) -> usize {
        self.pages.read().stored_len(Self::PER_PAGE)
    }

    fn write(&mut self) -> Result<bool> {
        self.base.write_header_if_needed()?;

        let stored_len = self.stored_len();
        let pushed_len = self.base.pushed().len();

        // Phase 1a: Copy metadata snapshot (minimal lock scope)
        let (truncate_at, starting_page_index, partial_page) = {
            let pages = self.pages.read();

            let real_stored_len = pages.stored_len(Self::PER_PAGE);
            if stored_len > real_stored_len {
                return Err(Error::CorruptedRegion { region_len: real_stored_len });
            }

            if pushed_len == 0 && stored_len == real_stored_len {
                return Ok(false);
            }

            let starting_page_index = Self::index_to_page_index(stored_len);
            if starting_page_index > pages.len() {
                return Err(Error::CorruptedRegion { region_len: pages.len() });
            }

            if starting_page_index < pages.len() {
                let partial_len = stored_len % Self::PER_PAGE;
                let page = *pages.get(starting_page_index)
                    .ok_or(Error::ExpectVecToHaveIndex)?;
                (page.start, starting_page_index, if partial_len != 0 { Some((page, partial_len)) } else { None })
            } else {
                let truncate_at = pages
                    .last()
                    .map_or(HEADER_OFFSET as u64, |page| page.start + page.bytes as u64);
                (truncate_at, starting_page_index, None)
            }
        };
        // Pages lock released — decompression happens without blocking readers

        // Phase 1b: Decompress partial page (if needed) outside lock
        let mut values = if let Some((page, partial_len)) = partial_page {
            let reader = self.create_reader();
            let compressed_data = reader.unchecked_read(page.start as usize, page.bytes as usize);
            let mut page_values = Self::decompress_bytes(compressed_data, page.values as usize)?;
            page_values.truncate(partial_len);
            page_values
        } else {
            vec![]
        };

        // Phase 2: Compress (no locks held)
        values.extend_from_slice(self.base.pushed());
        self.base.mut_pushed().clear();

        let num_pages = (values.len() + Self::PER_PAGE - 1) / Self::PER_PAGE;
        let mut buf = Vec::with_capacity(values.len() * Self::SIZE_OF_T);
        let mut page_sizes = Vec::with_capacity(num_pages);
        for chunk in values.chunks(Self::PER_PAGE) {
            let compressed = Self::compress_page(chunk)?;
            page_sizes.push((compressed.len(), chunk.len()));
            buf.extend_from_slice(&compressed);
        }

        // Phase 3: Write to region first (without holding pages lock to avoid deadlock)
        self.region().truncate_write(truncate_at as usize, &buf)?;

        // Now acquire pages lock and update page metadata
        let mut pages = self.pages.write();
        pages.truncate(starting_page_index);

        for (i, &(compressed_len, values_len)) in page_sizes.iter().enumerate() {
            let page_index = starting_page_index + i;

            let start = if page_index != 0 {
                let prev = pages
                    .get(page_index - 1)
                    .ok_or(Error::ExpectVecToHaveIndex)?;
                prev.start + prev.bytes as u64
            } else {
                HEADER_OFFSET as u64
            };

            pages.checked_push(
                page_index,
                Page::new(start, compressed_len as u32, values_len as u32),
            )?;
        }

        self.base.update_stored_len(stored_len + pushed_len);
        pages.flush()?;

        Ok(true)
    }

    #[inline]
    fn serialize_changes(&self) -> Result<Vec<u8>> {
        self.base.serialize_changes(
            Self::SIZE_OF_T,
            |from, to| self.collect_stored_range(from, to),
            |vals, buf| {
                for v in vals {
                    S::write_to_vec(v, buf);
                }
            },
        )
    }

    #[inline]
    fn db(&self) -> Database {
        self.base.db()
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

impl<I, T, S> WritableVec<I, T> for CompressedVecInner<I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: CompressionStrategy<T>,
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
        if self.base.truncate_pushed(index) {
            self.base.update_stored_len(index);
        }
        Ok(())
    }

    fn reset(&mut self) -> Result<()> {
        self.pages.write().reset();
        self.truncate_if_needed_at(0)?;
        self.base.reset_base()
    }

    fn reset_unsaved(&mut self) {
        self.base.reset_unsaved_base();
    }

    fn is_dirty(&self) -> bool {
        !self.base.pushed().is_empty()
    }

    fn stamped_write_with_changes(&mut self, stamp: Stamp) -> Result<()> {
        if self.base.saved_stamped_changes() == 0 {
            return self.stamped_write(stamp);
        }

        let data = self.serialize_changes()?;
        self.base.save_change_file(stamp, &data)?;
        self.stamped_write(stamp)?;
        self.base.save_prev();

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
    }
}

impl<I, T, S> ReadableVec<I, T> for CompressedVecInner<I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: CompressionStrategy<T>,
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

impl<I, T, S> TypedVec for CompressedVecInner<I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: CompressionStrategy<T>,
{
    type I = I;
    type T = T;
}
