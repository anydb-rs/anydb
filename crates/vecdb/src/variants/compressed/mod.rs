use std::{
    collections::{BTreeMap, BTreeSet},
    mem,
    path::PathBuf,
    sync::Arc,
};

use log::info;
use parking_lot::RwLock;
use rawdb::{Database, Reader, Region};

use crate::{
    AnyStoredVec, AnyVec, AsInnerSlice, BoxedVecIterator, Compressable, Error, Format,
    FromInnerSlice, GenericStoredVec, HEADER_OFFSET, Header, IterableVec, RawVec, Result, TypedVec,
    VecIndex, Version, likely, unlikely, variants::ImportOptions,
};

mod iterators;
mod page;
mod pages;

pub use iterators::*;
use page::*;
use pages::*;

const PCO_COMPRESSION_LEVEL: usize = 4;
/// Maximum size in bytes of a single compressed (pco) page
pub(crate) const MAX_UNCOMPRESSED_PAGE_SIZE: usize = 16 * 1024; // 16 KiB

const VERSION: Version = Version::TWO;

/// Compressed storage vector using Pcodec for lossless numerical compression.
///
/// Values are compressed in pages for better space efficiency. Best for sequential
/// access patterns of numerical data. Random access is possible but less efficient
/// than RawVec - prefer the latter for random access workloads.
#[derive(Debug)]
pub struct CompressedVec<I, T> {
    inner: RawVec<I, T>,
    pages: Arc<RwLock<Pages>>,
}

impl<I, T> CompressedVec<I, T>
where
    I: VecIndex,
    T: Compressable,
{
    const PER_PAGE: usize = MAX_UNCOMPRESSED_PAGE_SIZE / Self::SIZE_OF_T;

    /// Same as import but will reset the vec under certain errors, so be careful !
    pub fn forced_import(db: &Database, name: &str, version: Version) -> Result<Self> {
        Self::forced_import_with((db, name, version).into())
    }

    /// Same as import but will reset the vec under certain errors, so be careful !
    pub fn forced_import_with(mut options: ImportOptions) -> Result<Self> {
        options.version = options.version + VERSION;
        let res = Self::import_with(options);
        match res {
            Err(Error::DifferentCompressionMode)
            | Err(Error::WrongEndian)
            | Err(Error::WrongLength)
            | Err(Error::DifferentVersion { .. }) => {
                info!("Resetting {}...", options.name);
                options
                    .db
                    .remove_region_if_exists(&Self::vec_region_name_with(options.name))?;
                options
                    .db
                    .remove_region_if_exists(&Self::holes_region_name_with(options.name))?;
                options
                    .db
                    .remove_region_if_exists(&Self::pages_region_name_(options.name))?;
                Self::import_with(options)
            }
            _ => res,
        }
    }

    pub fn import(db: &Database, name: &str, version: Version) -> Result<Self> {
        Self::import_with((db, name, version).into())
    }

    #[inline]
    pub fn import_with(options: ImportOptions) -> Result<Self> {
        let inner = RawVec::import_(options, Format::Compressed)?;

        let pages = Pages::import(options.db, &Self::pages_region_name_(options.name))?;

        let this = Self {
            inner,
            pages: Arc::new(RwLock::new(pages)),
        };

        this.update_stored_len(this.real_stored_len());

        Ok(this)
    }

    #[inline]
    fn decode_page(&self, page_index: usize, reader: &Reader) -> Result<Vec<T>> {
        Self::decode_page_(self.stored_len(), page_index, reader, &self.pages.read())
    }

    #[inline]
    fn decode_page_(
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
            });
        } else if unlikely(page_index >= pages.len()) {
            return Err(Error::ExpectVecToHaveIndex);
        }

        let page = pages.get(page_index).unwrap();
        let len = page.bytes as usize;
        let offset = page.start as usize;

        let compressed_data = reader.unchecked_read(offset, len);
        Self::decompress_bytes(compressed_data, page.values as usize)
    }

    /// Stateless: decompress raw bytes into Vec<T>
    #[inline]
    fn decompress_bytes(compressed_data: &[u8], expected_values: usize) -> Result<Vec<T>> {
        let vec: Vec<T::NumberType> = pco::standalone::simple_decompress(compressed_data)?;
        let vec = T::from_inner_slice(vec);

        if likely(vec.len() == expected_values) {
            return Ok(vec);
        }

        dbg!((compressed_data.len(), vec.len(), expected_values));
        dbg!(&vec);
        unreachable!("Decompressed page has wrong number of values")
    }

    #[inline]
    fn compress_page(chunk: &[T]) -> Vec<u8> {
        if chunk.len() > Self::PER_PAGE {
            panic!();
        }

        pco::standalone::simpler_compress(chunk.as_inner_slice(), PCO_COMPRESSION_LEVEL).unwrap()
    }

    #[inline]
    fn index_to_page_index(index: usize) -> usize {
        index / Self::PER_PAGE
    }

    #[inline]
    fn page_index_to_index(page_index: usize) -> usize {
        page_index * Self::PER_PAGE
    }

    #[inline]
    pub fn iter(&self) -> Result<CompressedVecIterator<'_, I, T>> {
        CompressedVecIterator::new(self)
    }

    #[inline]
    pub fn clean_iter(&self) -> Result<CleanCompressedVecIterator<'_, I, T>> {
        CleanCompressedVecIterator::new(self)
    }

    #[inline]
    pub fn dirty_iter(&self) -> Result<DirtyCompressedVecIterator<'_, I, T>> {
        DirtyCompressedVecIterator::new(self)
    }

    #[inline]
    pub fn boxed_iter(&self) -> Result<BoxedVecIterator<'_, I, T>> {
        Ok(Box::new(CompressedVecIterator::new(self)?))
    }

    fn pages_region_name(&self) -> String {
        Self::pages_region_name_(self.name())
    }
    fn pages_region_name_(name: &str) -> String {
        format!("{}_pages", Self::vec_region_name_with(name))
    }

    #[inline]
    pub fn is_dirty(&self) -> bool {
        !self.is_pushed_empty()
    }

    /// Removes this vector and all its associated regions from the database
    pub fn remove(self) -> Result<()> {
        // Remove main region (through inner RawVec)
        self.inner.remove()?;

        // Remove pages region
        let pages = Arc::try_unwrap(self.pages)
            .map_err(|_| Error::Str("Cannot remove CompressedVec: pages still referenced"))?;
        pages.into_inner().remove()?;

        Ok(())
    }
}

impl<I, T> Clone for CompressedVec<I, T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            pages: self.pages.clone(),
        }
    }
}

impl<I, T> AnyVec for CompressedVec<I, T>
where
    I: VecIndex,
    T: Compressable,
{
    #[inline]
    fn version(&self) -> Version {
        self.inner.version()
    }

    #[inline]
    fn name(&self) -> &str {
        self.inner.name()
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
    fn region_names(&self) -> Vec<String> {
        let mut v = self.inner.region_names();
        v.push(self.pages_region_name());
        v
    }
}

impl<I, T> AnyStoredVec for CompressedVec<I, T>
where
    I: VecIndex,
    T: Compressable,
{
    #[inline]
    fn db_path(&self) -> PathBuf {
        self.inner.db_path()
    }

    #[inline]
    fn region(&self) -> &Region {
        self.inner.region()
    }

    #[inline]
    fn header(&self) -> &Header {
        self.inner.header()
    }

    #[inline]
    fn mut_header(&mut self) -> &mut Header {
        self.inner.mut_header()
    }

    #[inline]
    fn saved_stamped_changes(&self) -> u16 {
        self.inner.saved_stamped_changes()
    }

    #[inline]
    fn stored_len(&self) -> usize {
        self.inner.stored_len()
    }

    #[inline]
    fn real_stored_len(&self) -> usize {
        self.pages.read().stored_len(Self::PER_PAGE)
    }

    fn write(&mut self) -> Result<()> {
        self.inner.write_header_if_needed()?;

        let stored_len = self.stored_len();
        let pushed_len = self.pushed_len();
        let real_stored_len = self.real_stored_len();
        assert!(stored_len <= real_stored_len);
        let truncated = stored_len != real_stored_len;
        let has_new_data = pushed_len != 0;

        if !has_new_data && !truncated {
            // info!("Nothing to push {}", self.region_index());
            return Ok(());
        }

        let mut pages = self.pages.write();
        let pages_len = pages.len();
        let starting_page_index = Self::index_to_page_index(stored_len);
        assert!(starting_page_index <= pages_len);

        let mut values = vec![];

        let offset = HEADER_OFFSET as u64;

        let truncate_at = if starting_page_index < pages_len {
            let len = stored_len % Self::PER_PAGE;

            if len != 0 {
                let mut page_values = Self::decode_page_(
                    stored_len,
                    starting_page_index,
                    &self.create_static_reader(),
                    &pages,
                )?;
                page_values.truncate(len);
                values = page_values;
            }

            pages.truncate(starting_page_index).unwrap().start
        } else {
            pages
                .last()
                .map_or(offset, |page| page.start + page.bytes as u64)
        };

        values.append(&mut mem::take(self.inner.mut_pushed()));

        let compressed = values
            .chunks(Self::PER_PAGE)
            .map(|chunk| (Self::compress_page(chunk), chunk.len()))
            .collect::<Vec<_>>();

        compressed.iter().enumerate().for_each(|(i, (bytes, len))| {
            let page_index = starting_page_index + i;

            let start = if page_index != 0 {
                let prev = pages.get(page_index - 1).unwrap();
                prev.start + prev.bytes as u64
            } else {
                offset
            };

            let page = Page::new(start, bytes.len() as u32, *len as u32);

            pages.checked_push(page_index, page);
        });

        let buf = compressed
            .into_iter()
            .flat_map(|(v, _)| v)
            .collect::<Vec<_>>();

        self.region().truncate_write(truncate_at as usize, &buf)?;

        self.update_stored_len(stored_len + pushed_len);

        pages.flush()?;

        Ok(())
    }

    #[inline]
    fn serialize_changes(&self) -> Result<Vec<u8>> {
        self.inner.serialize_changes()
    }

    #[inline]
    fn db(&self) -> Database {
        self.inner.db()
    }
}

impl<I, T> GenericStoredVec<I, T> for CompressedVec<I, T>
where
    I: VecIndex,
    T: Compressable,
{
    #[inline]
    fn unchecked_read_at(&self, index: usize, reader: &Reader) -> Result<T> {
        let page_index = Self::index_to_page_index(index);
        let decoded_index = index % Self::PER_PAGE;
        Ok(unsafe {
            *self
                .decode_page(page_index, reader)?
                .get_unchecked(decoded_index)
        })
    }

    #[inline]
    fn pushed(&self) -> &[T] {
        self.inner.pushed()
    }
    #[inline]
    fn mut_pushed(&mut self) -> &mut Vec<T> {
        self.inner.mut_pushed()
    }
    #[inline]
    fn prev_pushed(&self) -> &[T] {
        self.inner.prev_pushed()
    }
    #[inline]
    fn mut_prev_pushed(&mut self) -> &mut Vec<T> {
        self.inner.mut_prev_pushed()
    }
    #[inline]
    fn holes(&self) -> &BTreeSet<usize> {
        self.inner.holes()
    }
    #[inline]
    fn mut_holes(&mut self) -> &mut BTreeSet<usize> {
        panic!("unsupported for now")
    }
    #[inline]
    fn prev_holes(&self) -> &BTreeSet<usize> {
        self.inner.prev_holes()
    }
    #[inline]
    fn mut_prev_holes(&mut self) -> &mut BTreeSet<usize> {
        panic!("unsupported for now")
    }
    #[inline]
    fn updated(&self) -> &BTreeMap<usize, T> {
        self.inner.updated()
    }
    #[inline]
    fn mut_updated(&mut self) -> &mut BTreeMap<usize, T> {
        panic!("unsupported for now")
    }
    #[inline]
    fn prev_updated(&self) -> &BTreeMap<usize, T> {
        self.inner.prev_updated()
    }
    #[inline]
    fn mut_prev_updated(&mut self) -> &mut BTreeMap<usize, T> {
        panic!("unsupported for now")
    }

    #[inline]
    #[doc(hidden)]
    fn update_stored_len(&self, val: usize) {
        self.inner.update_stored_len(val);
    }
    #[inline]
    fn prev_stored_len(&self) -> usize {
        self.inner.prev_stored_len()
    }
    #[inline]
    fn mut_prev_stored_len(&mut self) -> &mut usize {
        self.inner.mut_prev_stored_len()
    }

    fn reset(&mut self) -> Result<()> {
        self.pages.write().reset();
        self.clear()
    }
}

impl<'a, I, T> IntoIterator for &'a CompressedVec<I, T>
where
    I: VecIndex,
    T: Compressable,
{
    type Item = T;
    type IntoIter = CompressedVecIterator<'a, I, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter().expect("CompressedVecIter::new(self) to work")
    }
}

impl<I, T> IterableVec<I, T> for CompressedVec<I, T>
where
    I: VecIndex,
    T: Compressable,
{
    fn iter(&self) -> BoxedVecIterator<'_, I, T> {
        Box::new(self.into_iter())
    }
}

impl<I, T> TypedVec for CompressedVec<I, T>
where
    I: VecIndex,
    T: Compressable,
{
    type I = I;
    type T = T;
}
