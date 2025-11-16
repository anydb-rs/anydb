use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

use rawdb::{Database, Reader, Region};

use crate::{
    AnyStoredVec, AnyVec, BoxedVecIterator, Compressable, GenericStoredVec, Header, IterableVec,
    Result, TypedVec, VecIndex, Version, variants::ImportOptions,
};

use super::{CompressedVec, RawVec};

mod format;
mod iterator;

pub use format::*;
pub use iterator::*;

/// Enum wrapper for stored vectors, supporting both raw and compressed formats.
///
/// This allows runtime selection between RawVec and CompressedVec storage formats
/// based on the data characteristics and access patterns.
#[derive(Debug, Clone)]
pub enum StoredVec<I, T> {
    Raw(RawVec<I, T>),
    Compressed(CompressedVec<I, T>),
}

impl<I, T> StoredVec<I, T>
where
    I: VecIndex,
    T: Compressable,
{
    pub fn forced_import(
        db: &Database,
        name: &str,
        version: Version,
        format: Format,
    ) -> Result<Self> {
        Self::forced_import_with((db, name, version).into(), format)
    }

    pub fn forced_import_with(options: ImportOptions, format: Format) -> Result<Self> {
        if options.version == Version::ZERO {
            dbg!(options);
            panic!("Version must be at least 1, can't verify endianness otherwise");
        }

        if format.is_compressed() {
            Ok(Self::Compressed(CompressedVec::forced_import_with(
                options,
            )?))
        } else {
            Ok(Self::Raw(RawVec::forced_import_with(options)?))
        }
    }

    /// Removes this vector and all its associated regions from the database
    pub fn remove(self) -> Result<()> {
        match self {
            StoredVec::Raw(v) => v.remove(),
            StoredVec::Compressed(v) => v.remove(),
        }
    }
}

impl<I, T> AnyVec for StoredVec<I, T>
where
    I: VecIndex,
    T: Compressable,
{
    #[inline]
    fn version(&self) -> Version {
        match self {
            StoredVec::Raw(v) => v.version(),
            StoredVec::Compressed(v) => v.version(),
        }
    }

    #[inline]
    fn index_type_to_string(&self) -> &'static str {
        I::to_string()
    }

    #[inline]
    fn len(&self) -> usize {
        self.pushed_len() + self.stored_len()
    }

    fn name(&self) -> &str {
        match self {
            StoredVec::Raw(v) => v.name(),
            StoredVec::Compressed(v) => v.name(),
        }
    }

    #[inline]
    fn value_type_to_size_of(&self) -> usize {
        size_of::<T>()
    }

    #[inline]
    fn region_names(&self) -> Vec<String> {
        match self {
            StoredVec::Raw(v) => v.region_names(),
            StoredVec::Compressed(v) => v.region_names(),
        }
    }
}

impl<I, T> AnyStoredVec for StoredVec<I, T>
where
    I: VecIndex,
    T: Compressable,
{
    #[inline]
    fn db_path(&self) -> PathBuf {
        match self {
            StoredVec::Raw(v) => v.db_path(),
            StoredVec::Compressed(v) => v.db_path(),
        }
    }

    #[inline]
    fn region(&self) -> &Region {
        match self {
            StoredVec::Raw(v) => v.region(),
            StoredVec::Compressed(v) => v.region(),
        }
    }

    #[inline]
    fn db(&self) -> Database {
        match self {
            StoredVec::Raw(v) => v.db(),
            StoredVec::Compressed(v) => v.db(),
        }
    }

    #[inline]
    fn header(&self) -> &Header {
        match self {
            StoredVec::Raw(v) => v.header(),
            StoredVec::Compressed(v) => v.header(),
        }
    }

    #[inline]
    fn mut_header(&mut self) -> &mut Header {
        match self {
            StoredVec::Raw(v) => v.mut_header(),
            StoredVec::Compressed(v) => v.mut_header(),
        }
    }

    #[inline]
    fn saved_stamped_changes(&self) -> u16 {
        match self {
            StoredVec::Raw(v) => v.saved_stamped_changes(),
            StoredVec::Compressed(v) => v.saved_stamped_changes(),
        }
    }

    #[inline]
    fn stored_len(&self) -> usize {
        match self {
            StoredVec::Raw(v) => v.stored_len(),
            StoredVec::Compressed(v) => v.stored_len(),
        }
    }

    #[inline]
    fn real_stored_len(&self) -> usize {
        match self {
            StoredVec::Raw(v) => v.real_stored_len(),
            StoredVec::Compressed(v) => v.real_stored_len(),
        }
    }

    fn write(&mut self) -> Result<()> {
        match self {
            StoredVec::Raw(v) => v.write(),
            StoredVec::Compressed(v) => v.write(),
        }
    }

    fn serialize_changes(&self) -> Result<Vec<u8>> {
        match self {
            StoredVec::Raw(v) => v.serialize_changes(),
            StoredVec::Compressed(v) => v.serialize_changes(),
        }
    }
}

impl<I, T> GenericStoredVec<I, T> for StoredVec<I, T>
where
    I: VecIndex,
    T: Compressable,
{
    #[inline]
    fn unchecked_read_at(&self, index: usize, reader: &Reader) -> Result<T> {
        match self {
            StoredVec::Raw(v) => v.unchecked_read_at(index, reader),
            StoredVec::Compressed(v) => v.unchecked_read_at(index, reader),
        }
    }

    #[inline]
    fn pushed(&self) -> &[T] {
        match self {
            StoredVec::Raw(v) => v.pushed(),
            StoredVec::Compressed(v) => v.pushed(),
        }
    }
    #[inline]
    fn mut_pushed(&mut self) -> &mut Vec<T> {
        match self {
            StoredVec::Raw(v) => v.mut_pushed(),
            StoredVec::Compressed(v) => v.mut_pushed(),
        }
    }
    #[inline]
    fn prev_pushed(&self) -> &[T] {
        match self {
            StoredVec::Raw(v) => v.prev_pushed(),
            StoredVec::Compressed(v) => v.prev_pushed(),
        }
    }
    #[inline]
    fn mut_prev_pushed(&mut self) -> &mut Vec<T> {
        match self {
            StoredVec::Raw(v) => v.mut_prev_pushed(),
            StoredVec::Compressed(v) => v.mut_prev_pushed(),
        }
    }

    #[inline]
    fn holes(&self) -> &BTreeSet<usize> {
        match self {
            StoredVec::Raw(v) => v.holes(),
            StoredVec::Compressed(v) => v.holes(),
        }
    }
    #[inline]
    fn mut_holes(&mut self) -> &mut BTreeSet<usize> {
        match self {
            StoredVec::Raw(v) => v.mut_holes(),
            StoredVec::Compressed(v) => v.mut_holes(),
        }
    }
    #[inline]
    fn prev_holes(&self) -> &BTreeSet<usize> {
        match self {
            StoredVec::Raw(v) => v.prev_holes(),
            StoredVec::Compressed(v) => v.prev_holes(),
        }
    }
    #[inline]
    fn mut_prev_holes(&mut self) -> &mut BTreeSet<usize> {
        match self {
            StoredVec::Raw(v) => v.mut_prev_holes(),
            StoredVec::Compressed(v) => v.mut_prev_holes(),
        }
    }

    #[inline]
    fn updated(&self) -> &BTreeMap<usize, T> {
        match self {
            StoredVec::Raw(v) => v.updated(),
            StoredVec::Compressed(v) => v.updated(),
        }
    }
    #[inline]
    fn mut_updated(&mut self) -> &mut BTreeMap<usize, T> {
        match self {
            StoredVec::Raw(v) => v.mut_updated(),
            StoredVec::Compressed(v) => v.mut_updated(),
        }
    }
    #[inline]
    fn prev_updated(&self) -> &BTreeMap<usize, T> {
        match self {
            StoredVec::Raw(v) => v.prev_updated(),
            StoredVec::Compressed(v) => v.prev_updated(),
        }
    }
    #[inline]
    fn mut_prev_updated(&mut self) -> &mut BTreeMap<usize, T> {
        match self {
            StoredVec::Raw(v) => v.mut_prev_updated(),
            StoredVec::Compressed(v) => v.mut_prev_updated(),
        }
    }

    #[inline]
    #[doc(hidden)]
    fn update_stored_len(&self, val: usize) {
        match self {
            StoredVec::Raw(v) => v.update_stored_len(val),
            StoredVec::Compressed(v) => v.update_stored_len(val),
        }
    }
    fn prev_stored_len(&self) -> usize {
        match self {
            StoredVec::Raw(v) => v.prev_stored_len(),
            StoredVec::Compressed(v) => v.prev_stored_len(),
        }
    }
    fn mut_prev_stored_len(&mut self) -> &mut usize {
        match self {
            StoredVec::Raw(v) => v.mut_prev_stored_len(),
            StoredVec::Compressed(v) => v.mut_prev_stored_len(),
        }
    }

    #[inline]
    fn truncate_if_needed(&mut self, index: I) -> Result<()> {
        match self {
            StoredVec::Raw(v) => v.truncate_if_needed(index),
            StoredVec::Compressed(v) => v.truncate_if_needed(index),
        }
    }

    #[inline]
    fn reset(&mut self) -> Result<()> {
        match self {
            StoredVec::Raw(v) => v.reset(),
            StoredVec::Compressed(v) => v.reset(),
        }
    }
}

impl<'a, I, T> IntoIterator for &'a StoredVec<I, T>
where
    I: VecIndex,
    T: Compressable,
{
    type Item = T;
    type IntoIter = StoredVecIterator<'a, I, T>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            StoredVec::Compressed(v) => StoredVecIterator::Compressed(v.into_iter()),
            StoredVec::Raw(v) => StoredVecIterator::Raw(v.into_iter()),
        }
    }
}

impl<I, T> IterableVec<I, T> for StoredVec<I, T>
where
    I: VecIndex,
    T: Compressable,
{
    fn iter(&self) -> BoxedVecIterator<'_, I, T> {
        Box::new(self.into_iter())
    }
}

impl<I, T> TypedVec for StoredVec<I, T>
where
    I: VecIndex,
    T: Compressable,
{
    type I = I;
    type T = T;
}
