use rawdb::Database;

use crate::{
    BoxedVecIterator, CompressedVecInner, Format, ImportOptions, Importable, Result, VecIndex,
    Version,
};

mod iterators;
mod strategy;
mod value;

pub use iterators::*;
pub use strategy::*;
pub use value::*;

/// Compressed storage using Zstd for maximum general-purpose compression.
///
/// Zstd (Zstandard) provides the best compression ratios among general-purpose
/// algorithms, with good decompression speed. Ideal when storage is expensive.
///
/// # Performance Characteristics
/// - Highest compression ratios (typically 3-5x, better than LZ4)
/// - Fast decompression (slower compression than LZ4)
/// - Works well with any data type
///
/// # When to Use
/// - Storage space is expensive
/// - Can tolerate slower compression (decompression is fast)
#[derive(Debug, Clone)]
#[must_use = "Vector should be stored to keep data accessible"]
pub struct ZstdVec<I, T>(CompressedVecInner<I, T, ZstdStrategy<T>>);

impl<I, T> Importable for ZstdVec<I, T>
where
    I: VecIndex,
    T: ZstdVecValue,
{
    fn import(db: &Database, name: &str, version: Version) -> Result<Self> {
        Self::import_with((db, name, version).into())
    }

    fn import_with(options: ImportOptions) -> Result<Self> {
        Ok(Self(CompressedVecInner::import_with(
            options,
            Format::Zstd,
        )?))
    }

    fn forced_import(db: &Database, name: &str, version: Version) -> Result<Self> {
        Self::forced_import_with((db, name, version).into())
    }

    fn forced_import_with(options: ImportOptions) -> Result<Self> {
        Ok(Self(CompressedVecInner::forced_import_with(
            options,
            Format::Zstd,
        )?))
    }
}

impl<I, T> ZstdVec<I, T>
where
    I: VecIndex,
    T: ZstdVecValue,
{
    #[inline]
    pub fn iter(&self) -> Result<ZstdVecIterator<'_, I, T>> {
        self.0.iter()
    }

    #[inline]
    pub fn clean_iter(&self) -> Result<CleanZstdVecIterator<'_, I, T>> {
        self.0.clean_iter()
    }

    #[inline]
    pub fn dirty_iter(&self) -> Result<DirtyZstdVecIterator<'_, I, T>> {
        self.0.dirty_iter()
    }

    #[inline]
    pub fn boxed_iter(&self) -> Result<BoxedVecIterator<'_, I, T>> {
        self.0.boxed_iter()
    }

    /// Removes this vector and all its associated regions from the database
    pub fn remove(self) -> Result<()> {
        self.0.remove()
    }
}

vecdb_macros::vec_wrapper!(
    ZstdVec,
    CompressedVecInner<I, T, ZstdStrategy<T>>,
    ZstdVecValue,
    ZstdVecIterator
);
