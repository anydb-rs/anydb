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

/// Compressed storage using LZ4 for speed-optimized general-purpose compression.
///
/// LZ4 prioritizes compression/decompression speed over ratio, making it ideal
/// for workloads where CPU time matters more than storage space.
///
/// # Performance Characteristics
/// - Extremely fast compression/decompression (hundreds of MB/s)
/// - Moderate compression ratios (typically 2-3x)
/// - Works well with any data type
///
/// # When to Use
/// - Speed is more important than storage savings
/// - Mixed data types (not just numbers)
/// - Need compression but can't afford CPU overhead
#[derive(Debug, Clone)]
#[must_use = "Vector should be stored to keep data accessible"]
pub struct LZ4Vec<I, T>(CompressedVecInner<I, T, LZ4Strategy<T>>);

impl<I, T> Importable for LZ4Vec<I, T>
where
    I: VecIndex,
    T: LZ4VecValue,
{
    fn import(db: &Database, name: &str, version: Version) -> Result<Self> {
        Self::import_with((db, name, version).into())
    }

    fn import_with(options: ImportOptions) -> Result<Self> {
        Ok(Self(CompressedVecInner::import_with(options, Format::LZ4)?))
    }

    fn forced_import(db: &Database, name: &str, version: Version) -> Result<Self> {
        Self::forced_import_with((db, name, version).into())
    }

    fn forced_import_with(options: ImportOptions) -> Result<Self> {
        Ok(Self(CompressedVecInner::forced_import_with(
            options,
            Format::LZ4,
        )?))
    }
}

impl<I, T> LZ4Vec<I, T>
where
    I: VecIndex,
    T: LZ4VecValue,
{
    #[inline]
    pub fn iter(&self) -> Result<LZ4VecIterator<'_, I, T>> {
        self.0.iter()
    }

    #[inline]
    pub fn clean_iter(&self) -> Result<CleanLZ4VecIterator<'_, I, T>> {
        self.0.clean_iter()
    }

    #[inline]
    pub fn dirty_iter(&self) -> Result<DirtyLZ4VecIterator<'_, I, T>> {
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
    LZ4Vec,
    CompressedVecInner<I, T, LZ4Strategy<T>>,
    LZ4VecValue,
    LZ4VecIterator
);
