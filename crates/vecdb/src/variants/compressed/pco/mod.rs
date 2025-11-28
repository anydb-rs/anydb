use rawdb::Database;

use crate::{
    BoxedVecIterator, CompressedVecInner, Format, ImportOptions, Importable, Result, VecIndex,
    Version,
};

mod iterators;
mod strategy;
mod r#trait;
mod value;

pub use iterators::*;
pub use strategy::*;
pub use r#trait::*;
pub use value::*;

/// Compressed storage using Pcodec for optimal numeric data compression.
///
/// Pcodec (Pco) provides the best compression ratios for numerical sequences
/// through specialized quantization and encoding. Ideal for time-series, scientific
/// data, and any workload dominated by numeric values.
///
/// # Performance Characteristics
/// - Best-in-class compression for numeric types (often 2-10x better than LZ4/Zstd)
/// - Sequential access optimized (values compressed in pages)
/// - Random access possible but slower than raw formats
///
/// # When to Use
/// - Numeric data dominates (integers, floats)
/// - Storage space is critical
/// - Sequential access patterns are common
#[derive(Debug, Clone)]
#[must_use = "Vector should be stored to keep data accessible"]
pub struct PcoVec<I, T>(CompressedVecInner<I, T, PcodecStrategy<T>>);

impl<I, T> Importable for PcoVec<I, T>
where
    I: VecIndex,
    T: PcoVecValue,
{
    fn import(db: &Database, name: &str, version: Version) -> Result<Self> {
        Self::import_with((db, name, version).into())
    }

    fn import_with(options: ImportOptions) -> Result<Self> {
        Ok(Self(CompressedVecInner::import_with(options, Format::Pco)?))
    }

    fn forced_import(db: &Database, name: &str, version: Version) -> Result<Self> {
        Self::forced_import_with((db, name, version).into())
    }

    fn forced_import_with(options: ImportOptions) -> Result<Self> {
        Ok(Self(CompressedVecInner::forced_import_with(
            options,
            Format::Pco,
        )?))
    }
}

impl<I, T> PcoVec<I, T>
where
    I: VecIndex,
    T: PcoVecValue,
{
    #[inline]
    pub fn iter(&self) -> Result<PcodecVecIterator<'_, I, T>> {
        self.0.iter()
    }

    #[inline]
    pub fn clean_iter(&self) -> Result<CleanPcodecVecIterator<'_, I, T>> {
        self.0.clean_iter()
    }

    #[inline]
    pub fn dirty_iter(&self) -> Result<DirtyPcodecVecIterator<'_, I, T>> {
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
    PcoVec,
    CompressedVecInner<I, T, PcodecStrategy<T>>,
    PcoVecValue,
    PcodecVecIterator
);
