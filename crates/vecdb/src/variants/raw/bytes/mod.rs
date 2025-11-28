use rawdb::Database;

use crate::{BoxedVecIterator, Format, ImportOptions, Importable, Result, VecIndex, Version};

use super::RawVecInner;

mod iterators;
mod strategy;
mod r#trait;
mod value;

pub use iterators::*;
pub use strategy::*;
pub use r#trait::*;
pub use value::*;

/// Raw storage vector using explicit byte serialization in little-endian format.
///
/// Uses the `Bytes` trait to serialize values with `to_bytes()/from_bytes()` in
/// **LITTLE-ENDIAN** format, ensuring **portability across different endianness systems**.
///
/// Like `ZeroCopyVec`, this wraps `RawVecInner` and supports:
/// - Holes (deleted indices)
/// - Updated values (modifications to stored data)
/// - Push/rollback operations
///
/// The only difference from `ZeroCopyVec` is the serialization strategy:
/// - `BytesVec`: Explicit little-endian, portable across architectures
/// - `ZeroCopyVec`: Native byte order, faster but not portable
///
/// Use `BytesVec` when:
/// - Sharing data between systems with different endianness
/// - Cross-platform compatibility is required
/// - Custom serialization logic is needed
///
/// Use `ZeroCopyVec` when:
/// - Maximum performance is critical
/// - Data stays on the same architecture
#[derive(Debug, Clone)]
#[must_use = "Vector should be stored to keep data accessible"]
pub struct BytesVec<I, T>(pub(crate) RawVecInner<I, T, BytesStrategy<T>>);

impl<I, T> Importable for BytesVec<I, T>
where
    I: VecIndex,
    T: BytesVecValue,
{
    fn import(db: &Database, name: &str, version: Version) -> Result<Self> {
        Self::import_with((db, name, version).into())
    }

    fn import_with(options: ImportOptions) -> Result<Self> {
        Ok(Self(RawVecInner::import_with(options, Format::Bytes)?))
    }

    fn forced_import(db: &Database, name: &str, version: Version) -> Result<Self> {
        Self::forced_import_with((db, name, version).into())
    }

    fn forced_import_with(options: ImportOptions) -> Result<Self> {
        Ok(Self(RawVecInner::forced_import_with(
            options,
            Format::Bytes,
        )?))
    }
}

impl<I, T> BytesVec<I, T>
where
    I: VecIndex,
    T: BytesVecValue,
{
    /// The size of T in bytes.
    pub const SIZE_OF_T: usize = size_of::<T>();

    #[inline]
    pub fn iter(&self) -> Result<BytesVecIterator<'_, I, T>> {
        self.0.iter()
    }

    #[inline]
    pub fn clean_iter(&self) -> Result<CleanBytesVecIterator<'_, I, T>> {
        self.0.clean_iter()
    }

    #[inline]
    pub fn dirty_iter(&self) -> Result<DirtyBytesVecIterator<'_, I, T>> {
        self.0.dirty_iter()
    }

    #[inline]
    pub fn boxed_iter(&self) -> Result<BoxedVecIterator<'_, I, T>> {
        self.0.boxed_iter()
    }
}

vecdb_macros::vec_wrapper!(
    BytesVec,
    RawVecInner<I, T, BytesStrategy<T>>,
    BytesVecValue,
    BytesVecIterator
);
