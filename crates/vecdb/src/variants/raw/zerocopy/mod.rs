use std::ops::{Deref, DerefMut};

use rawdb::Reader;

use crate::{
    AnyStoredVec, AnyVec, BoxedVecIterator, HEADER_OFFSET, IterableVec, Result, TypedVec, VecIndex,
    Version,
};

use super::{CleanRawVecIterator, DirtyRawVecIterator, RawVecInner, RawVecIterator};

mod iterators;
mod strategy;
mod value;

pub use iterators::*;
pub use strategy::*;
pub use value::*;

/// Raw storage vector that stores values as-is without compression using zerocopy.
///
/// This is the most basic storage format, writing values directly to disk
/// with minimal overhead. Uses zerocopy for direct memory mapping without copying.
/// Ideal for random access patterns and data that doesn't compress well.
#[derive(Debug, Clone)]
#[must_use = "Vector should be stored to keep data accessible"]
pub struct ZeroCopyVec<I, T>(pub(crate) RawVecInner<I, T, ZeroCopyStrategy<T>>);

impl<I, T> ZeroCopyVec<I, T>
where
    I: VecIndex,
    T: ZeroCopyVecValue,
{
    /// The size of T in bytes.
    pub const SIZE_OF_T: usize = size_of::<T>();

    #[inline]
    pub fn iter(&self) -> Result<ZeroCopyVecIterator<'_, I, T>> {
        RawVecIterator::new(&self.0)
    }

    #[inline]
    pub fn clean_iter(&self) -> Result<CleanZeroCopyVecIterator<'_, I, T>> {
        CleanRawVecIterator::new(&self.0)
    }

    #[inline]
    pub fn dirty_iter(&self) -> Result<DirtyZeroCopyVecIterator<'_, I, T>> {
        DirtyRawVecIterator::new(&self.0)
    }

    #[inline]
    pub fn boxed_iter(&self) -> Result<BoxedVecIterator<'_, I, T>> {
        Ok(Box::new(self.iter()?))
    }

    // ============================================================================
    // Zerocopy-specific read methods (return references directly from mmap)
    // ============================================================================

    /// Returns a reference to the value at the given index directly from the memory-mapped file.
    /// This avoids copying the value and is very efficient for large types.
    ///
    /// Note: This only works for stored (not pushed/updated) values.
    /// Returns None if the index is in holes, beyond stored length, or in updated layer.
    #[inline]
    pub fn read_ref<'a>(&self, index: I, reader: &'a Reader) -> Option<&'a T> {
        self.read_ref_at(index.to_usize(), reader)
    }

    /// Returns a reference to the value at the given usize index directly from the memory-mapped file.
    #[inline]
    pub fn read_ref_at<'a>(&self, index: usize, reader: &'a Reader) -> Option<&'a T> {
        // Cannot return ref for holes
        if !self.holes().is_empty() && self.holes().contains(&index) {
            return None;
        }

        let stored_len = self.stored_len();

        // Cannot return ref for pushed values (they're in a Vec, not mmap)
        if index >= stored_len {
            return None;
        }

        // Cannot return ref for updated values (they're in a BTreeMap, not mmap)
        if !self.updated().is_empty() && self.updated().contains_key(&index) {
            return None;
        }

        self.unchecked_read_ref_at(index, reader)
    }

    /// Returns a reference without bounds checking.
    /// Safety: Caller must ensure index is within stored bounds and not a hole or updated.
    #[inline]
    pub fn unchecked_read_ref_at<'a>(&self, index: usize, reader: &'a Reader) -> Option<&'a T> {
        let offset = (index * Self::SIZE_OF_T) + HEADER_OFFSET;
        let bytes = reader.prefixed(offset);
        T::ref_from_prefix(bytes).map(|(v, _)| v).ok()
    }
}

impl<I, T> Deref for ZeroCopyVec<I, T> {
    type Target = RawVecInner<I, T, ZeroCopyStrategy<T>>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<I, T> DerefMut for ZeroCopyVec<I, T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<'a, I, T> IntoIterator for &'a ZeroCopyVec<I, T>
where
    I: VecIndex,
    T: ZeroCopyVecValue,
{
    type Item = T;
    type IntoIter = ZeroCopyVecIterator<'a, I, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter().expect("ZeroCopyVecIter::new(self) to work")
    }
}

impl<I, T> IterableVec<I, T> for ZeroCopyVec<I, T>
where
    I: VecIndex,
    T: ZeroCopyVecValue,
{
    fn iter(&self) -> BoxedVecIterator<'_, I, T> {
        Box::new(self.into_iter())
    }
}

impl<I, T> AnyVec for ZeroCopyVec<I, T>
where
    I: VecIndex,
    T: ZeroCopyVecValue,
{
    #[inline]
    fn version(&self) -> Version {
        self.0.version()
    }

    #[inline]
    fn name(&self) -> &str {
        self.0.name()
    }

    #[inline]
    fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    fn index_type_to_string(&self) -> &'static str {
        self.0.index_type_to_string()
    }

    #[inline]
    fn value_type_to_size_of(&self) -> usize {
        self.0.value_type_to_size_of()
    }

    #[inline]
    fn region_names(&self) -> Vec<String> {
        self.0.region_names()
    }
}

impl<I, T> TypedVec for ZeroCopyVec<I, T>
where
    I: VecIndex,
    T: ZeroCopyVecValue,
{
    type I = I;
    type T = T;
}
