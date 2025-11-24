mod iterators;
mod strategy;
mod r#trait;
mod value;

use std::ops::{Deref, DerefMut};

pub use iterators::*;
pub use strategy::*;
pub use r#trait::*;
pub use value::*;

use crate::{AnyVec, BoxedVecIterator, IterableVec, Result, TypedVec, VecIndex, Version};

use super::{CleanRawVecIterator, DirtyRawVecIterator, RawVecInner, RawVecIterator};

/// Raw storage vector that stores values using custom Bytes serialization.
///
/// This is similar to ZeroCopyVec but uses the Bytes trait for serialization
/// instead of zerocopy. Useful for types that need custom serialization logic
/// but still want efficient raw storage.
#[derive(Debug, Clone)]
#[must_use = "Vector should be stored to keep data accessible"]
pub struct BytesVec<I, T>(pub(crate) RawVecInner<I, T, BytesStrategy<T>>);

impl<I, T> BytesVec<I, T>
where
    I: VecIndex,
    T: BytesVecValue,
{
    /// The size of T in bytes.
    pub const SIZE_OF_T: usize = T::SIZE;

    #[inline]
    pub fn iter(&self) -> Result<BytesVecIterator<'_, I, T>> {
        RawVecIterator::new(&self.0)
    }

    #[inline]
    pub fn clean_iter(&self) -> Result<CleanBytesVecIterator<'_, I, T>> {
        CleanRawVecIterator::new(&self.0)
    }

    #[inline]
    pub fn dirty_iter(&self) -> Result<DirtyBytesVecIterator<'_, I, T>> {
        DirtyRawVecIterator::new(&self.0)
    }

    #[inline]
    pub fn boxed_iter(&self) -> Result<BoxedVecIterator<'_, I, T>> {
        Ok(Box::new(self.iter()?))
    }
}

impl<I, T> Deref for BytesVec<I, T> {
    type Target = RawVecInner<I, T, BytesStrategy<T>>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<I, T> DerefMut for BytesVec<I, T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<'a, I, T> IntoIterator for &'a BytesVec<I, T>
where
    I: VecIndex,
    T: BytesVecValue,
{
    type Item = T;
    type IntoIter = BytesVecIterator<'a, I, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter().expect("BytesVecIter::new(self) to work")
    }
}

impl<I, T> IterableVec<I, T> for BytesVec<I, T>
where
    I: VecIndex,
    T: BytesVecValue,
{
    fn iter(&self) -> BoxedVecIterator<'_, I, T> {
        Box::new(self.into_iter())
    }
}

impl<I, T> AnyVec for BytesVec<I, T>
where
    I: VecIndex,
    T: BytesVecValue,
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

impl<I, T> TypedVec for BytesVec<I, T>
where
    I: VecIndex,
    T: BytesVecValue,
{
    type I = I;
    type T = T;
}
