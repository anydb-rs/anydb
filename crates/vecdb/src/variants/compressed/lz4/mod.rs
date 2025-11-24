use std::ops::{Deref, DerefMut};

use rawdb::Database;

use crate::{
    AnyVec, BoxedVecIterator, Format, IterableVec, Result, TypedVec, VecIndex, Version,
    variants::ImportOptions,
};

use super::CompressedVecInner;

mod iterators;
mod strategy;
mod value;

pub use iterators::*;
pub use strategy::*;
pub use value::*;

/// Compressed storage vector using LZ4 for fast compression/decompression.
///
/// LZ4 offers very fast compression and decompression speeds with moderate
/// compression ratios. Best for scenarios where speed is more important than
/// compression ratio.
#[derive(Debug, Clone)]
#[must_use = "Vector should be stored to keep data accessible"]
pub struct LZ4Vec<I, T>(CompressedVecInner<I, T, LZ4Strategy<T>>);

impl<I, T> Deref for LZ4Vec<I, T> {
    type Target = CompressedVecInner<I, T, LZ4Strategy<T>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<I, T> DerefMut for LZ4Vec<I, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<I, T> LZ4Vec<I, T>
where
    I: VecIndex,
    T: LZ4VecValue,
{
    /// Same as import but will reset the vec under certain errors, so be careful !
    pub fn forced_import(db: &Database, name: &str, version: Version) -> Result<Self> {
        Self::forced_import_with((db, name, version, Format::LZ4).into())
    }

    /// Same as import but will reset the vec under certain errors, so be careful !
    pub fn forced_import_with(mut options: ImportOptions) -> Result<Self> {
        options.format = Format::LZ4;
        Ok(Self(CompressedVecInner::forced_import_with(options)?))
    }

    pub fn import(db: &Database, name: &str, version: Version) -> Result<Self> {
        Self::import_with((db, name, version, Format::LZ4).into())
    }

    #[inline]
    pub fn import_with(mut options: ImportOptions) -> Result<Self> {
        options.format = Format::LZ4;
        Ok(Self(CompressedVecInner::import_with(options)?))
    }

    #[inline]
    pub fn iter(&self) -> Result<LZ4VecIterator<'_, I, T>> {
        LZ4VecIterator::new(&self.0)
    }

    #[inline]
    pub fn clean_iter(&self) -> Result<CleanLZ4VecIterator<'_, I, T>> {
        CleanLZ4VecIterator::new(&self.0)
    }

    #[inline]
    pub fn dirty_iter(&self) -> Result<DirtyLZ4VecIterator<'_, I, T>> {
        DirtyLZ4VecIterator::new(&self.0)
    }

    #[inline]
    pub fn boxed_iter(&self) -> Result<BoxedVecIterator<'_, I, T>> {
        Ok(Box::new(LZ4VecIterator::new(&self.0)?))
    }

    /// Removes this vector and all its associated regions from the database
    pub fn remove(self) -> Result<()> {
        self.0.remove()
    }
}

impl<'a, I, T> IntoIterator for &'a LZ4Vec<I, T>
where
    I: VecIndex,
    T: LZ4VecValue,
{
    type Item = T;
    type IntoIter = LZ4VecIterator<'a, I, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter().expect("LZ4VecIter::new(self) to work")
    }
}

impl<I, T> AnyVec for LZ4Vec<I, T>
where
    I: VecIndex,
    T: LZ4VecValue,
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

impl<I, T> TypedVec for LZ4Vec<I, T>
where
    I: VecIndex,
    T: LZ4VecValue,
{
    type I = I;
    type T = T;
}

impl<I, T> IterableVec<I, T> for LZ4Vec<I, T>
where
    I: VecIndex,
    T: LZ4VecValue,
{
    fn iter(&self) -> BoxedVecIterator<'_, I, T> {
        Box::new(self.into_iter())
    }
}
