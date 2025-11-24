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

/// Compressed storage vector using Zstd for high compression ratios.
///
/// Zstd offers excellent compression ratios with good decompression speed.
/// Best for scenarios where storage space is more important than compression speed.
#[derive(Debug, Clone)]
#[must_use = "Vector should be stored to keep data accessible"]
pub struct ZstdVec<I, T>(CompressedVecInner<I, T, ZstdStrategy<T>>);

impl<I, T> Deref for ZstdVec<I, T> {
    type Target = CompressedVecInner<I, T, ZstdStrategy<T>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<I, T> DerefMut for ZstdVec<I, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<I, T> ZstdVec<I, T>
where
    I: VecIndex,
    T: ZstdVecValue,
{
    /// Same as import but will reset the vec under certain errors, so be careful !
    pub fn forced_import(db: &Database, name: &str, version: Version) -> Result<Self> {
        Self::forced_import_with((db, name, version, Format::Zstd).into())
    }

    /// Same as import but will reset the vec under certain errors, so be careful !
    pub fn forced_import_with(mut options: ImportOptions) -> Result<Self> {
        options.format = Format::Zstd;
        Ok(Self(CompressedVecInner::forced_import_with(options)?))
    }

    pub fn import(db: &Database, name: &str, version: Version) -> Result<Self> {
        Self::import_with((db, name, version, Format::Zstd).into())
    }

    #[inline]
    pub fn import_with(mut options: ImportOptions) -> Result<Self> {
        options.format = Format::Zstd;
        Ok(Self(CompressedVecInner::import_with(options)?))
    }

    #[inline]
    pub fn iter(&self) -> Result<ZstdVecIterator<'_, I, T>> {
        ZstdVecIterator::new(&self.0)
    }

    #[inline]
    pub fn clean_iter(&self) -> Result<CleanZstdVecIterator<'_, I, T>> {
        CleanZstdVecIterator::new(&self.0)
    }

    #[inline]
    pub fn dirty_iter(&self) -> Result<DirtyZstdVecIterator<'_, I, T>> {
        DirtyZstdVecIterator::new(&self.0)
    }

    #[inline]
    pub fn boxed_iter(&self) -> Result<BoxedVecIterator<'_, I, T>> {
        Ok(Box::new(ZstdVecIterator::new(&self.0)?))
    }

    /// Removes this vector and all its associated regions from the database
    pub fn remove(self) -> Result<()> {
        self.0.remove()
    }
}

impl<'a, I, T> IntoIterator for &'a ZstdVec<I, T>
where
    I: VecIndex,
    T: ZstdVecValue,
{
    type Item = T;
    type IntoIter = ZstdVecIterator<'a, I, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter().expect("ZstdVecIter::new(self) to work")
    }
}

impl<I, T> AnyVec for ZstdVec<I, T>
where
    I: VecIndex,
    T: ZstdVecValue,
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

impl<I, T> TypedVec for ZstdVec<I, T>
where
    I: VecIndex,
    T: ZstdVecValue,
{
    type I = I;
    type T = T;
}

impl<I, T> IterableVec<I, T> for ZstdVec<I, T>
where
    I: VecIndex,
    T: ZstdVecValue,
{
    fn iter(&self) -> BoxedVecIterator<'_, I, T> {
        Box::new(self.into_iter())
    }
}
