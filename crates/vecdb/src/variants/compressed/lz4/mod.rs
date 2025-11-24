use std::{
    ops::{Deref, DerefMut},
    path::PathBuf,
};

use rawdb::{Database, Region};

use crate::{
    AnyStoredVec, AnyVec, BoxedVecIterator, Format, GenericStoredVec, Header, ImportOptions,
    Importable, IterableVec, Result, TypedVec, VecIndex, Version,
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

impl<I, T> AnyStoredVec for LZ4Vec<I, T>
where
    I: VecIndex,
    T: LZ4VecValue,
{
    #[inline]
    fn db_path(&self) -> PathBuf {
        self.0.db_path()
    }

    #[inline]
    fn region(&self) -> &Region {
        self.0.region()
    }

    #[inline]
    fn header(&self) -> &Header {
        self.0.header()
    }

    #[inline]
    fn mut_header(&mut self) -> &mut Header {
        self.0.mut_header()
    }

    #[inline]
    fn saved_stamped_changes(&self) -> u16 {
        self.0.saved_stamped_changes()
    }

    #[inline]
    fn db(&self) -> Database {
        self.0.db()
    }

    #[inline]
    fn real_stored_len(&self) -> usize {
        self.0.real_stored_len()
    }

    #[inline]
    fn stored_len(&self) -> usize {
        self.0.stored_len()
    }

    #[inline]
    fn write(&mut self) -> Result<()> {
        self.0.write()
    }

    #[inline]
    fn serialize_changes(&self) -> Result<Vec<u8>> {
        self.0.serialize_changes()
    }

    fn remove(self) -> Result<()> {
        self.0.remove()
    }
}

impl<I, T> GenericStoredVec<I, T> for LZ4Vec<I, T>
where
    I: VecIndex,
    T: LZ4VecValue,
{
    #[inline]
    fn unchecked_read_at(&self, index: usize, reader: &rawdb::Reader) -> Result<T> {
        self.0.unchecked_read_at(index, reader)
    }

    #[inline(always)]
    fn read_value_from_bytes(&self, bytes: &[u8]) -> Result<T> {
        self.0.read_value_from_bytes(bytes)
    }

    #[inline]
    fn value_to_bytes(&self, value: &T) -> Vec<u8> {
        self.0.value_to_bytes(value)
    }

    #[inline]
    fn pushed(&self) -> &[T] {
        self.0.pushed()
    }

    #[inline]
    fn mut_pushed(&mut self) -> &mut Vec<T> {
        self.0.mut_pushed()
    }

    #[inline]
    fn prev_pushed(&self) -> &[T] {
        self.0.prev_pushed()
    }

    #[inline]
    fn mut_prev_pushed(&mut self) -> &mut Vec<T> {
        self.0.mut_prev_pushed()
    }

    #[inline]
    fn update_stored_len(&self, val: usize) {
        self.0.update_stored_len(val)
    }

    #[inline]
    fn prev_stored_len(&self) -> usize {
        self.0.prev_stored_len()
    }

    #[inline]
    fn mut_prev_stored_len(&mut self) -> &mut usize {
        self.0.mut_prev_stored_len()
    }

    #[inline]
    fn reset(&mut self) -> Result<()> {
        self.0.reset()
    }
}
