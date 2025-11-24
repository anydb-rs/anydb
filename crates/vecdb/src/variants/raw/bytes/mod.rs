mod iterators;
mod strategy;
mod r#trait;
mod value;

use std::{
    ops::{Deref, DerefMut},
    path::PathBuf,
};

use rawdb::{Database, Region};

pub use iterators::*;
pub use strategy::*;
pub use r#trait::*;
pub use value::*;

use crate::{
    AnyStoredVec, AnyVec, BoxedVecIterator, Format, GenericStoredVec, Header, Importable,
    ImportOptions, IterableVec, Result, Stamp, TypedVec, VecIndex, Version,
};

use super::{CleanRawVecIterator, DirtyRawVecIterator, RawVecInner, RawVecIterator};

/// Raw storage vector that stores values using custom Bytes serialization.
///
/// This is similar to ZeroCopyVec but uses the Bytes trait for serialization
/// instead of zerocopy. Useful for types that need custom serialization logic
/// but still want efficient raw storage.
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
        Ok(Self(RawVecInner::forced_import_with(options, Format::Bytes)?))
    }
}

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

impl<I, T> AnyStoredVec for BytesVec<I, T>
where
    I: VecIndex,
    T: BytesVecValue,
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

impl<I, T> GenericStoredVec<I, T> for BytesVec<I, T>
where
    I: VecIndex,
    T: BytesVecValue,
{
    #[inline]
    fn unchecked_read_at(&self, index: usize, reader: &rawdb::Reader) -> Result<T> {
        self.0.unchecked_read_at(index, reader)
    }

    #[inline]
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

    // Override methods that handle holes/updated
    #[inline]
    fn get_stored_value_for_serialization(
        &self,
        index: usize,
        reader: &rawdb::Reader,
    ) -> Result<T> {
        self.0.get_stored_value_for_serialization(index, reader)
    }

    #[inline]
    fn restore_truncated_value(&mut self, index: usize, value: T) {
        self.0.restore_truncated_value(index, value)
    }

    #[inline]
    fn truncate_if_needed_at(&mut self, index: usize) -> Result<()> {
        self.0.truncate_if_needed_at(index)
    }

    #[inline]
    fn reset_unsaved(&mut self) {
        self.0.reset_unsaved()
    }

    #[inline]
    fn is_dirty(&self) -> bool {
        self.0.is_dirty()
    }

    #[inline]
    fn stamped_flush_with_changes(&mut self, stamp: Stamp) -> Result<()> {
        self.0.stamped_flush_with_changes(stamp)
    }

    #[inline]
    fn rollback_before(&mut self, stamp: Stamp) -> Result<crate::Stamp> {
        self.0.rollback_before(stamp)
    }

    #[inline]
    fn rollback(&mut self) -> Result<()> {
        self.0.rollback()
    }

    #[inline]
    fn deserialize_then_undo_changes(&mut self, bytes: &[u8]) -> Result<()> {
        self.0.deserialize_then_undo_changes(bytes)
    }
}
