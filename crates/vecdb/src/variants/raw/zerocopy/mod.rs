use std::ops::{Deref, DerefMut};

use rawdb::Reader;

use std::path::PathBuf;

use rawdb::{Database, Region};

use crate::{
    AnyStoredVec, AnyVec, BoxedVecIterator, Format, GenericStoredVec, HEADER_OFFSET, Header,
    ImportOptions, Importable, IterableVec, Result, Stamp, TypedVec, VecIndex, Version,
};

use super::RawVecInner;

mod iterators;
mod strategy;
mod value;

pub use iterators::*;
pub use strategy::*;
pub use value::*;

/// Raw storage vector using zerocopy for direct memory mapping in native byte order.
///
/// Uses the `zerocopy` crate for direct memory-mapped access without copying, providing
/// the fastest possible performance. Values are stored in **NATIVE byte order**.
///
/// Like `BytesVec`, this wraps `RawVecInner` and supports:
/// - Holes (deleted indices)
/// - Updated values (modifications to stored data)
/// - Push/rollback operations
///
/// The only difference from `BytesVec` is the serialization strategy:
/// - `ZeroCopyVec`: Native byte order, faster but not portable
/// - `BytesVec`: Explicit little-endian, portable across architectures
///
/// # Portability Warning
///
/// **NOT portable across systems with different endianness.** Data written on a
/// little-endian system (x86) cannot be read correctly on a big-endian system.
/// For portable storage, use `BytesVec` instead.
///
/// Use `ZeroCopyVec` when:
/// - Maximum performance is critical
/// - Data stays on the same architecture
///
/// Use `BytesVec` when:
/// - Cross-platform compatibility is needed
/// - Sharing data between different architectures
#[derive(Debug, Clone)]
#[must_use = "Vector should be stored to keep data accessible"]
pub struct ZeroCopyVec<I, T>(pub(crate) RawVecInner<I, T, ZeroCopyStrategy<T>>);

impl<I, T> Importable for ZeroCopyVec<I, T>
where
    I: VecIndex,
    T: ZeroCopyVecValue,
{
    fn import(db: &Database, name: &str, version: Version) -> Result<Self> {
        Self::import_with((db, name, version).into())
    }

    fn import_with(options: ImportOptions) -> Result<Self> {
        Ok(Self(RawVecInner::import_with(options, Format::ZeroCopy)?))
    }

    fn forced_import(db: &Database, name: &str, version: Version) -> Result<Self> {
        Self::forced_import_with((db, name, version).into())
    }

    fn forced_import_with(options: ImportOptions) -> Result<Self> {
        Ok(Self(RawVecInner::forced_import_with(
            options,
            Format::ZeroCopy,
        )?))
    }
}

impl<I, T> ZeroCopyVec<I, T>
where
    I: VecIndex,
    T: ZeroCopyVecValue,
{
    /// The size of T in bytes.
    pub const SIZE_OF_T: usize = size_of::<T>();

    #[inline]
    pub fn iter(&self) -> Result<ZeroCopyVecIterator<'_, I, T>> {
        self.0.iter()
    }

    #[inline]
    pub fn clean_iter(&self) -> Result<CleanZeroCopyVecIterator<'_, I, T>> {
        self.0.clean_iter()
    }

    #[inline]
    pub fn dirty_iter(&self) -> Result<DirtyZeroCopyVecIterator<'_, I, T>> {
        self.0.dirty_iter()
    }

    #[inline]
    pub fn boxed_iter(&self) -> Result<BoxedVecIterator<'_, I, T>> {
        self.0.boxed_iter()
    }

    // ============================================================================
    // Zerocopy-specific read methods (return references directly from mmap)
    // ============================================================================

    /// Returns a reference to the value directly from the memory-mapped file without copying.
    /// Very efficient for large types or frequent reads.
    ///
    /// Returns `None` if:
    /// - Index is marked as a hole (deleted)
    /// - Index is beyond stored length (might be in pushed layer)
    /// - Index has an updated value (in the updated map, not on disk)
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

    /// Returns a reference without bounds or hole checking.
    ///
    /// # Safety
    /// Caller must ensure index is within stored bounds and not in holes or updated map.
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

impl<I, T> AnyStoredVec for ZeroCopyVec<I, T>
where
    I: VecIndex,
    T: ZeroCopyVecValue,
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

impl<I, T> GenericStoredVec<I, T> for ZeroCopyVec<I, T>
where
    I: VecIndex,
    T: ZeroCopyVecValue,
{
    #[inline]
    fn unchecked_read_at(&self, index: usize, reader: &Reader) -> Result<T> {
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

    // Override methods that handle holes/updated
    #[inline]
    fn get_stored_value_for_serialization(&self, index: usize, reader: &Reader) -> Result<T> {
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
    fn rollback_before(&mut self, stamp: Stamp) -> Result<Stamp> {
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
