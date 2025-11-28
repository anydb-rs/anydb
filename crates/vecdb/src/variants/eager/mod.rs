use std::{fmt::Debug, path::PathBuf};

use rawdb::{Database, Reader, Region};

mod aggregates;
mod arithmetic;
mod checked_sub;
mod lookback;
mod saturating_add;
mod statistics;
mod transforms;

pub use checked_sub::*;
pub use saturating_add::*;

use crate::{
    AnyStoredVec, AnyVec, BoxedVecIterator, Exit, GenericStoredVec, Header, ImportOptions,
    Importable, IterableVec, PrintableIndex, Result, StoredVec, TypedVec, Version,
};

/// Wrapper for computing and storing derived values from source vectors.
///
/// `EagerVec` wraps any `StoredVec` and provides computation methods to derive and persist
/// calculated values. Results are stored on disk and automatically recomputed when:
/// - Source data versions change
/// - The vector's computation logic version changes
///
/// # Key Features
/// - **Incremental Updates**: Only computes missing values, not the entire dataset
/// - **Automatic Versioning**: Detects stale data and recomputes automatically
/// - **Batched Writes**: Flushes periodically to prevent excessive memory usage
///
/// # Common Operations
/// - Transformations: `compute_transform()`, `compute_range()`
/// - Arithmetic: `compute_add()`, `compute_subtract()`, `compute_multiply()`, `compute_divide()`
/// - Moving statistics: `compute_sma()`, `compute_ema()`, `compute_sum()`, `compute_max()`, `compute_min()`
/// - Lookback calculations: `compute_change()`, `compute_percentage_change()`
#[derive(Debug, Clone)]
#[must_use = "Vector should be stored to keep data accessible"]
pub struct EagerVec<V>(V);

impl<V: Importable> Importable for EagerVec<V> {
    fn import(db: &Database, name: &str, version: Version) -> Result<Self> {
        Ok(Self(V::import(db, name, version)?))
    }

    fn import_with(options: ImportOptions) -> Result<Self> {
        Ok(Self(V::import_with(options)?))
    }

    fn forced_import(db: &Database, name: &str, version: Version) -> Result<Self> {
        Ok(Self(V::forced_import(db, name, version)?))
    }

    fn forced_import_with(options: ImportOptions) -> Result<Self> {
        Ok(Self(V::forced_import_with(options)?))
    }
}

impl<V> EagerVec<V>
where
    V: StoredVec,
{
    #[inline]
    pub fn inner_version(&self) -> Version {
        self.0.header().vec_version()
    }

    /// Helper that repeatedly calls a compute function until it completes.
    /// Flushes between iterations when batch limit is hit.
    pub(super) fn repeat_until_complete<F>(&mut self, exit: &Exit, mut f: F) -> Result<()>
    where
        F: FnMut(&mut Self) -> Result<()>,
    {
        loop {
            f(self)?;
            let batch_limit_reached = self.batch_limit_reached();
            self.safe_flush(exit)?;
            if !batch_limit_reached {
                break;
            }
        }

        Ok(())
    }

    /// Removes this vector and all its associated regions from the database
    pub fn remove(self) -> Result<()> {
        self.0.remove()
    }
}

impl<V> AnyVec for EagerVec<V>
where
    V: StoredVec,
{
    #[inline]
    fn version(&self) -> Version {
        self.0.header().computed_version()
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
        <V::I as PrintableIndex>::to_string()
    }

    #[inline]
    fn value_type_to_size_of(&self) -> usize {
        size_of::<V::T>()
    }

    #[inline]
    fn region_names(&self) -> Vec<String> {
        self.0.region_names()
    }
}

impl<V> AnyStoredVec for EagerVec<V>
where
    V: StoredVec,
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
    fn write(&mut self) -> Result<()> {
        self.0.write()
    }

    #[inline]
    fn stored_len(&self) -> usize {
        self.0.stored_len()
    }

    #[inline]
    fn real_stored_len(&self) -> usize {
        self.0.real_stored_len()
    }

    #[inline]
    fn serialize_changes(&self) -> Result<Vec<u8>> {
        self.0.serialize_changes()
    }

    #[inline]
    fn db(&self) -> Database {
        self.0.db()
    }

    fn remove(self) -> Result<()> {
        self.0.remove()
    }
}

impl<V> GenericStoredVec<V::I, V::T> for EagerVec<V>
where
    V: StoredVec,
{
    #[inline]
    fn unchecked_read_at(&self, index: usize, reader: &Reader) -> Result<V::T> {
        self.0.unchecked_read_at(index, reader)
    }

    #[inline(always)]
    fn read_value_from_bytes(&self, bytes: &[u8]) -> Result<V::T> {
        self.0.read_value_from_bytes(bytes)
    }

    #[inline]
    fn value_to_bytes(&self, value: &V::T) -> Vec<u8> {
        self.0.value_to_bytes(value)
    }

    #[inline]
    fn pushed(&self) -> &[V::T] {
        self.0.pushed()
    }
    #[inline]
    fn mut_pushed(&mut self) -> &mut Vec<V::T> {
        self.0.mut_pushed()
    }
    #[inline]
    fn prev_pushed(&self) -> &[V::T] {
        self.0.prev_pushed()
    }
    #[inline]
    fn mut_prev_pushed(&mut self) -> &mut Vec<V::T> {
        self.0.mut_prev_pushed()
    }

    #[inline]
    #[doc(hidden)]
    fn update_stored_len(&self, val: usize) {
        self.0.update_stored_len(val);
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
    fn truncate_if_needed(&mut self, index: V::I) -> Result<()> {
        self.0.truncate_if_needed(index)
    }

    #[inline]
    fn reset(&mut self) -> Result<()> {
        self.0.reset()
    }
}

impl<'a, V> IntoIterator for &'a EagerVec<V>
where
    V: StoredVec,
    &'a V: IntoIterator<Item = V::T>,
{
    type Item = V::T;
    type IntoIter = <&'a V as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        (&self.0).into_iter()
    }
}

impl<V> IterableVec<V::I, V::T> for EagerVec<V>
where
    V: StoredVec,
{
    fn iter(&self) -> BoxedVecIterator<'_, V::I, V::T> {
        self.0.iter()
    }
}

impl<V> TypedVec for EagerVec<V>
where
    V: StoredVec,
{
    type I = V::I;
    type T = V::T;
}
