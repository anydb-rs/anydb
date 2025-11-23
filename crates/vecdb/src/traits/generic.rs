use std::{cmp::Ordering, collections::BTreeMap, fs, path::PathBuf};

use log::info;
use rawdb::Reader;
use zerocopy::{FromBytes, IntoBytes};

use crate::{
    AnyStoredVec, Error, Result, SIZE_OF_U64, Stamp, Version, likely, vec_region_name_with,
};

const ONE_KIB: usize = 1024;
const ONE_MIB: usize = ONE_KIB * ONE_KIB;
const ONE_GIB: usize = ONE_KIB * ONE_MIB;
const MAX_CACHE_SIZE: usize = ONE_GIB;

use super::{VecIndex, VecValue};

pub trait GenericStoredVec<I, T>: AnyStoredVec + Send + Sync
where
    I: VecIndex,
    T: VecValue,
{
    const SIZE_OF_T: usize = size_of::<T>();

    // ============================================================================
    // Reader Creation
    // ============================================================================

    /// Creates a reader to the underlying region.
    /// Be careful with deadlocks - drop the reader before mutable ops.
    fn create_reader(&'_ self) -> Reader {
        self.region().create_reader()
    }

    // ============================================================================
    // Read Operations (Result-returning)
    // ============================================================================

    /// Reads value at index using provided reader.
    #[inline(always)]
    fn read(&self, index: I, reader: &Reader) -> Result<T> {
        self.read_at(index.to_usize(), reader)
    }

    /// Reads value at index, creating a temporary reader.
    /// For multiple reads, prefer `read()` with a reused reader.
    #[inline(always)]
    fn read_once(&self, index: I) -> Result<T> {
        self.read(index, &self.create_reader())
    }

    /// Reads value at usize index using provided reader.
    #[inline(always)]
    fn read_at(&self, index: usize, reader: &Reader) -> Result<T> {
        let len = self.len();
        if likely(index < len) {
            self.unchecked_read_at(index, reader)
        } else {
            Err(Error::IndexTooHigh { index, len })
        }
    }

    /// Reads value at index using provided reader without checking the upper bound.
    #[doc(hidden)]
    #[inline(always)]
    fn unchecked_read(&self, index: I, reader: &Reader) -> Result<T> {
        self.unchecked_read_at(index.to_usize(), reader)
    }

    /// Reads value at usize index using provided reader without checking the upper bound.
    #[doc(hidden)]
    fn unchecked_read_at(&self, index: usize, reader: &Reader) -> Result<T>;

    /// Reads value at usize index, creating a temporary reader.
    /// For multiple reads, prefer `read_at()` with a reused reader.
    #[inline]
    fn read_at_once(&self, index: usize) -> Result<T> {
        self.read_at(index, &self.create_reader())
    }

    /// Reads value at index using provided reader. Panics if read fails.
    #[inline(always)]
    fn read_unwrap(&self, index: I, reader: &Reader) -> T {
        self.read(index, reader).unwrap()
    }

    /// Reads value at index, creating a temporary reader. Panics if read fails.
    /// For multiple reads, prefer `read_unwrap()` with a reused reader.
    #[inline]
    fn read_unwrap_once(&self, index: I) -> T {
        self.read_unwrap(index, &self.create_reader())
    }

    /// Reads value at usize index using provided reader. Panics if read fails.
    #[inline]
    fn read_at_unwrap(&self, index: usize, reader: &Reader) -> T {
        self.read_at(index, reader).unwrap()
    }

    /// Reads value at usize index, creating a temporary reader. Panics if read fails.
    /// For multiple reads, prefer `read_at_unwrap()` with a reused reader.
    #[inline]
    fn read_at_unwrap_once(&self, index: usize) -> T {
        self.read_at_unwrap(index, &self.create_reader())
    }

    // ============================================================================
    // Get Pushed or Read Operations
    // ============================================================================

    /// Gets value from pushed layer or storage using provided reader.
    #[inline(always)]
    fn get_pushed_or_read(&self, index: I, reader: &Reader) -> Result<Option<T>> {
        self.get_pushed_or_read_at(index.to_usize(), reader)
    }

    /// Gets value from pushed layer or storage, creating a temporary reader.
    /// For multiple reads, prefer `get_pushed_or_read()` with a reused reader.
    #[inline]
    fn get_pushed_or_read_once(&self, index: I) -> Result<Option<T>> {
        self.get_pushed_or_read(index, &self.create_reader())
    }

    /// Gets value from pushed layer or storage at usize index using provided reader.
    /// Does not check the updated layer.
    #[inline(always)]
    fn get_pushed_or_read_at(&self, index: usize, reader: &Reader) -> Result<Option<T>> {
        let stored_len = self.stored_len();
        if index >= stored_len {
            return Ok(self.pushed().get(index - stored_len).cloned());
        }
        Ok(Some(self.unchecked_read_at(index, reader)?))
    }

    /// Gets value from pushed layer or storage at usize index, creating a temporary reader.
    /// For multiple reads, prefer `get_pushed_or_read_at()` with a reused reader.
    #[inline]
    fn get_pushed_or_read_at_once(&self, index: usize) -> Result<Option<T>> {
        self.get_pushed_or_read_at(index, &self.create_reader())
    }

    /// Gets value from pushed layer only (no disk reads).
    #[inline(always)]
    fn get_pushed_at(&self, index: usize, stored_len: usize) -> Option<&T> {
        let pushed = self.pushed();
        let offset = index.checked_sub(stored_len)?;
        pushed.get(offset)
    }

    // ============================================================================
    // Length Operations
    // ============================================================================

    /// Returns the length including both stored and pushed (uncommitted) values.
    /// Named `len_` to avoid conflict with `AnyVec::len`.
    #[inline]
    fn len_(&self) -> usize {
        self.stored_len() + self.pushed_len()
    }

    /// Returns the number of pushed (uncommitted) values.
    #[inline]
    fn pushed_len(&self) -> usize {
        self.pushed().len()
    }

    /// Returns true if there are no pushed (uncommitted) values.
    #[inline]
    fn is_pushed_empty(&self) -> bool {
        self.pushed_len() == 0
    }

    /// Returns true if the index is within the length.
    #[inline]
    fn has(&self, index: I) -> bool {
        self.has_at(index.to_usize())
    }

    /// Returns true if the usize index is within the length.
    #[inline]
    fn has_at(&self, index: usize) -> bool {
        index < self.len_()
    }

    // ============================================================================
    // Pushed Layer Access
    // ============================================================================

    #[doc(hidden)]
    fn prev_pushed(&self) -> &[T];
    #[doc(hidden)]
    fn mut_prev_pushed(&mut self) -> &mut Vec<T>;
    /// Returns the current pushed (uncommitted) values.
    fn pushed(&self) -> &[T];
    /// Returns a mutable reference to the current pushed (uncommitted) values.
    fn mut_pushed(&mut self) -> &mut Vec<T>;

    /// Pushes a new value to the end of the vector.
    #[inline]
    fn push(&mut self, value: T) {
        self.mut_pushed().push(value)
    }

    /// Pushes a value if the index equals the current length, otherwise does nothing if already exists.
    /// Returns an error if the index is too high.
    #[inline]
    fn push_if_needed(&mut self, index: I, value: T) -> Result<()> {
        let index = index.to_usize();
        let len = self.len();

        if index == len {
            self.push(value);
            return Ok(());
        }

        // Already pushed
        if index < len {
            return Ok(());
        }

        // This should never happen in correct code
        debug_assert!(
            false,
            "Index too high: idx={}, len={}, header={:?}, region={}",
            index,
            len,
            self.header(),
            self.region().index()
        );

        Err(Error::IndexTooHigh { index, len })
    }

    /// Pushes a value at the given index, truncating if necessary.
    #[inline]
    fn truncate_push(&mut self, index: I, value: T) -> Result<()> {
        self.truncate_push_at(index.to_usize(), value)
    }

    /// Pushes a value at the given usize index, truncating if necessary.
    #[inline]
    fn truncate_push_at(&mut self, index: usize, value: T) -> Result<()> {
        let len = self.len();
        match len.cmp(&index) {
            Ordering::Less => {
                return Err(Error::IndexTooHigh { index, len });
            }
            ord => {
                if ord == Ordering::Greater {
                    self.truncate_if_needed_at(index)?;
                }
                self.push(value);
            }
        }
        Ok(())
    }

    /// Returns true if the pushed cache has reached the batch limit (~1GiB).
    #[inline]
    fn batch_limit_reached(&self) -> bool {
        self.pushed_len() * Self::SIZE_OF_T >= MAX_CACHE_SIZE
    }

    // ============================================================================
    // Storage Length Management
    // ============================================================================

    #[doc(hidden)]
    fn prev_stored_len(&self) -> usize;
    #[doc(hidden)]
    fn mut_prev_stored_len(&mut self) -> &mut usize;
    #[doc(hidden)]
    fn update_stored_len(&self, val: usize);

    // ============================================================================
    // Truncate Operations
    // ============================================================================

    /// Truncates the vector to the given index if the current length exceeds it.
    fn truncate_if_needed(&mut self, index: I) -> Result<()> {
        self.truncate_if_needed_at(index.to_usize())
    }

    /// Default truncate implementation handling pushed layer only.
    /// Returns true if stored_len needs to be updated to `index`.
    /// RawVec overrides truncate_if_needed_at to also handle holes/updated.
    #[doc(hidden)]
    fn default_truncate_if_needed_at(&mut self, index: usize) -> Result<bool> {
        let stored_len = self.stored_len();
        let pushed_len = self.pushed_len();
        let len = stored_len + pushed_len;

        if index >= len {
            return Ok(false);
        }

        if index <= stored_len {
            self.mut_pushed().clear();
        } else {
            self.mut_pushed().truncate(index - stored_len);
        }

        Ok(index < stored_len)
    }

    /// Truncates the vector to the given usize index if the current length exceeds it.
    fn truncate_if_needed_at(&mut self, index: usize) -> Result<()> {
        if self.default_truncate_if_needed_at(index)? {
            self.update_stored_len(index);
        }
        Ok(())
    }

    /// Truncates the vector to the given index if needed, updating the stamp.
    #[inline]
    fn truncate_if_needed_with_stamp(&mut self, index: I, stamp: Stamp) -> Result<()> {
        self.update_stamp(stamp);
        self.truncate_if_needed(index)
    }

    // ============================================================================
    // Reset and Clear Operations
    // ============================================================================

    /// Resets the vector state.
    fn reset(&mut self) -> Result<()>;

    /// Clears all values from the vector.
    #[inline]
    fn clear(&mut self) -> Result<()> {
        self.truncate_if_needed_at(0)
    }

    /// Default reset_unsaved implementation - clears pushed layer only.
    /// RawVec overrides to also clear holes/updated.
    #[doc(hidden)]
    fn default_reset_unsaved(&mut self) {
        self.mut_pushed().clear();
    }

    /// Resets uncommitted changes.
    fn reset_unsaved(&mut self) {
        self.default_reset_unsaved();
    }

    /// Validates the computed version against the stored version, resetting if they don't match.
    fn validate_computed_version_or_reset(&mut self, version: Version) -> Result<()> {
        if version != self.header().computed_version() {
            self.mut_header().update_computed_version(version);
            if !self.is_empty() {
                self.reset()?;
            }
        }

        if self.is_empty() {
            info!(
                "Computing {}_to_{}...",
                self.index_type_to_string(),
                self.name()
            )
        }

        Ok(())
    }

    // ============================================================================
    // Dirty State Checking
    // ============================================================================

    /// Returns true if there are uncommitted changes (pushed values).
    fn is_dirty(&self) -> bool {
        !self.is_pushed_empty()
    }

    // ============================================================================
    // Changes and Rollback Operations
    // ============================================================================

    /// Returns the path to the changes directory for this vector.
    fn changes_path(&self) -> PathBuf {
        self.db_path().join(self.index_to_name()).join("changes")
    }

    /// Flushes with the given stamp, optionally saving changes for rollback.
    #[inline]
    fn stamped_flush_maybe_with_changes(&mut self, stamp: Stamp, with_changes: bool) -> Result<()> {
        if with_changes {
            self.stamped_flush_with_changes(stamp)
        } else {
            self.stamped_flush(stamp)
        }
    }

    /// Default implementation of stamped_flush_with_changes.
    /// Handles file management, serialization, flush, and base prev_ field updates.
    /// RawVec overrides stamped_flush_with_changes to also update prev_holes/prev_updated.
    #[doc(hidden)]
    fn default_stamped_flush_with_changes(&mut self, stamp: Stamp) -> Result<()> {
        let saved_stamped_changes = self.saved_stamped_changes();

        if saved_stamped_changes == 0 {
            return self.stamped_flush(stamp);
        }

        let path = self.changes_path();

        fs::create_dir_all(&path)?;

        let files: BTreeMap<Stamp, PathBuf> = fs::read_dir(&path)?
            .filter_map(|entry| {
                let path = entry.ok()?.path();
                let name = path.file_name()?.to_str()?;
                if let Ok(s) = name.parse::<u64>().map(Stamp::from) {
                    if s < stamp {
                        Some((s, path))
                    } else {
                        let _ = fs::remove_file(path);
                        None
                    }
                } else {
                    None
                }
            })
            .collect();

        for (_, path) in files.iter().take(
            files
                .len()
                .saturating_sub((saved_stamped_changes - 1) as usize),
        ) {
            fs::remove_file(path)?;
        }

        fs::write(
            path.join(u64::from(stamp).to_string()),
            self.serialize_changes()?,
        )?;

        self.stamped_flush(stamp)?;

        // Update prev_ fields to reflect the PERSISTED state after flush
        *self.mut_prev_stored_len() = self.stored_len();
        *self.mut_prev_pushed() = vec![];

        Ok(())
    }

    /// Flushes with the given stamp, saving changes to enable rollback.
    fn stamped_flush_with_changes(&mut self, stamp: Stamp) -> Result<()> {
        self.default_stamped_flush_with_changes(stamp)
    }

    /// Default implementation of rollback_before.
    /// Handles the rollback loop and base prev_ field updates.
    /// RawVec overrides rollback_before to also update prev_holes/prev_updated.
    #[doc(hidden)]
    fn default_rollback_before(&mut self, stamp: Stamp) -> Result<Stamp> {
        if self.stamp() < stamp {
            return Ok(self.stamp());
        }

        let dir = fs::read_dir(self.changes_path())?
            .filter_map(|entry| {
                let path = entry.ok()?.path();
                let name = path.file_name()?.to_str()?;
                if let Ok(stamp) = name.parse::<u64>().map(Stamp::from) {
                    Some((stamp, path))
                } else {
                    None
                }
            })
            .collect::<BTreeMap<Stamp, PathBuf>>();

        let mut iter = dir.range(..=self.stamp());

        while let Some((&s, _)) = iter.next_back()
            && self.stamp() >= stamp
        {
            if s != self.stamp() {
                return Err(Error::StampMismatch {
                    file: s,
                    vec: self.stamp(),
                });
            }
            self.rollback()?;
        }

        // Save the restored state to prev_ fields so they're available for the next flush
        *self.mut_prev_stored_len() = self.stored_len();
        *self.mut_prev_pushed() = self.pushed().to_vec();

        Ok(self.stamp())
    }

    /// Rolls back changes to before the given stamp.
    fn rollback_before(&mut self, stamp: Stamp) -> Result<Stamp> {
        self.default_rollback_before(stamp)
    }

    /// Rolls back the most recent change set.
    fn rollback(&mut self) -> Result<()> {
        let path = self
            .changes_path()
            .join(u64::from(self.stamp()).to_string());
        let bytes = fs::read(&path)?;
        self.deserialize_then_undo_changes(&bytes)
    }

    /// Restores a truncated value during deserialization.
    /// Default implementation pushes it back. RawVec overrides to insert into updated map.
    #[doc(hidden)]
    fn restore_truncated_value(&mut self, _index: usize, value: T) {
        self.push(value);
    }

    /// Default implementation of deserialize_then_undo_changes.
    /// Returns the position after parsing the base data so RawVec can continue.
    #[doc(hidden)]
    fn default_deserialize_then_undo_changes(&mut self, bytes: &[u8]) -> Result<usize> {
        let mut pos = 0;
        let mut len = SIZE_OF_U64;

        let prev_stamp = u64::read_from_bytes(&bytes[..pos + len])?;
        self.mut_header().update_stamp(Stamp::new(prev_stamp));
        pos += len;

        let prev_stored_len = usize::read_from_bytes(&bytes[pos..pos + len])?;
        pos += len;

        let _stored_len = usize::read_from_bytes(&bytes[pos..pos + len])?;
        pos += len;

        let current_stored_len = self.stored_len();

        // Restore to the length BEFORE the changes that we're undoing
        if prev_stored_len < current_stored_len {
            self.truncate_if_needed_at(prev_stored_len)?;
        } else if prev_stored_len > current_stored_len {
            self.update_stored_len(prev_stored_len);
        }

        let truncated_count = usize::read_from_bytes(&bytes[pos..pos + len])?;
        pos += len;

        // Clear pushed (will be replaced with prev_pushed from change file)
        self.mut_pushed().clear();

        // Restore truncated values
        if truncated_count > 0 {
            len = Self::SIZE_OF_T * truncated_count;
            let truncated_values = bytes[pos..pos + len]
                .chunks(Self::SIZE_OF_T)
                .map(|b| T::read_from_bytes(b).map_err(|_| Error::ZeroCopyError))
                .collect::<Result<Vec<_>>>()?;
            pos += len;

            let start_index = prev_stored_len - truncated_count;
            for (i, val) in truncated_values.into_iter().enumerate() {
                self.restore_truncated_value(start_index + i, val);
            }
        }

        len = SIZE_OF_U64;
        let prev_pushed_len = usize::read_from_bytes(&bytes[pos..pos + len])?;
        pos += len;
        len = Self::SIZE_OF_T * prev_pushed_len;
        let mut prev_pushed = bytes[pos..pos + len]
            .chunks(Self::SIZE_OF_T)
            .map(|s| T::read_from_bytes(s).map_err(|_| Error::ZeroCopyError))
            .collect::<Result<Vec<_>>>()?;
        pos += len;
        self.mut_pushed().append(&mut prev_pushed);

        len = SIZE_OF_U64;
        let pushed_len = usize::read_from_bytes(&bytes[pos..pos + len])?;
        pos += len;
        // Skip current pushed values (we already restored prev_pushed)
        pos += Self::SIZE_OF_T * pushed_len;

        // After rollback, prev_* should reflect the rolled-back state
        *self.mut_prev_pushed() = self.pushed().to_vec();

        Ok(pos)
    }

    /// Deserializes change data and undoes those changes.
    /// Base implementation handles pushed and truncated data. RawVec overrides for holes/updated.
    fn deserialize_then_undo_changes(&mut self, bytes: &[u8]) -> Result<()> {
        self.default_deserialize_then_undo_changes(bytes)?;
        Ok(())
    }

    // ============================================================================
    // Serialization
    // ============================================================================

    /// Gets a stored value for serialization purposes.
    /// Default implementation reads directly from disk.
    /// RawVec overrides to check prev_updated first.
    #[doc(hidden)]
    fn get_stored_value_for_serialization(&self, index: usize, reader: &Reader) -> Result<T> {
        self.unchecked_read_at(index, reader)
    }

    /// Default implementation of serialize_changes.
    /// Serializes: stamp, prev_stored_len, stored_len, truncated_count, truncated_values,
    /// prev_pushed, pushed.
    /// RawVec calls this and appends holes/updated data.
    #[doc(hidden)]
    fn default_serialize_changes(&self) -> Result<Vec<u8>> {
        let mut bytes = vec![];
        let reader = self.create_reader();

        bytes.extend(self.stamp().as_bytes());

        let prev_stored_len = self.prev_stored_len();
        let stored_len = self.stored_len();

        bytes.extend(prev_stored_len.as_bytes());
        bytes.extend(stored_len.as_bytes());

        let truncated = prev_stored_len.checked_sub(stored_len).unwrap_or_default();
        bytes.extend(truncated.as_bytes());

        if truncated > 0 {
            let truncated_vals = (stored_len..prev_stored_len)
                .map(|i| self.get_stored_value_for_serialization(i, &reader))
                .collect::<Result<Vec<_>>>()?;
            bytes.extend(truncated_vals.as_bytes());
        }

        bytes.extend(self.prev_pushed().len().as_bytes());
        bytes.extend(self.prev_pushed().iter().flat_map(|v| v.as_bytes()));

        bytes.extend(self.pushed().len().as_bytes());
        bytes.extend(self.pushed().iter().flat_map(|v| v.as_bytes()));

        Ok(bytes)
    }

    // ============================================================================
    // Names
    // ============================================================================

    /// Returns the region name for this vector.
    fn vec_region_name(&self) -> String {
        vec_region_name_with::<I>(self.name())
    }
}
