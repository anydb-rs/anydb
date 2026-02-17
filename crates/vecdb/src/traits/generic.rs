use std::{collections::BTreeMap, fs, path::PathBuf};

use log::info;
use rawdb::unlikely;

use crate::{
    AnyStoredVec, Bytes, Error, Result, SIZE_OF_U64, Stamp, VecIndex, VecValue, Version,
};

/// Maximum in-memory cache size before forcing a flush (1 GiB).
/// Prevents unbounded memory growth when pushing many values without flushing.
pub(crate) const MAX_CACHE_SIZE: usize = 1024 * 1024 * 1024;

/// Typed interface for stored vectors (push, truncate, rollback).
///
/// Provides the core write operations for all stored vec types.
/// For reading, use [`ScannableVec`] (`collect_range`, `fold_range`, etc.).
/// Raw vecs (`BytesVec`, `ZeroCopyVec`) additionally provide
/// `VecReader` for O(1) random access.
///
/// [`ScannableVec`]: crate::ScannableVec
pub trait GenericStoredVec<I, T>: AnyStoredVec
where
    I: VecIndex,
    T: VecValue,
{
    const SIZE_OF_T: usize = size_of::<T>();

    // ── Serialization helpers ────────────────────────────────────────

    /// Collects stored values in `[from, to)` for serialization purposes.
    ///
    /// May read beyond `stored_len` to recover truncated values still on disk.
    /// `RawVecInner` checks `prev_updated` first, then reads via reader.
    /// `CompressedVecInner` decodes the relevant pages directly.
    #[doc(hidden)]
    fn collect_stored_range(&self, from: usize, to: usize) -> Result<Vec<T>>;

    /// Deserializes a value from bytes using the vector's strategy.
    fn read_value_from_bytes(&self, bytes: &[u8]) -> Result<T>;

    /// Serializes a value into the buffer using the vector's strategy.
    fn write_value_to(&self, value: &T, buf: &mut Vec<u8>);

    /// Serializes multiple values into the buffer.
    #[inline(always)]
    fn write_values_to(&self, values: &[T], buf: &mut Vec<u8>) {
        for v in values {
            self.write_value_to(v, buf);
        }
    }

    #[doc(hidden)]
    #[inline(always)]
    fn get_pushed_at(&self, index: usize, stored_len: usize) -> Option<&T> {
        let pushed = self.pushed();
        let offset = index.checked_sub(stored_len)?;
        pushed.get(offset)
    }

    // ── Length / state queries ────────────────────────────────────────

    /// Total length including stored and pushed (uncommitted) values.
    ///
    /// Named `len_` to avoid conflict with `AnyVec::len` (which returns the same value).
    #[inline]
    fn len_(&self) -> usize {
        self.stored_len() + self.pushed_len()
    }

    /// Number of pushed (uncommitted) values in the memory buffer.
    #[inline]
    fn pushed_len(&self) -> usize {
        self.pushed().len()
    }

    /// Returns true if there are no pushed (uncommitted) values.
    #[inline]
    fn is_pushed_empty(&self) -> bool {
        self.pushed_len() == 0
    }

    /// Returns true if the typed index is within bounds.
    #[inline]
    fn has(&self, index: I) -> bool {
        self.has_at(index.to_usize())
    }

    /// Returns true if the usize index is within bounds.
    #[inline]
    fn has_at(&self, index: usize) -> bool {
        index < self.len_()
    }

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

    /// Pushes a value at the given index, erroring if index != current length.
    /// Use this when you expect to always append in order.
    #[inline]
    fn checked_push(&mut self, index: I, value: T) -> Result<()> {
        self.checked_push_at(index.to_usize(), value)
    }

    /// Pushes a value at the given usize index, erroring if index != current length.
    /// Use this when you expect to always append in order.
    #[inline]
    fn checked_push_at(&mut self, index: usize, value: T) -> Result<()> {
        let len = self.len();

        if unlikely(index != len) {
            return Err(Error::UnexpectedIndex {
                expected: len,
                got: index,
            });
        }

        self.push(value);
        Ok(())
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

        if unlikely(len < index) {
            return Err(Error::IndexTooHigh { index, len, name: self.name().to_string() });
        } else if unlikely(len > index) {
            self.truncate_if_needed_at(index)?;
        }

        self.push(value);

        Ok(())
    }

    /// Returns true if the pushed cache has reached the batch limit (~1GiB).
    ///
    /// When this limit is reached, the caller should flush to disk before continuing.
    /// This prevents excessive memory usage during bulk operations.
    #[inline]
    fn batch_limit_reached(&self) -> bool {
        self.pushed_len() * Self::SIZE_OF_T >= MAX_CACHE_SIZE
    }

    /// Extends the vector to `target_len`, filling with `value`.
    /// Batches writes in ~1GB chunks to avoid memory explosion.
    fn fill_to(&mut self, target_len: usize, value: T) -> Result<()>
    where
        T: Copy,
    {
        let batch_count = MAX_CACHE_SIZE / Self::SIZE_OF_T;

        while self.len() < target_len {
            let count = (target_len - self.len()).min(batch_count);
            let new_len = self.pushed_len() + count;
            self.mut_pushed().resize(new_len, value);
            self.write()?;
        }
        Ok(())
    }

    #[doc(hidden)]
    fn prev_stored_len(&self) -> usize;
    #[doc(hidden)]
    fn mut_prev_stored_len(&mut self) -> &mut usize;
    #[doc(hidden)]
    fn update_stored_len(&self, val: usize);

    /// Truncates the vector to the given index if the current length exceeds it.
    fn truncate_if_needed(&mut self, index: I) -> Result<()> {
        self.truncate_if_needed_at(index.to_usize())
    }

    /// Default truncate implementation handling pushed layer only.
    /// Returns true if stored_len needs to be updated to `index`.
    /// RawVecInner overrides truncate_if_needed_at to also handle holes/updated.
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

    /// Resets the vector state.
    fn reset(&mut self) -> Result<()>;

    /// Default reset implementation - clears data and rollback history.
    /// Clears pushed data, resets stored_len to 0, and removes rollback changes directory.
    /// Vector types should call this and perform any additional type-specific cleanup.
    #[doc(hidden)]
    fn default_reset(&mut self) -> Result<()> {
        // Clear the data
        self.clear()?;

        // Reset to fresh state - clear rollback history
        *self.mut_prev_stored_len() = 0;
        self.mut_prev_pushed().clear();
        self.update_stamp(Stamp::default());

        // Remove changes directory if it exists
        let changes_path = self.changes_path();
        if changes_path.exists() {
            std::fs::remove_dir_all(&changes_path)?;
        }

        Ok(())
    }

    /// Clears all values from the vector.
    #[inline]
    fn clear(&mut self) -> Result<()> {
        self.truncate_if_needed_at(0)
    }

    /// Default reset_unsaved implementation - clears pushed layer only.
    /// RawVecInner overrides to also clear holes/updated.
    #[doc(hidden)]
    fn default_reset_unsaved(&mut self) {
        self.mut_pushed().clear();
    }

    /// Resets uncommitted changes.
    fn reset_unsaved(&mut self) {
        self.default_reset_unsaved();
    }

    /// Validates the computed version against the stored version, resetting if they don't match.
    /// Automatically includes the vec's own version - only pass dependency versions.
    fn validate_computed_version_or_reset(&mut self, dep_version: Version) -> Result<()> {
        let version = self.header().vec_version() + dep_version;
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

    /// Returns true if there are uncommitted changes (pushed values).
    fn is_dirty(&self) -> bool {
        !self.is_pushed_empty()
    }

    /// Returns the path to the changes directory for this vector.
    fn changes_path(&self) -> PathBuf {
        self.db_path().join("changes").join(self.region_name())
    }

    /// Flushes with the given stamp, optionally saving changes for rollback.
    #[inline]
    fn stamped_write_maybe_with_changes(&mut self, stamp: Stamp, with_changes: bool) -> Result<()> {
        if with_changes {
            self.stamped_write_with_changes(stamp)
        } else {
            self.stamped_write(stamp)
        }
    }

    /// Default implementation of stamped_write_with_changes.
    /// Handles file management, serialization, flush, and base prev_ field updates.
    /// RawVecInner overrides stamped_write_with_changes to also update prev_holes/prev_updated.
    #[doc(hidden)]
    fn default_stamped_write_with_changes(&mut self, stamp: Stamp) -> Result<()> {
        let saved_stamped_changes = self.saved_stamped_changes();

        if saved_stamped_changes == 0 {
            return self.stamped_write(stamp);
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

        self.stamped_write(stamp)?;

        // Update prev_ fields to reflect the PERSISTED state after flush
        *self.mut_prev_stored_len() = self.stored_len();
        self.mut_prev_pushed().clear();

        Ok(())
    }

    /// Flushes with the given stamp, saving changes to enable rollback.
    fn stamped_write_with_changes(&mut self, stamp: Stamp) -> Result<()> {
        self.default_stamped_write_with_changes(stamp)
    }

    /// Default implementation of rollback_before.
    /// Handles the rollback loop and base prev_ field updates.
    /// RawVecInner overrides rollback_before to also update prev_holes/prev_updated.
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
    /// Default implementation pushes it back. RawVecInner overrides to insert into updated map.
    #[doc(hidden)]
    fn restore_truncated_value(&mut self, _index: usize, value: T) {
        self.push(value);
    }

    /// Default implementation of deserialize_then_undo_changes.
    /// Returns the position after parsing the base data so RawVecInner can continue.
    #[doc(hidden)]
    fn default_deserialize_then_undo_changes(&mut self, bytes: &[u8]) -> Result<usize> {
        let mut pos = 0;
        let mut len = SIZE_OF_U64;

        let prev_stamp = Stamp::from_bytes(&bytes[..pos + len])?;
        self.mut_header().update_stamp(prev_stamp);
        pos += len;

        let prev_stored_len = usize::from_bytes(&bytes[pos..pos + len])?;
        pos += len;

        let _stored_len = usize::from_bytes(&bytes[pos..pos + len])?;
        pos += len;

        let current_stored_len = self.stored_len();

        // Restore to the length BEFORE the changes that we're undoing
        if prev_stored_len < current_stored_len {
            self.truncate_if_needed_at(prev_stored_len)?;
        } else if prev_stored_len > current_stored_len {
            self.update_stored_len(prev_stored_len);
        }

        let truncated_count = usize::from_bytes(&bytes[pos..pos + len])?;
        pos += len;

        // Clear pushed (will be replaced with prev_pushed from change file)
        self.mut_pushed().clear();

        // Restore truncated values
        if truncated_count > 0 {
            len = Self::SIZE_OF_T * truncated_count;
            let truncated_values = bytes[pos..pos + len]
                .chunks(Self::SIZE_OF_T)
                .map(|b| self.read_value_from_bytes(b))
                .collect::<Result<Vec<_>>>()?;
            pos += len;

            let start_index = prev_stored_len - truncated_count;
            for (i, val) in truncated_values.into_iter().enumerate() {
                self.restore_truncated_value(start_index + i, val);
            }
        }

        len = SIZE_OF_U64;
        let prev_pushed_len = usize::from_bytes(&bytes[pos..pos + len])?;
        pos += len;
        len = Self::SIZE_OF_T * prev_pushed_len;
        let mut prev_pushed = bytes[pos..pos + len]
            .chunks(Self::SIZE_OF_T)
            .map(|s| self.read_value_from_bytes(s))
            .collect::<Result<Vec<_>>>()?;
        pos += len;
        self.mut_pushed().append(&mut prev_pushed);

        len = SIZE_OF_U64;
        let pushed_len = usize::from_bytes(&bytes[pos..pos + len])?;
        pos += len;
        // Skip current pushed values (we already restored prev_pushed)
        pos += Self::SIZE_OF_T * pushed_len;

        // After rollback, prev_* should reflect the rolled-back state
        *self.mut_prev_pushed() = self.pushed().to_vec();

        Ok(pos)
    }

    /// Deserializes change data and undoes those changes.
    /// Base implementation handles pushed and truncated data. RawVecInner overrides for holes/updated.
    fn deserialize_then_undo_changes(&mut self, bytes: &[u8]) -> Result<()> {
        self.default_deserialize_then_undo_changes(bytes)?;
        Ok(())
    }

    /// Default implementation of serialize_changes.
    /// Serializes: stamp, prev_stored_len, stored_len, truncated_count, truncated_values,
    /// prev_pushed, pushed.
    /// RawVecInner calls this and appends holes/updated data.
    #[doc(hidden)]
    fn default_serialize_changes(&self) -> Result<Vec<u8>> {
        let prev_stored_len = self.prev_stored_len();
        let stored_len = self.stored_len();
        let truncated = prev_stored_len.checked_sub(stored_len).unwrap_or_default();

        // Pre-allocate: 4 headers + truncated_count + prev_pushed_len + pushed_len (6 × 8 bytes)
        // + truncated values + prev_pushed values + pushed values
        let value_count = truncated + self.prev_pushed().len() + self.pushed().len();
        let mut bytes = Vec::with_capacity(6 * SIZE_OF_U64 + value_count * Self::SIZE_OF_T);

        bytes.extend(self.stamp().to_bytes());

        bytes.extend(prev_stored_len.to_bytes());
        bytes.extend(stored_len.to_bytes());

        bytes.extend(truncated.to_bytes());

        if truncated > 0 {
            let truncated_vals = self.collect_stored_range(stored_len, prev_stored_len)?;
            self.write_values_to(&truncated_vals, &mut bytes);
        }

        bytes.extend(self.prev_pushed().len().to_bytes());
        self.write_values_to(self.prev_pushed(), &mut bytes);

        bytes.extend(self.pushed().len().to_bytes());
        self.write_values_to(self.pushed(), &mut bytes);

        Ok(bytes)
    }
}
