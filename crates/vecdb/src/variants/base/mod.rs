use std::{collections::BTreeMap, fs, marker::PhantomData, path::PathBuf, sync::Arc};

use rawdb::{Database, Region};

use crate::{Bytes, Error, Result, SIZE_OF_U64, Stamp, VecIndex, VecValue, Version};

mod format;
mod header;
mod options;
mod shared_len;
mod stored_len;
mod with_prev;

pub use format::*;
pub use header::*;
pub use options::*;
pub use shared_len::*;
pub use stored_len::*;
pub use with_prev::*;

/// Base storage vector with fields common to all stored vector implementations.
///
/// Holds the core state shared across BytesVec, ZeroCopyVec, and compressed variants:
/// region storage, header metadata, pushed values, and length tracking.
#[derive(Debug, Clone)]
pub(crate) struct BaseVec<I, T> {
    pushed: WithPrev<Vec<T>>,
    stored_len: StoredLen,
    region: Region,
    header: Header,
    name: Arc<str>,
    saved_stamped_changes: u16,
    phantom: PhantomData<I>,
}

impl<I, T> BaseVec<I, T>
where
    I: VecIndex,
    T: VecValue,
{
    /// Import or create a BaseVec from the database.
    pub fn import(options: ImportOptions, format: Format) -> Result<Self> {
        let region = options
            .db
            .create_region_if_needed(&vec_region_name_with::<I>(options.name))?;

        let region_len = region.meta().len();
        if region_len > 0 && region_len < HEADER_OFFSET {
            return Err(Error::CorruptedRegion { region_len });
        }

        let header = if region_len == 0 {
            Header::create_and_write(&region, options.version, format)?
        } else {
            Header::import_and_verify(&region, options.version, format)?
        };

        Ok(Self {
            region,
            header,
            name: Arc::from(options.name),
            pushed: WithPrev::default(),
            stored_len: StoredLen::default(),
            saved_stamped_changes: options.saved_stamped_changes,
            phantom: PhantomData,
        })
    }

    #[inline]
    pub fn region(&self) -> &Region {
        &self.region
    }

    #[inline]
    pub fn header(&self) -> &Header {
        &self.header
    }

    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[inline]
    pub fn pushed(&self) -> &[T] {
        self.pushed.current()
    }

    #[inline]
    pub fn mut_pushed(&mut self) -> &mut Vec<T> {
        self.pushed.current_mut()
    }

    #[inline]
    pub fn reserve_pushed(&mut self, additional: usize) {
        self.pushed.current_mut().reserve(additional);
    }

    #[inline]
    pub fn prev_pushed(&self) -> &[T] {
        self.pushed.previous()
    }

    #[inline]
    pub fn stored_len(&self) -> usize {
        self.stored_len.get()
    }

    #[inline]
    pub fn update_stored_len(&self, val: usize) {
        self.stored_len.set(val);
    }

    #[inline]
    pub fn prev_stored_len(&self) -> usize {
        self.stored_len.previous()
    }

    #[inline(always)]
    pub fn mut_prev_stored_len(&mut self) -> &mut usize {
        self.stored_len.previous_mut()
    }

    #[inline(always)]
    pub fn saved_stamped_changes(&self) -> u16 {
        self.saved_stamped_changes
    }

    #[inline(always)]
    pub fn version(&self) -> Version {
        self.header.vec_version()
    }

    #[inline]
    pub fn db(&self) -> Database {
        self.region.db()
    }

    #[inline]
    pub fn db_path(&self) -> PathBuf {
        self.region.db().path().to_path_buf()
    }

    #[inline]
    pub fn mut_header(&mut self) -> &mut Header {
        &mut self.header
    }

    pub fn write_header_if_needed(&mut self) -> Result<()> {
        if self.header.modified() {
            let r = self.region.clone();
            self.header.write(&r)?;
        }
        Ok(())
    }

    /// Removes this vector's region from the database
    pub fn remove(self) -> Result<()> {
        self.region.remove()?;
        Ok(())
    }

    /// Returns the region name for this vector (same as AnyVec::index_to_name)
    pub fn index_to_name(&self) -> String {
        vec_region_name(&self.name, I::to_string())
    }

    /// Total length: stored + pushed.
    #[inline]
    pub fn len(&self) -> usize {
        self.stored_len() + self.pushed().len()
    }

    /// Fold over pushed values in `[from, to)` — tight pointer loop for LLVM vectorization.
    #[inline]
    pub fn fold_pushed<B, F: FnMut(B, T) -> B>(
        &self,
        from: usize,
        to: usize,
        init: B,
        mut f: F,
    ) -> B {
        let stored_len = self.stored_len();
        let start = from.max(stored_len);
        if start >= to {
            return init;
        }
        let pushed = self.pushed();
        let slice_from = start - stored_len;
        let slice_to = (to - stored_len).min(pushed.len());
        let ptr = pushed.as_ptr();
        let mut acc = init;
        let mut i = slice_from;
        while i < slice_to {
            acc = f(acc, unsafe { ptr.add(i).read() });
            i += 1;
        }
        acc
    }

    /// Fallible fold over pushed values in `[from, to)` via direct slice access.
    #[inline]
    pub fn try_fold_pushed<B, E, F: FnMut(B, T) -> std::result::Result<B, E>>(
        &self,
        from: usize,
        to: usize,
        init: B,
        mut f: F,
    ) -> std::result::Result<B, E> {
        let stored_len = self.stored_len();
        let start = from.max(stored_len);
        if start >= to {
            return Ok(init);
        }
        let pushed = self.pushed();
        let mut acc = init;
        for v in &pushed[(start - stored_len)..(to - stored_len).min(pushed.len())] {
            acc = f(acc, v.clone())?;
        }
        Ok(acc)
    }

    /// Path to the changes directory for this vector.
    pub fn changes_path(&self) -> PathBuf {
        self.db_path()
            .join("changes")
            .join(vec_region_name(&self.name, I::to_string()))
    }

    /// Truncate pushed layer only. Returns true if stored_len needs updating to `index`.
    pub fn truncate_pushed(&mut self, index: usize) -> bool {
        let stored_len = self.stored_len();
        let len = stored_len + self.pushed().len();

        if index >= len {
            return false;
        }

        if index <= stored_len {
            self.pushed.current_mut().clear();
        } else {
            self.pushed.current_mut().truncate(index - stored_len);
        }

        index < stored_len
    }

    /// Full reset: clear pushed, zero stored_len, clear prev_, remove changes dir.
    pub fn reset_base(&mut self) -> Result<()> {
        self.pushed.clear();
        self.stored_len.set(0);
        *self.stored_len.previous_mut() = 0;
        self.header.update_stamp(Stamp::default());

        let changes_path = self.changes_path();
        if changes_path.exists() {
            fs::remove_dir_all(&changes_path)?;
        }

        Ok(())
    }

    /// Clear just the pushed buffer.
    pub fn reset_unsaved_base(&mut self) {
        self.pushed.current_mut().clear();
    }

    /// Serialize base change data. Caller provides strategy-specific callbacks.
    pub fn serialize_changes(
        &self,
        size_of_t: usize,
        collect_stored: impl FnOnce(usize, usize) -> Result<Vec<T>>,
        write_values: impl Fn(&[T], &mut Vec<u8>),
    ) -> Result<Vec<u8>> {
        let prev_stored_len = self.prev_stored_len();
        let stored_len = self.stored_len();
        let truncated = prev_stored_len.checked_sub(stored_len).unwrap_or_default();

        let value_count = truncated + self.prev_pushed().len() + self.pushed().len();
        let mut bytes = Vec::with_capacity(6 * SIZE_OF_U64 + value_count * size_of_t);

        bytes.extend(self.header.stamp().to_bytes());
        bytes.extend(prev_stored_len.to_bytes());
        bytes.extend(stored_len.to_bytes());
        bytes.extend(truncated.to_bytes());

        if truncated > 0 {
            let truncated_vals = collect_stored(stored_len, prev_stored_len)?;
            write_values(&truncated_vals, &mut bytes);
        }

        bytes.extend(self.prev_pushed().len().to_bytes());
        write_values(self.prev_pushed(), &mut bytes);

        bytes.extend(self.pushed().len().to_bytes());
        write_values(self.pushed(), &mut bytes);

        Ok(bytes)
    }

    /// Write change file to disk, prune old files.
    /// Caller must check `saved_stamped_changes > 0` before calling.
    pub fn save_change_file(&self, stamp: Stamp, data: &[u8]) -> Result<()> {
        debug_assert!(self.saved_stamped_changes > 0);
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
                .saturating_sub((self.saved_stamped_changes - 1) as usize),
        ) {
            fs::remove_file(path)?;
        }

        fs::write(path.join(u64::from(stamp).to_string()), data)?;
        Ok(())
    }

    /// Update prev_ fields after a successful write.
    pub fn save_prev(&mut self) {
        self.stored_len.save();
        self.pushed.previous_mut().clear();
    }

    /// Update prev_ fields after rollback.
    pub fn save_prev_for_rollback(&mut self) {
        self.stored_len.save();
        self.pushed.save();
    }

    /// Read the change file for the current stamp.
    pub fn read_current_change_file(&self) -> Result<Vec<u8>> {
        let path = self
            .changes_path()
            .join(u64::from(self.header.stamp()).to_string());
        Ok(fs::read(path)?)
    }

    /// Find rollback stamp files.
    pub fn find_rollback_files(&self) -> Result<BTreeMap<Stamp, PathBuf>> {
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
        Ok(dir)
    }

    /// Parse change file bytes. No &self needed.
    ///
    /// Returns `Error::Overflow` on arithmetic overflow,
    /// `Error::WrongLength` if the data is truncated.
    pub fn parse_change_data(
        bytes: &[u8],
        size_of_t: usize,
        read_value: impl Fn(&[u8]) -> Result<T>,
    ) -> Result<ChangeData<T>> {
        let mut pos = 0;
        let mut len = SIZE_OF_U64;

        fn check_bounds(bytes: &[u8], pos: usize, len: usize) -> Result<()> {
            let end = pos.checked_add(len).ok_or(Error::Overflow)?;
            if end > bytes.len() {
                return Err(Error::WrongLength {
                    received: bytes.len(),
                    expected: end,
                });
            }
            Ok(())
        }

        check_bounds(bytes, pos, len)?;
        let prev_stamp = Stamp::from_bytes(&bytes[pos..pos + len])?;
        pos += len;

        check_bounds(bytes, pos, len)?;
        let prev_stored_len = usize::from_bytes(&bytes[pos..pos + len])?;
        pos += len;

        // stored_len (skip, not needed for rollback)
        check_bounds(bytes, pos, len)?;
        pos += len;

        check_bounds(bytes, pos, len)?;
        let truncated_count = usize::from_bytes(&bytes[pos..pos + len])?;
        pos += len;

        let truncated_start = prev_stored_len
            .checked_sub(truncated_count)
            .ok_or(Error::Underflow)?;

        let truncated_values = if truncated_count > 0 {
            len = size_of_t.checked_mul(truncated_count).ok_or(Error::Overflow)?;
            check_bounds(bytes, pos, len)?;
            let vals = bytes[pos..pos + len]
                .chunks(size_of_t)
                .map(&read_value)
                .collect::<Result<Vec<_>>>()?;
            pos += len;
            vals
        } else {
            vec![]
        };

        len = SIZE_OF_U64;
        check_bounds(bytes, pos, len)?;
        let prev_pushed_len = usize::from_bytes(&bytes[pos..pos + len])?;
        pos += len;
        len = size_of_t.checked_mul(prev_pushed_len).ok_or(Error::Overflow)?;
        check_bounds(bytes, pos, len)?;
        let prev_pushed = bytes[pos..pos + len]
            .chunks(size_of_t)
            .map(&read_value)
            .collect::<Result<Vec<_>>>()?;
        pos += len;

        len = SIZE_OF_U64;
        check_bounds(bytes, pos, len)?;
        let pushed_len = usize::from_bytes(&bytes[pos..pos + len])?;
        pos += len;
        // Skip current pushed values (we already have prev_pushed)
        let skip = size_of_t.checked_mul(pushed_len).ok_or(Error::Overflow)?;
        check_bounds(bytes, pos, skip)?;
        pos += skip;

        Ok(ChangeData {
            prev_stamp,
            prev_stored_len,
            truncated_start,
            truncated_values,
            prev_pushed,
            bytes_consumed: pos,
        })
    }

    /// Apply base rollback: update stamp, stored_len, clear pushed, append prev_pushed.
    /// Caller handles type-specific truncation BEFORE calling this.
    pub fn apply_rollback(&mut self, data: &ChangeData<T>) {
        self.header.update_stamp(data.prev_stamp);
        self.stored_len.set(data.prev_stored_len);

        // Clone into current, then save() copies current→previous (reuses both allocations)
        self.pushed.current_mut().clone_from(&data.prev_pushed);
        self.pushed.save();
    }
}

/// Parsed change data returned by parse_change_data, consumed by apply_rollback.
#[derive(Debug)]
pub(crate) struct ChangeData<T> {
    pub prev_stamp: Stamp,
    pub prev_stored_len: usize,
    pub truncated_start: usize,
    pub truncated_values: Vec<T>,
    pub prev_pushed: Vec<T>,
    pub bytes_consumed: usize,
}

/// Returns the region name for the given vector name.
pub fn vec_region_name_with<I: VecIndex>(name: &str) -> String {
    vec_region_name(name, I::to_string())
}

/// Returns the region name for the given vector name.
pub fn vec_region_name(name: &str, index: &str) -> String {
    format!("{name}/{index}")
}
