use std::{
    collections::{BTreeMap, BTreeSet},
    marker::PhantomData,
    mem,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use log::info;
use rawdb::{Database, Reader, Region};
use zerocopy::{FromBytes, IntoBytes};

use crate::{
    AnyStoredVec, AnyVec, BUFFER_SIZE, BoxedVecIterator, Error, GenericStoredVec, IterableVec,
    Result, TypedVec, VecIndex, VecValue, Version,
};

use super::Format;

mod header;
mod iterators;
mod options;

pub use header::*;
pub use iterators::*;
pub use options::*;

const VERSION: Version = Version::ONE;

/// Raw storage vector that stores values as-is without compression.
///
/// This is the most basic storage format, writing values directly to disk
/// with minimal overhead. Ideal for random access patterns and data that
/// doesn't compress well.
#[derive(Debug, Clone)]
#[must_use = "Vector should be stored to keep data accessible"]
pub struct RawVec<I, T> {
    region: Region,

    header: Header,
    name: Arc<str>,
    prev_pushed: Vec<T>,
    pushed: Vec<T>,
    has_stored_holes: bool,
    holes: BTreeSet<usize>,
    prev_holes: BTreeSet<usize>,
    updated: BTreeMap<usize, T>,
    prev_updated: BTreeMap<usize, T>,
    prev_stored_len: usize,
    stored_len: Arc<AtomicUsize>,
    /// Default is 0
    saved_stamped_changes: u16,

    phantom: PhantomData<I>,
}

impl<I, T> RawVec<I, T>
where
    I: VecIndex,
    T: VecValue,
{
    /// Same as import but will reset the vec under certain errors, so be careful !
    pub fn forced_import(db: &Database, name: &str, version: Version) -> Result<Self> {
        Self::forced_import_with((db, name, version).into())
    }

    /// Same as import but will reset the vec under certain errors, so be careful !
    pub fn forced_import_with(options: ImportOptions) -> Result<Self> {
        let res = Self::import_with(options);
        match res {
            Err(Error::DifferentCompressionMode)
            | Err(Error::WrongEndian)
            | Err(Error::WrongLength)
            | Err(Error::DifferentVersion { .. }) => {
                info!("Resetting {}...", options.name);
                options
                    .db
                    .remove_region_if_exists(&Self::vec_region_name_with(options.name))?;
                options
                    .db
                    .remove_region_if_exists(&Self::holes_region_name_with(options.name))?;
                Self::import_with(options)
            }
            _ => res,
        }
    }

    pub fn import(db: &Database, name: &str, version: Version) -> Result<Self> {
        Self::import_with((db, name, version).into())
    }

    pub fn import_with(mut options: ImportOptions) -> Result<Self> {
        options.version = options.version + VERSION;
        Self::import_(options, Format::Raw)
    }

    #[doc(hidden)]
    pub fn import_(
        ImportOptions {
            db,
            name,
            version,
            saved_stamped_changes,
        }: ImportOptions,
        format: Format,
    ) -> Result<Self> {
        let region = db.create_region_if_needed(&Self::vec_region_name_with(name))?;

        let region_len = region.meta().len();
        if region_len > 0
            && (region_len < HEADER_OFFSET
                || (format.is_raw()
                    && !(region_len - HEADER_OFFSET).is_multiple_of(Self::SIZE_OF_T)))
        {
            return Err(Error::CorruptedRegion { region_len });
        }

        let header = if region_len == 0 {
            Header::create_and_write(&region, version, format)?
        } else {
            Header::import_and_verify(&region, version, format)?
        };

        let holes = if let Some(holes) = db.get_region(&Self::holes_region_name_with(name)) {
            Some(
                holes
                    .create_reader()
                    .read_all()
                    .chunks(size_of::<usize>())
                    .map(|b| -> Result<usize> { usize::read_from_bytes(b).map_err(|e| e.into()) })
                    .collect::<Result<BTreeSet<usize>>>()?,
            )
        } else {
            None
        };

        let mut this = Self {
            region: region.clone(),
            header,
            name: Arc::from(name),
            prev_pushed: vec![],
            pushed: vec![],
            has_stored_holes: holes.is_some(),
            holes: holes.clone().unwrap_or_default(),
            prev_holes: holes.unwrap_or_default(),
            updated: BTreeMap::new(),
            prev_updated: BTreeMap::new(),
            phantom: PhantomData,
            prev_stored_len: 0,
            stored_len: Arc::new(AtomicUsize::new(0)),
            saved_stamped_changes,
        };

        let len = this.real_stored_len();
        *this.mut_prev_stored_len() = len;
        this.update_stored_len(len);

        Ok(this)
    }

    #[inline]
    pub fn iter(&self) -> Result<RawVecIterator<'_, I, T>> {
        RawVecIterator::new(self)
    }

    #[inline]
    pub fn clean_iter(&self) -> Result<CleanRawVecIterator<'_, I, T>> {
        CleanRawVecIterator::new(self)
    }

    #[inline]
    pub fn dirty_iter(&self) -> Result<DirtyRawVecIterator<'_, I, T>> {
        DirtyRawVecIterator::new(self)
    }

    #[inline]
    pub fn boxed_iter(&self) -> Result<BoxedVecIterator<'_, I, T>> {
        Ok(Box::new(RawVecIterator::new(self)?))
    }

    pub fn write_header_if_needed(&mut self) -> Result<()> {
        if self.header.modified() {
            self.header.write(&self.region)?;
        }
        Ok(())
    }

    #[inline]
    pub fn prev_holes(&self) -> &BTreeSet<usize> {
        &self.prev_holes
    }

    #[inline]
    pub fn is_dirty(&self) -> bool {
        !self.is_pushed_empty() || !self.holes.is_empty() || !self.updated.is_empty()
    }

    /// Calculate optimal buffer size aligned to SIZE_OF_T
    #[inline]
    const fn aligned_buffer_size() -> usize {
        (BUFFER_SIZE / Self::SIZE_OF_T) * Self::SIZE_OF_T
    }

    /// Removes this vector and all its associated regions from the database
    pub fn remove(self) -> Result<()> {
        let db = self.region.db();
        let holes_region_name = self.holes_region_name();
        let has_stored_holes = self.has_stored_holes;

        // Remove main region
        self.region.remove()?;

        // Remove holes region if it exists
        if has_stored_holes {
            db.remove_region(&holes_region_name)?;
        }

        Ok(())
    }
}


impl<I, T> AnyVec for RawVec<I, T>
where
    I: VecIndex,
    T: VecValue,
{
    #[inline]
    fn version(&self) -> Version {
        self.header.vec_version()
    }

    #[inline]
    fn name(&self) -> &str {
        &self.name
    }

    #[inline]
    fn len(&self) -> usize {
        self.len_()
    }

    #[inline]
    fn index_type_to_string(&self) -> &'static str {
        I::to_string()
    }

    #[inline]
    fn value_type_to_size_of(&self) -> usize {
        size_of::<T>()
    }

    #[inline]
    fn region_names(&self) -> Vec<String> {
        vec![self.index_to_name()]
    }
}

impl<I, T> AnyStoredVec for RawVec<I, T>
where
    I: VecIndex,
    T: VecValue,
{
    #[inline]
    fn db_path(&self) -> PathBuf {
        self.region.db().path().to_path_buf()
    }

    #[inline]
    fn header(&self) -> &Header {
        &self.header
    }

    #[inline]
    fn mut_header(&mut self) -> &mut Header {
        &mut self.header
    }

    #[inline]
    fn saved_stamped_changes(&self) -> u16 {
        self.saved_stamped_changes
    }

    fn db(&self) -> Database {
        self.region.db()
    }

    #[inline]
    fn real_stored_len(&self) -> usize {
        (self.region.meta().len() - HEADER_OFFSET) / Self::SIZE_OF_T
    }

    #[inline]
    fn stored_len(&self) -> usize {
        self.stored_len.load(Ordering::Acquire)
    }

    fn write(&mut self) -> Result<()> {
        self.write_header_if_needed()?;

        let stored_len = self.stored_len();
        let pushed_len = self.pushed_len();
        let real_stored_len = self.real_stored_len();
        // After rollback, stored_len can be > real_stored_len (missing items are in updated map)
        let truncated = stored_len < real_stored_len;
        let expanded = stored_len > real_stored_len;
        let has_new_data = pushed_len != 0;
        let has_updated_data = !self.updated.is_empty();
        let has_holes = !self.holes.is_empty();
        let had_holes = self.has_stored_holes;

        if !truncated && !expanded && !has_new_data && !has_updated_data && !has_holes && !had_holes
        {
            return Ok(());
        }

        let from = stored_len * Self::SIZE_OF_T + HEADER_OFFSET;

        if has_new_data {
            self.region
                .truncate_write(from, mem::take(&mut self.pushed).as_bytes())?;
            self.update_stored_len(stored_len + pushed_len);
        } else if truncated {
            self.region.truncate(from)?;
        }

        if has_updated_data {
            let updated = mem::take(&mut self.updated);
            updated.into_iter().try_for_each(|(i, v)| -> Result<()> {
                let bytes = v.as_bytes();
                let at = (i * Self::SIZE_OF_T) + HEADER_OFFSET;
                self.region.write_at(bytes, at)?;
                Ok(())
            })?;
        }

        if has_holes {
            self.has_stored_holes = true;
            let holes = self
                .region
                .db()
                .create_region_if_needed(&self.holes_region_name())?;
            let bytes = self
                .holes
                .iter()
                .flat_map(|i| i.to_ne_bytes())
                .collect::<Vec<_>>();
            holes.truncate_write(0, &bytes)?;
        } else if had_holes {
            self.has_stored_holes = false;
            self.region.db().remove_region(&self.holes_region_name())?;
        }

        Ok(())
    }

    fn region(&self) -> &Region {
        &self.region
    }

    fn serialize_changes(&self) -> Result<Vec<u8>> {
        let mut bytes = vec![];
        let reader = self.create_reader();

        bytes.extend(self.stamp().as_bytes());

        // let real_stored_len = self.real_stored_len();
        let prev_stored_len = self.prev_stored_len();
        let stored_len = self.stored_len();

        bytes.extend(prev_stored_len.as_bytes());
        bytes.extend(stored_len.as_bytes());

        let truncated = prev_stored_len.checked_sub(stored_len).unwrap_or_default();
        bytes.extend(truncated.as_bytes());
        if truncated > 0 {
            let truncated_vals = (stored_len..prev_stored_len)
                .map(|i| {
                    // Prefer prev_updated, then read from disk
                    // Use unchecked_read_at since these indices may be beyond current logical length
                    self.prev_updated
                        .get(&i)
                        .cloned()
                        .unwrap_or_else(|| self.unchecked_read_at(i, &reader).unwrap())
                })
                .collect::<Vec<_>>();
            bytes.extend(truncated_vals.as_bytes());
        }

        bytes.extend(self.prev_pushed.len().as_bytes());
        bytes.extend(self.prev_pushed.iter().flat_map(|v| v.as_bytes()));

        bytes.extend(self.pushed.len().as_bytes());
        bytes.extend(self.pushed.iter().flat_map(|v| v.as_bytes()));

        let (prev_modified_indexes, prev_modified_values) = self
            .prev_updated
            .iter()
            .map(|(&i, v)| (i, v.clone()))
            .collect::<(Vec<_>, Vec<_>)>();
        bytes.extend(prev_modified_indexes.len().as_bytes());
        bytes.extend(prev_modified_indexes.as_bytes());
        bytes.extend(prev_modified_values.as_bytes());

        let (modified_indexes, modified_values) = self
            .updated
            .keys()
            .map(|&i| {
                // Prefer prev_updated values over disk values (for post-rollback state)
                // Use unchecked_read_at since after rollback, indices may be beyond current logical length
                let val = self
                    .prev_updated
                    .get(&i)
                    .cloned()
                    .unwrap_or_else(|| self.unchecked_read_at(i, &reader).unwrap());
                (i, val)
            })
            .collect::<(Vec<_>, Vec<_>)>();
        bytes.extend(modified_indexes.len().as_bytes());
        bytes.extend(modified_indexes.as_bytes());
        bytes.extend(modified_values.as_bytes());

        let prev_holes = self.prev_holes.iter().copied().collect::<Vec<_>>();
        bytes.extend(prev_holes.len().as_bytes());
        bytes.extend(prev_holes.as_bytes());

        let holes = self.holes.iter().copied().collect::<Vec<_>>();
        bytes.extend(holes.len().as_bytes());
        bytes.extend(holes.as_bytes());

        Ok(bytes)
    }
}

impl<I, T> GenericStoredVec<I, T> for RawVec<I, T>
where
    I: VecIndex,
    T: VecValue,
{
    #[inline(always)]
    fn unchecked_read_at(&self, index: usize, reader: &Reader) -> Result<T> {
        T::read_from_prefix(reader.prefixed((index * Self::SIZE_OF_T) + HEADER_OFFSET))
            .map(|(v, _)| v)
            .map_err(Error::from)
    }

    #[inline]
    fn pushed(&self) -> &[T] {
        self.pushed.as_slice()
    }
    #[inline]
    fn mut_pushed(&mut self) -> &mut Vec<T> {
        &mut self.pushed
    }
    #[inline]
    fn prev_pushed(&self) -> &[T] {
        self.prev_pushed.as_slice()
    }
    #[inline]
    fn mut_prev_pushed(&mut self) -> &mut Vec<T> {
        &mut self.prev_pushed
    }

    #[inline(always)]
    fn holes(&self) -> &BTreeSet<usize> {
        &self.holes
    }
    #[inline]
    fn mut_holes(&mut self) -> &mut BTreeSet<usize> {
        &mut self.holes
    }
    #[inline]
    fn prev_holes(&self) -> &BTreeSet<usize> {
        &self.prev_holes
    }
    #[inline]
    fn mut_prev_holes(&mut self) -> &mut BTreeSet<usize> {
        &mut self.prev_holes
    }

    fn prev_stored_len(&self) -> usize {
        self.prev_stored_len
    }
    fn mut_prev_stored_len(&mut self) -> &mut usize {
        &mut self.prev_stored_len
    }
    fn update_stored_len(&self, val: usize) {
        self.stored_len.store(val, Ordering::Release);
    }

    #[inline(always)]
    fn updated(&self) -> &BTreeMap<usize, T> {
        &self.updated
    }
    #[inline]
    fn mut_updated(&mut self) -> &mut BTreeMap<usize, T> {
        &mut self.updated
    }
    #[inline]
    fn prev_updated(&self) -> &BTreeMap<usize, T> {
        &self.prev_updated
    }
    #[inline]
    fn mut_prev_updated(&mut self) -> &mut BTreeMap<usize, T> {
        &mut self.prev_updated
    }

    fn reset(&mut self) -> Result<()> {
        self.clear()
    }
}

impl<'a, I, T> IntoIterator for &'a RawVec<I, T>
where
    I: VecIndex,
    T: VecValue,
{
    type Item = T;
    type IntoIter = RawVecIterator<'a, I, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter().expect("RawVecIter::new(self) to work")
    }
}

impl<I, T> IterableVec<I, T> for RawVec<I, T>
where
    I: VecIndex,
    T: VecValue,
{
    fn iter(&self) -> BoxedVecIterator<'_, I, T> {
        Box::new(self.into_iter())
    }
}

impl<I, T> TypedVec for RawVec<I, T>
where
    I: VecIndex,
    T: VecValue,
{
    type I = I;
    type T = T;
}
