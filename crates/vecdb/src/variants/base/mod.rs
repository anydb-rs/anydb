use std::{
    marker::PhantomData,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use rawdb::{Database, Region};

use crate::{Error, Result, VecIndex, VecValue, Version};

mod format;
mod header;
mod options;

pub use format::*;
pub use header::*;
pub use options::*;

/// Base storage vector with fields common to all stored vector implementations.
///
/// Holds the core state shared across BytesVec, ZeroCopyVec, and compressed variants:
/// region storage, header metadata, pushed values, and length tracking.
#[derive(Debug, Clone)]
pub(crate) struct BaseVec<I, T> {
    region: Region,
    header: Header,
    name: Arc<str>,
    prev_pushed: Vec<T>,
    pushed: Vec<T>,
    prev_stored_len: usize,
    stored_len: Arc<AtomicUsize>,
    /// Default is 0
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

        let mut base = Self {
            region,
            header,
            name: Arc::from(options.name),
            prev_pushed: vec![],
            pushed: vec![],
            prev_stored_len: 0,
            stored_len: Arc::new(AtomicUsize::new(0)),
            saved_stamped_changes: 0,
            phantom: PhantomData,
        };

        base.saved_stamped_changes = options.saved_stamped_changes;

        Ok(base)
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
        &self.pushed
    }

    #[inline]
    pub fn mut_pushed(&mut self) -> &mut Vec<T> {
        &mut self.pushed
    }

    #[inline]
    pub fn prev_pushed(&self) -> &[T] {
        &self.prev_pushed
    }

    #[inline]
    pub fn mut_prev_pushed(&mut self) -> &mut Vec<T> {
        &mut self.prev_pushed
    }

    #[inline]
    pub fn stored_len(&self) -> usize {
        self.stored_len.load(Ordering::SeqCst)
    }

    #[inline]
    pub fn update_stored_len(&self, val: usize) {
        self.stored_len.store(val, Ordering::SeqCst);
    }

    #[inline]
    pub fn prev_stored_len(&self) -> usize {
        self.prev_stored_len
    }

    #[inline(always)]
    pub fn mut_prev_stored_len(&mut self) -> &mut usize {
        &mut self.prev_stored_len
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
}

/// Returns the region name for the given vector name.
pub fn vec_region_name_with<I: VecIndex>(name: &str) -> String {
    vec_region_name(name, I::to_string())
}

/// Returns the region name for the given vector name.
pub fn vec_region_name(name: &str, index: &str) -> String {
    format!("{name}/{index}")
}
