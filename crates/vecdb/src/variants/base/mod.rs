use std::{marker::PhantomData, path::PathBuf, sync::Arc};

use rawdb::{Database, Region};

use crate::{Error, Result, VecIndex, VecValue, Version};

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
    region: Region,
    header: Header,
    name: Arc<str>,
    pushed: WithPrev<Vec<T>>,
    stored_len: StoredLen,
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
    pub fn mut_prev_pushed(&mut self) -> &mut Vec<T> {
        self.pushed.previous_mut()
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
}

/// Returns the region name for the given vector name.
pub fn vec_region_name_with<I: VecIndex>(name: &str) -> String {
    vec_region_name(name, I::to_string())
}

/// Returns the region name for the given vector name.
pub fn vec_region_name(name: &str, index: &str) -> String {
    format!("{name}/{index}")
}
