mod any_vec;
mod readable;
mod typed;

use std::marker::PhantomData;

use crate::{
    Error, MMAP_CROSSOVER_BYTES, RawIoSource, RawMmapSource, ReadOnlyBaseVec, Result, Stamp,
    VecIndex, VecReader, VecValue,
};

use super::RawStrategy;

/// Lean read-only view of a raw vector (~40 bytes).
///
/// Carries only the fields needed for disk reads: region, shared length,
/// name/header metadata. No holes, no updated map, no pushed buffer,
/// no rollback state.
///
/// Created via [`ReadWriteRawVec::read_only_clone`].
#[derive(Debug, Clone)]
pub struct ReadOnlyRawVec<I, T, S> {
    pub(super) base: ReadOnlyBaseVec<I, T>,
    _strategy: PhantomData<S>,
}

impl<I, T, S> ReadOnlyRawVec<I, T, S> {
    pub(crate) fn new(base: ReadOnlyBaseVec<I, T>) -> Self {
        Self {
            base,
            _strategy: PhantomData,
        }
    }
}

impl<I, T, S> ReadOnlyRawVec<I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: RawStrategy<T>,
{
    pub(crate) fn region(&self) -> &rawdb::Region {
        self.base.region()
    }

    pub(crate) fn stored_len(&self) -> usize {
        self.base.stored_len()
    }

    #[inline]
    pub fn stamp(&self) -> Stamp {
        self.base.header().stamp()
    }

    pub fn reader(&self) -> VecReader<I, T, S> {
        VecReader::from_read_only(self)
    }

    #[inline]
    pub fn read_at_once(&self, index: usize) -> Result<T> {
        self.reader()
            .try_get(index)
            .ok_or_else(|| Error::IndexTooHigh {
                index,
                len: self.base.len(),
                name: self.base.name().to_string(),
            })
    }

    #[inline]
    pub fn read_once(&self, index: I) -> Result<T> {
        self.read_at_once(index.to_usize())
    }

    #[inline(always)]
    pub(super) fn fold_source<B, F: FnMut(B, T) -> B>(
        &self,
        from: usize,
        to: usize,
        len: usize,
        init: B,
        f: F,
    ) -> B {
        let range_bytes = (to - from) * size_of::<T>();
        if range_bytes > MMAP_CROSSOVER_BYTES {
            RawIoSource::<I, T, S>::new_from_parts(self.base.region(), len, from, to).fold(init, f)
        } else {
            RawMmapSource::<I, T, S>::new_from_parts(self.base.region(), len, from, to)
                .fold(init, f)
        }
    }

    #[inline(always)]
    pub(super) fn try_fold_source<B, E, F: FnMut(B, T) -> std::result::Result<B, E>>(
        &self,
        from: usize,
        to: usize,
        len: usize,
        init: B,
        f: F,
    ) -> std::result::Result<B, E> {
        let range_bytes = (to - from) * size_of::<T>();
        if range_bytes > MMAP_CROSSOVER_BYTES {
            RawIoSource::<I, T, S>::new_from_parts(self.base.region(), len, from, to)
                .try_fold(init, f)
        } else {
            RawMmapSource::<I, T, S>::new_from_parts(self.base.region(), len, from, to)
                .try_fold(init, f)
        }
    }
}
