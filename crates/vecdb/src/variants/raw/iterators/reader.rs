use std::marker::PhantomData;

use rawdb::Reader;

use crate::{AnyStoredVec, HEADER_OFFSET, VecIndex, VecValue};

use super::super::{RawStrategy, RawVecInner};

/// Read-only random-access handle into a raw vector's stored data.
///
/// Created via `raw_vec.reader()` (available on BytesVec/ZeroCopyVec via Deref).
/// Provides O(1) point reads directly from the memory-mapped file.
///
/// Only sees **stored** (persisted) values — does not check holes, updates,
/// or pushed values. For full dirty-state reads, use `get_any_or_read`.
pub struct VecReader<I, T, S> {
    _reader: Reader,
    data: *const u8,
    data_len: usize,
    stored_len: usize,
    _marker: PhantomData<(I, T, S)>,
}

unsafe impl<I: Send, T: Send, S: Send> Send for VecReader<I, T, S> {}
unsafe impl<I: Sync, T: Sync, S: Sync> Sync for VecReader<I, T, S> {}

impl<I, T, S> VecReader<I, T, S>
where
    T: VecValue,
    S: RawStrategy<T>,
{
    const SIZE_OF_T: usize = size_of::<T>();

    pub fn new(vec: &RawVecInner<I, T, S>) -> Self
    where
        I: VecIndex,
    {
        let reader = vec.region().create_reader();
        let stored_len = vec.stored_len();
        let data_len = stored_len * Self::SIZE_OF_T;
        let slice = reader.prefixed(HEADER_OFFSET);
        let ptr = slice.as_ptr();

        Self {
            _reader: reader,
            data: ptr,
            data_len,
            stored_len,
            _marker: PhantomData,
        }
    }

    /// Returns the pre-computed data slice covering all stored values.
    #[inline(always)]
    fn data(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data, self.data_len) }
    }

    /// Returns the value at `index`.
    ///
    /// # Panics
    /// Panics if `index >= len()`.
    #[inline(always)]
    pub fn get(&self, index: usize) -> T {
        assert!(
            index < self.stored_len,
            "index {index} out of bounds (len {})",
            self.stored_len
        );
        let offset = index * Self::SIZE_OF_T;
        S::read(&self.data()[offset..offset + Self::SIZE_OF_T])
            .expect("Failed to deserialize value")
    }

    /// Returns the value at `index`, or `None` if out of bounds.
    #[inline(always)]
    pub fn try_get(&self, index: usize) -> Option<T> {
        if index >= self.stored_len {
            return None;
        }
        let offset = index * Self::SIZE_OF_T;
        Some(
            S::read(&self.data()[offset..offset + Self::SIZE_OF_T])
                .expect("Failed to deserialize value"),
        )
    }

    /// Returns the number of stored values.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.stored_len
    }

    /// Returns `true` if the reader is empty.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.stored_len == 0
    }
}
