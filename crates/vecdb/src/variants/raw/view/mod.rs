use std::{iter::FusedIterator, marker::PhantomData, ops::Bound, ops::RangeBounds};

use rawdb::Reader;

use crate::{HEADER_OFFSET, TypedVecIterator, VecIndex, VecIterator, VecValue};

use super::RawStrategy;

/// Read-only mmap-backed view into a raw (uncompressed) vector.
///
/// Created via `vec.view()`. Provides fast random access and range iteration
/// directly from the memory-mapped file, without the overhead of creating
/// a reader for each access.
///
/// The data slice is computed once at construction time (matching Reader's
/// own `transmute` pattern), so `get()` / `iter()` / `range()` are direct
/// slice operations with no per-call overhead.
///
/// Only sees **stored** (persisted) values — pushed but unflushed values
/// are not visible.
///
/// Also implements `TypedVecIterator` so it can be returned from `iter_small_range`
/// as a `BoxedVecIterator`, providing ~6.6x faster small-range access vs buffered I/O.
pub struct RawVecView<I, T, S> {
    // SAFETY: Field order matters. `_reader` keeps the mmap guard alive.
    // `data` is a pointer into that mmap. `_reader` must outlive `data`,
    // which it does because `data` is a raw pointer with no destructor.
    _reader: Reader,
    data: *const u8,
    data_len: usize,
    stored_len: usize,
    pos: usize,
    end: usize,
    _marker: PhantomData<(I, T, S)>,
}

// SAFETY: RawVecView is read-only. The mmap data it points to is shared
// immutable memory protected by Reader's RwLockReadGuard, which is Sync.
unsafe impl<I: Send, T: Send, S: Send> Send for RawVecView<I, T, S> {}
unsafe impl<I: Sync, T: Sync, S: Sync> Sync for RawVecView<I, T, S> {}

impl<I, T, S> RawVecView<I, T, S>
where
    T: VecValue,
    S: RawStrategy<T>,
{
    const SIZE_OF_T: usize = size_of::<T>();

    pub(crate) fn new(reader: Reader, stored_len: usize) -> Self {
        let data_len = stored_len * Self::SIZE_OF_T;
        let slice = reader.prefixed(HEADER_OFFSET);
        let ptr = slice.as_ptr();

        Self {
            _reader: reader,
            data: ptr,
            data_len,
            stored_len,
            pos: 0,
            end: stored_len,
            _marker: PhantomData,
        }
    }

    /// Returns the pre-computed data slice covering all stored values.
    #[inline(always)]
    fn data(&self) -> &[u8] {
        // SAFETY: `data` points into `_reader`'s mmap which is alive for `&self`.
        // `data_len` was computed from `stored_len * SIZE_OF_T` at construction time
        // and is bounded by the region length (reader.prefixed already validated this).
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

    /// Returns `true` if the view is empty.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.stored_len == 0
    }

    /// Returns an iterator over all stored values.
    #[inline(always)]
    pub fn iter(&self) -> RawVecViewIter<'_, T, S> {
        RawVecViewIter {
            chunks: self.data().chunks_exact(Self::SIZE_OF_T),
            _marker: PhantomData,
        }
    }

    /// Returns an iterator over values in the given range.
    #[inline]
    pub fn range(&self, range: impl RangeBounds<usize>) -> RawVecViewIter<'_, T, S> {
        let start = match range.start_bound() {
            Bound::Included(&s) => s,
            Bound::Excluded(&s) => s + 1,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(&e) => (e + 1).min(self.stored_len),
            Bound::Excluded(&e) => e.min(self.stored_len),
            Bound::Unbounded => self.stored_len,
        };
        let start = start.min(end);

        let data = self.data();
        RawVecViewIter {
            chunks: data[start * Self::SIZE_OF_T..end * Self::SIZE_OF_T]
                .chunks_exact(Self::SIZE_OF_T),
            _marker: PhantomData,
        }
    }
}

// --- TypedVecIterator implementation for RawVecView ---
// Enables returning RawVecView as a BoxedVecIterator from iter_small_range.

impl<I, T, S> Iterator for RawVecView<I, T, S>
where
    T: VecValue,
    S: RawStrategy<T>,
{
    type Item = T;

    #[inline(always)]
    fn next(&mut self) -> Option<T> {
        if self.pos >= self.end {
            return None;
        }
        let val = self.try_get(self.pos)?;
        self.pos += 1;
        Some(val)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.end.saturating_sub(self.pos);
        (remaining, Some(remaining))
    }
}

impl<I, T, S> ExactSizeIterator for RawVecView<I, T, S>
where
    T: VecValue,
    S: RawStrategy<T>,
{
}

impl<I, T, S> FusedIterator for RawVecView<I, T, S>
where
    T: VecValue,
    S: RawStrategy<T>,
{
}

impl<I, T, S> VecIterator for RawVecView<I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: RawStrategy<T>,
{
    #[inline]
    fn set_position_to(&mut self, i: usize) {
        self.pos = i;
    }

    #[inline]
    fn set_end_to(&mut self, i: usize) {
        self.end = i;
    }

    #[inline]
    fn vec_len(&self) -> usize {
        self.stored_len
    }
}

impl<I, T, S> TypedVecIterator for RawVecView<I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: RawStrategy<T>,
{
    type I = I;
    type T = T;
}

/// Iterator over values in a [`RawVecView`].
///
/// Wraps `ChunksExact<u8>` over the mmap slice, deserializing each chunk
/// via the strategy's `read()`. Implements `DoubleEndedIterator` for
/// reverse iteration.
pub struct RawVecViewIter<'a, T, S> {
    chunks: std::slice::ChunksExact<'a, u8>,
    _marker: PhantomData<(T, S)>,
}

impl<T, S> Iterator for RawVecViewIter<'_, T, S>
where
    T: VecValue,
    S: RawStrategy<T>,
{
    type Item = T;

    #[inline(always)]
    fn next(&mut self) -> Option<T> {
        let chunk = self.chunks.next()?;
        Some(S::read(chunk).expect("Failed to deserialize value"))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.chunks.size_hint()
    }

    #[inline]
    fn count(self) -> usize {
        self.chunks.count()
    }

    #[inline(always)]
    fn nth(&mut self, n: usize) -> Option<T> {
        let chunk = self.chunks.nth(n)?;
        Some(S::read(chunk).expect("Failed to deserialize value"))
    }

    #[inline]
    fn last(self) -> Option<T> {
        let chunk = self.chunks.last()?;
        Some(S::read(chunk).expect("Failed to deserialize value"))
    }
}

impl<T, S> DoubleEndedIterator for RawVecViewIter<'_, T, S>
where
    T: VecValue,
    S: RawStrategy<T>,
{
    #[inline(always)]
    fn next_back(&mut self) -> Option<T> {
        let chunk = self.chunks.next_back()?;
        Some(S::read(chunk).expect("Failed to deserialize value"))
    }

    #[inline(always)]
    fn nth_back(&mut self, n: usize) -> Option<T> {
        let chunk = self.chunks.nth_back(n)?;
        Some(S::read(chunk).expect("Failed to deserialize value"))
    }
}

impl<T, S> ExactSizeIterator for RawVecViewIter<'_, T, S>
where
    T: VecValue,
    S: RawStrategy<T>,
{
    #[inline(always)]
    fn len(&self) -> usize {
        self.chunks.len()
    }
}

impl<T, S> FusedIterator for RawVecViewIter<'_, T, S>
where
    T: VecValue,
    S: RawStrategy<T>,
{
}
