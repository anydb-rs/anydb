use std::iter::FusedIterator;

use crate::{
    AnyStoredVec, GenericStoredVec, Result, TypedVecIterator, VecIndex, VecIterator, VecValue,
    likely, unlikely,
};

use super::{CleanRawVecIterator, RawStrategy, RawVecInner};

/// Dirty raw vec iterator, full-featured with holes/updates/pushed support
pub struct DirtyRawVecIterator<'a, I, T, S> {
    inner: CleanRawVecIterator<'a, I, T, S>,
    index: usize,
    stored_len: usize,
    pushed_len: usize,
    holes: bool,
    updated: bool,
}

impl<'a, I, T, S> DirtyRawVecIterator<'a, I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: RawStrategy<T>,
{
    const SIZE_OF_T: usize = size_of::<T>();

    pub fn new(vec: &'a RawVecInner<I, T, S>) -> Result<Self> {
        let holes = !vec.holes().is_empty();
        let updated = !vec.updated().is_empty();

        let stored_len = vec.stored_len();
        let pushed_len = vec.pushed().len();

        Ok(Self {
            inner: CleanRawVecIterator::new(vec)?,
            index: 0,
            stored_len,
            pushed_len,
            holes,
            updated,
        })
    }

    /// Skip one stored element without reading it (for holes/updates optimization).
    ///
    /// # Critical Ordering
    /// Refill buffer BEFORE advancing position. If we advance first, refill_buffer()
    /// will reset buffer_pos to 0, losing the skip. This is a subtle but critical bug.
    #[inline(always)]
    fn skip_stored_element(&mut self) {
        if unlikely(self.inner.cant_read_buffer()) && likely(self.inner.can_read_file()) {
            self.inner.refill_buffer();
        }
        self.inner.buffer_pos += Self::SIZE_OF_T;
    }

    #[inline(always)]
    fn remaining(&self) -> usize {
        (self.vec_len()) - self.index
    }

    #[inline(always)]
    fn vec_len(&self) -> usize {
        self.stored_len + self.pushed_len
    }

    /// Set the absolute end position for the iterator
    #[inline(always)]
    fn set_absolute_end(&mut self, absolute_end: usize) {
        let new_total_len = absolute_end.min(self.vec_len());
        let new_pushed_len = new_total_len.saturating_sub(self.stored_len);
        self.pushed_len = new_pushed_len;

        // Cap inner iterator if new end is within stored range
        if absolute_end <= self.stored_len {
            self.inner.set_end_to(absolute_end);
        }
    }
}

impl<I, T, S> Iterator for DirtyRawVecIterator<'_, I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: RawStrategy<T>,
{
    type Item = T;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let index = self.index;
            self.index += 1;

            if unlikely(self.holes) && self.inner._vec.holes().contains(&index) {
                if index < self.stored_len {
                    self.skip_stored_element();
                }
                continue;
            }

            if index >= self.stored_len {
                return self
                    .inner
                    ._vec
                    .get_pushed_at(index, self.stored_len)
                    .cloned();
            }

            if unlikely(self.updated)
                && let Some(updated) = self.inner._vec.updated().get(&index)
            {
                if index < self.stored_len {
                    self.skip_stored_element();
                }
                return Some(updated.clone());
            }

            return self.inner.next();
        }
    }

    #[inline]
    fn nth(&mut self, n: usize) -> Option<T> {
        if n == 0 {
            return self.next();
        }

        let new_index = self.index.saturating_add(n);
        if new_index >= self.vec_len() {
            self.index = self.vec_len();
            return None;
        }

        // Fast path: no holes or updates, can use optimized inner nth
        if !self.holes && !self.updated {
            if new_index < self.stored_len {
                // All skips are in stored data
                self.inner.nth(n - 1)?;
                self.index = new_index;
                return self.next();
            } else if self.index < self.stored_len {
                // Skip to end of stored, then into pushed
                let stored_skip = self.stored_len - self.index;
                if stored_skip > 0 {
                    self.inner.nth(stored_skip - 1);
                }
                self.index = new_index;
                return self.next();
            } else {
                // Already in pushed, just update index
                self.index = new_index;
                return self.next();
            }
        }

        // With holes/updates: use set_position_to for O(1) seek
        self.set_position_to(new_index);
        self.next()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.remaining();
        (remaining, Some(remaining))
    }

    #[inline]
    fn count(self) -> usize {
        self.len()
    }

    fn last(mut self) -> Option<T> {
        let last_index = self.vec_len().checked_sub(1)?;
        let skip = last_index.checked_sub(self.index)?;
        self.nth(skip)
    }
}

impl<I, T, S> VecIterator for DirtyRawVecIterator<'_, I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: RawStrategy<T>,
{
    #[inline]
    fn set_position_to(&mut self, i: usize) {
        self.index = i.min(self.vec_len());

        // Update inner iterator position if within stored range
        if i < self.stored_len {
            self.inner.set_position_to(i);
        }
    }

    #[inline]
    fn set_end_to(&mut self, i: usize) {
        self.set_absolute_end(i);
    }

    #[inline]
    fn vec_len(&self) -> usize {
        self.vec_len()
    }
}

impl<I, T, S> TypedVecIterator for DirtyRawVecIterator<'_, I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: RawStrategy<T>,
{
    type I = I;
    type T = T;
}

impl<I, T, S> ExactSizeIterator for DirtyRawVecIterator<'_, I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: RawStrategy<T>,
{
    #[inline(always)]
    fn len(&self) -> usize {
        self.remaining()
    }
}

impl<I, T, S> FusedIterator for DirtyRawVecIterator<'_, I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: RawStrategy<T>,
{
}
