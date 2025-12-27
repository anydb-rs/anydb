use std::iter::FusedIterator;

use crate::{GenericStoredVec, Result, TypedVecIterator, VecIndex, VecIterator, VecValue, likely};

use super::{super::inner::{CompressedVecInner, CompressionStrategy}, CleanCompressedVecIterator};

/// Dirty compressed vec iterator, handles pushed values on top of stored data
pub struct DirtyCompressedVecIterator<'a, I, T, S> {
    inner: CleanCompressedVecIterator<'a, I, T, S>,
    index: usize,
    pushed_len: usize,
}

impl<'a, I, T, S> DirtyCompressedVecIterator<'a, I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: CompressionStrategy<T>,
{
    pub fn new(vec: &'a CompressedVecInner<I, T, S>) -> Result<Self> {
        let pushed_len = vec.pushed_len();

        Ok(Self {
            inner: CleanCompressedVecIterator::new(vec)?,
            index: 0,
            pushed_len,
        })
    }

    #[inline(always)]
    fn remaining(&self) -> usize {
        self.vec_len() - self.index
    }

    #[inline(always)]
    fn vec_len(&self) -> usize {
        self.inner.stored_len + self.pushed_len
    }

    /// Set the absolute end position for the iterator
    #[inline(always)]
    fn set_absolute_end(&mut self, absolute_end: usize) {
        let new_total_len = absolute_end.min(self.vec_len());
        let new_pushed_len = new_total_len.saturating_sub(self.inner.stored_len);
        self.pushed_len = new_pushed_len;

        // Cap inner iterator if new end is within stored range
        if absolute_end <= self.inner.stored_len {
            self.inner.set_end_to(absolute_end);
        }
    }
}

impl<I, T, S> Iterator for DirtyCompressedVecIterator<'_, I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: CompressionStrategy<T>,
{
    type Item = T;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        let index = self.index;
        self.index += 1;

        if likely(index < self.inner.stored_len) {
            return self.inner.next();
        }

        self.inner
            ._vec
            .get_pushed_at(index, self.inner.stored_len)
            .cloned()
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

        // Skip elements in the inner iterator if we're still in the stored range
        if self.index < self.inner.stored_len {
            let skip_in_stored = (new_index.min(self.inner.stored_len)) - self.index;
            if skip_in_stored > 0 {
                self.inner.nth(skip_in_stored - 1)?;
            }
        }

        self.index = new_index;
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

    fn last(self) -> Option<T> {
        let last_index = self.vec_len().checked_sub(1)?;

        if last_index < self.inner.stored_len {
            // Last element is in stored data
            self.inner.last()
        } else {
            // Last element is in pushed data
            self.inner
                ._vec
                .get_pushed_at(last_index, self.inner.stored_len)
                .cloned()
        }
    }
}

impl<I, T, S> VecIterator for DirtyCompressedVecIterator<'_, I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: CompressionStrategy<T>,
{
    #[inline]
    fn set_position_to(&mut self, i: usize) {
        self.index = i.min(self.vec_len());

        // Update inner iterator position if within stored range
        if i < self.inner.stored_len {
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

impl<I, T, S> TypedVecIterator for DirtyCompressedVecIterator<'_, I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: CompressionStrategy<T>,
{
    type I = I;
    type T = T;
}

impl<I, T, S> ExactSizeIterator for DirtyCompressedVecIterator<'_, I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: CompressionStrategy<T>,
{
    #[inline(always)]
    fn len(&self) -> usize {
        self.remaining()
    }
}

impl<I, T, S> FusedIterator for DirtyCompressedVecIterator<'_, I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: CompressionStrategy<T>,
{
}
