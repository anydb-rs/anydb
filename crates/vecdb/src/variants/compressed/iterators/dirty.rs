use std::iter::FusedIterator;

use crate::{
    Compressable, CompressedVec, GenericStoredVec, Result, TypedVecIterator, VecIndex, VecIterator,
    likely,
};

use super::CleanCompressedVecIterator;

/// Dirty compressed vec iterator, handles pushed values on top of stored data
pub struct DirtyCompressedVecIterator<'a, I, T> {
    inner: CleanCompressedVecIterator<'a, I, T>,
    index: usize,
    pushed_len: usize,
}

impl<'a, I, T> DirtyCompressedVecIterator<'a, I, T>
where
    I: VecIndex,
    T: Compressable,
{
    pub fn new(vec: &'a CompressedVec<I, T>) -> Result<Self> {
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

impl<I, T> Iterator for DirtyCompressedVecIterator<'_, I, T>
where
    I: VecIndex,
    T: Compressable,
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
            .copied()
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
                .copied()
        }
    }
}

impl<I, T> VecIterator for DirtyCompressedVecIterator<'_, I, T>
where
    I: VecIndex,
    T: Compressable,
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

impl<I, T> TypedVecIterator for DirtyCompressedVecIterator<'_, I, T>
where
    I: VecIndex,
    T: Compressable,
{
    type I = I;
    type T = T;
}

impl<I, T> ExactSizeIterator for DirtyCompressedVecIterator<'_, I, T>
where
    I: VecIndex,
    T: Compressable,
{
    #[inline(always)]
    fn len(&self) -> usize {
        self.remaining()
    }
}

impl<I, T> FusedIterator for DirtyCompressedVecIterator<'_, I, T>
where
    I: VecIndex,
    T: Compressable,
{
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AnyStoredVec, CompressedVec, GenericStoredVec, Version};
    use rawdb::Database;
    use tempfile::TempDir;

    fn setup() -> (TempDir, Database, CompressedVec<usize, i32>) {
        let temp = TempDir::new().unwrap();
        let db = Database::open(&temp.path().join("test.db")).unwrap();
        let vec = CompressedVec::import(&db, "test", Version::ONE).unwrap();
        (temp, db, vec)
    }

    #[test]
    fn test_compressed_dirty_iter_only_stored() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..1000 {
            vec.push(i);
        }
        vec.write().unwrap();

        let collected: Vec<i32> = vec.dirty_iter().unwrap().collect();
        assert_eq!(collected.len(), 1000);
        assert_eq!(collected[0], 0);
        assert_eq!(collected[999], 999);
    }

    #[test]
    fn test_compressed_dirty_iter_only_pushed() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..500 {
            vec.push(i);
        }
        // Don't flush

        let collected: Vec<i32> = vec.dirty_iter().unwrap().collect();
        assert_eq!(collected.len(), 500);

        for (i, &val) in collected.iter().enumerate() {
            assert_eq!(val, i as i32);
        }
    }

    #[test]
    fn test_compressed_dirty_iter_stored_and_pushed() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..5000 {
            vec.push(i);
        }
        vec.write().unwrap();

        for i in 5000..10000 {
            vec.push(i);
        }

        let collected: Vec<i32> = vec.dirty_iter().unwrap().collect();
        assert_eq!(collected.len(), 10000);

        for (i, &val) in collected.iter().enumerate() {
            assert_eq!(val, i as i32);
        }
    }

    #[test]
    fn test_compressed_dirty_iter_skip_across_boundary() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..5000 {
            vec.push(i);
        }
        vec.write().unwrap();

        for i in 5000..10000 {
            vec.push(i);
        }

        // Skip from stored into pushed
        let collected: Vec<i32> = vec.dirty_iter().unwrap().skip(4500).collect();
        assert_eq!(collected.len(), 5500);
        assert_eq!(collected[0], 4500);
        assert_eq!(collected[5499], 9999);
    }

    #[test]
    fn test_compressed_dirty_iter_take_across_boundary() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..5000 {
            vec.push(i);
        }
        vec.write().unwrap();

        for i in 5000..10000 {
            vec.push(i);
        }

        // Take from stored through pushed
        let collected: Vec<i32> = vec.dirty_iter().unwrap().skip(4000).take(2000).collect();

        assert_eq!(collected.len(), 2000);
        assert_eq!(collected[0], 4000);
        assert_eq!(collected[1999], 5999);
    }

    #[test]
    fn test_compressed_dirty_iter_nth_across_boundary() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..5000 {
            vec.push(i);
        }
        vec.write().unwrap();

        for i in 5000..7000 {
            vec.push(i);
        }

        let mut iter = vec.dirty_iter().unwrap();
        assert_eq!(iter.nth(4990), Some(4990)); // In stored
        assert_eq!(iter.next(), Some(4991)); // In stored
        assert_eq!(iter.nth(7), Some(4999)); // In stored
        assert_eq!(iter.next(), Some(5000)); // In pushed
        assert_eq!(iter.next(), Some(5001)); // In pushed
    }

    #[test]
    fn test_compressed_dirty_iter_set_position_toto_pushed() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..5000 {
            vec.push(i);
        }
        vec.write().unwrap();

        for i in 5000..10000 {
            vec.push(i);
        }

        let mut iter = vec.dirty_iter().unwrap();
        iter.set_position_to(7500); // Into pushed region
        assert_eq!(iter.next(), Some(7500));
        assert_eq!(iter.next(), Some(7501));
    }

    #[test]
    fn test_compressed_dirty_iter_last_in_pushed() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..5000 {
            vec.push(i);
        }
        vec.write().unwrap();

        for i in 5000..10000 {
            vec.push(i);
        }

        let iter = vec.dirty_iter().unwrap();
        assert_eq!(iter.last(), Some(9999));
    }

    #[test]
    fn test_compressed_dirty_iter_last_in_stored() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..5000 {
            vec.push(i);
        }
        vec.write().unwrap();

        let iter = vec.dirty_iter().unwrap();
        assert_eq!(iter.last(), Some(4999));
    }

    #[test]
    fn test_compressed_dirty_iter_exact_size_with_pushed() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..5000 {
            vec.push(i);
        }
        vec.write().unwrap();

        for i in 5000..7500 {
            vec.push(i);
        }

        let mut iter = vec.dirty_iter().unwrap();
        assert_eq!(iter.len(), 7500);

        iter.next();
        assert_eq!(iter.len(), 7499);

        iter.nth(4999); // Cross boundary
        assert_eq!(iter.len(), 2499);
    }

    #[test]
    fn test_compressed_dirty_iter_large_dataset_boundary() {
        let (_temp, _db, mut vec) = setup();

        // Large stored portion
        for i in 0..10000 {
            vec.push(i);
        }
        vec.write().unwrap();

        // Small pushed portion
        for i in 10000..10100 {
            vec.push(i);
        }

        let collected: Vec<i32> = vec.dirty_iter().unwrap().collect();
        assert_eq!(collected.len(), 10100);

        for (i, &val) in collected.iter().enumerate() {
            assert_eq!(val, i as i32);
        }
    }

    #[test]
    fn test_compressed_dirty_iter_skip_take_complex() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..8000 {
            vec.push(i);
        }
        vec.write().unwrap();

        for i in 8000..12000 {
            vec.push(i);
        }

        // Complex skip/take across boundary
        let collected: Vec<i32> = vec
            .dirty_iter()
            .unwrap()
            .skip(7000)
            .take(3000)
            .skip(500)
            .take(1000)
            .collect();

        assert_eq!(collected.len(), 1000);
        assert_eq!(collected[0], 7500);
        assert_eq!(collected[999], 8499);
    }
}
