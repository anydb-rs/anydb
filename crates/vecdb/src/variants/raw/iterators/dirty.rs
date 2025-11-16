use std::iter::FusedIterator;

use crate::{
    AnyStoredVec, GenericStoredVec, RawVec, Result, TypedVecIterator, VecIndex, VecIterator,
    VecValue, likely, unlikely,
};

use super::CleanRawVecIterator;

/// Dirty raw vec iterator, full-featured with holes/updates/pushed support
pub struct DirtyRawVecIterator<'a, I, T> {
    inner: CleanRawVecIterator<'a, I, T>,
    index: usize,
    stored_len: usize,
    pushed_len: usize,
    holes: bool,
    updated: bool,
}

impl<'a, I, T> DirtyRawVecIterator<'a, I, T>
where
    I: VecIndex,
    T: VecValue,
{
    const SIZE_OF_T: usize = size_of::<T>();

    pub fn new(vec: &'a RawVec<I, T>) -> Result<Self> {
        let holes = !vec.holes.is_empty();
        let updated = !vec.updated.is_empty();

        let stored_len = vec.stored_len();
        let pushed_len = vec.pushed_len();

        Ok(Self {
            inner: CleanRawVecIterator::new(vec)?,
            index: 0,
            stored_len,
            pushed_len,
            holes,
            updated,
        })
    }

    /// Skip one stored element without reading it (for holes/updates optimization)
    #[inline(always)]
    fn skip_stored_element(&mut self) {
        // Refill buffer BEFORE advancing position
        // Otherwise refill_buffer() resets buffer_pos to 0, losing the skip
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

impl<I, T> Iterator for DirtyRawVecIterator<'_, I, T>
where
    I: VecIndex,
    T: VecValue,
{
    type Item = T;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        let index = self.index;
        self.index += 1;

        if unlikely(self.holes) && self.inner._vec.holes().contains(&index) {
            if index < self.stored_len {
                self.skip_stored_element();
            }
            return self.next();
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

        self.inner.next()
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

        // Slow path: need to check each element for holes/updates
        for _ in 0..n {
            if self.index >= self.vec_len() {
                self.index = self.vec_len();
                return None;
            } else if self.index < self.stored_len {
                self.skip_stored_element();
            }
            self.index += 1;
        }
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
        self.nth(last_index - self.index)
    }
}

impl<I, T> VecIterator for DirtyRawVecIterator<'_, I, T>
where
    I: VecIndex,
    T: VecValue,
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

impl<I, T> TypedVecIterator for DirtyRawVecIterator<'_, I, T>
where
    I: VecIndex,
    T: VecValue,
{
    type I = I;
    type T = T;
}

impl<I, T> ExactSizeIterator for DirtyRawVecIterator<'_, I, T>
where
    I: VecIndex,
    T: VecValue,
{
    #[inline(always)]
    fn len(&self) -> usize {
        self.remaining()
    }
}

impl<I, T> FusedIterator for DirtyRawVecIterator<'_, I, T>
where
    I: VecIndex,
    T: VecValue,
{
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{GenericStoredVec, RawVec, Version};
    use rawdb::Database;
    use tempfile::TempDir;

    fn setup() -> (TempDir, Database, RawVec<usize, i32>) {
        let temp = TempDir::new().unwrap();
        let db = Database::open(&temp.path().join("test.db")).unwrap();
        let vec = RawVec::import(&db, "test", Version::ONE).unwrap();
        (temp, db, vec)
    }

    #[test]
    fn test_dirty_iter_only_stored() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..100 {
            vec.push(i);
        }
        vec.write().unwrap();

        let collected: Vec<i32> = vec.dirty_iter().unwrap().collect();
        assert_eq!(collected.len(), 100);
        assert_eq!(collected[0], 0);
        assert_eq!(collected[99], 99);
    }

    #[test]
    fn test_dirty_iter_only_pushed() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..50 {
            vec.push(i);
        }
        // Don't flush

        let collected: Vec<i32> = vec.dirty_iter().unwrap().collect();
        assert_eq!(collected.len(), 50);
        assert_eq!(collected[0], 0);
        assert_eq!(collected[49], 49);
    }

    #[test]
    fn test_dirty_iter_stored_and_pushed() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..50 {
            vec.push(i);
        }
        vec.write().unwrap();

        for i in 50..100 {
            vec.push(i);
        }

        let collected: Vec<i32> = vec.dirty_iter().unwrap().collect();
        assert_eq!(collected.len(), 100);

        for (i, &val) in collected.iter().enumerate() {
            assert_eq!(val, i as i32);
        }
    }

    #[test]
    fn test_dirty_iter_skip_across_boundary() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..50 {
            vec.push(i);
        }
        vec.write().unwrap();

        for i in 50..100 {
            vec.push(i);
        }

        // Skip from stored into pushed
        let collected: Vec<i32> = vec.dirty_iter().unwrap().skip(40).collect();
        assert_eq!(collected.len(), 60);
        assert_eq!(collected[0], 40);
        assert_eq!(collected[59], 99);
    }

    #[test]
    fn test_dirty_iter_take_across_boundary() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..50 {
            vec.push(i);
        }
        vec.write().unwrap();

        for i in 50..100 {
            vec.push(i);
        }

        // Take from stored through pushed
        let collected: Vec<i32> = vec.dirty_iter().unwrap().skip(40).take(20).collect();
        assert_eq!(collected.len(), 20);
        assert_eq!(collected[0], 40);
        assert_eq!(collected[19], 59);
    }

    #[test]
    fn test_dirty_iter_nth_across_boundary() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..50 {
            vec.push(i);
        }
        vec.write().unwrap();

        for i in 50..100 {
            vec.push(i);
        }

        let mut iter = vec.dirty_iter().unwrap();
        assert_eq!(iter.nth(45), Some(45)); // In stored
        assert_eq!(iter.next(), Some(46)); // In stored
        assert_eq!(iter.nth(2), Some(49)); // In stored
        assert_eq!(iter.next(), Some(50)); // In pushed
        assert_eq!(iter.next(), Some(51)); // In pushed
    }

    #[test]
    fn test_dirty_iter_set_position_to_pushed() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..50 {
            vec.push(i);
        }
        vec.write().unwrap();

        for i in 50..100 {
            vec.push(i);
        }

        let mut iter = vec.dirty_iter().unwrap();
        iter.set_position_to(75); // Into pushed region
        assert_eq!(iter.next(), Some(75));
        assert_eq!(iter.next(), Some(76));
    }

    #[test]
    fn test_dirty_iter_last_in_pushed() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..50 {
            vec.push(i);
        }
        vec.write().unwrap();

        for i in 50..100 {
            vec.push(i);
        }

        let iter = vec.dirty_iter().unwrap();
        assert_eq!(iter.last(), Some(99));
    }

    #[test]
    fn test_dirty_iter_last_in_stored() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..100 {
            vec.push(i);
        }
        vec.write().unwrap();

        let iter = vec.dirty_iter().unwrap();
        assert_eq!(iter.last(), Some(99));
    }

    #[test]
    fn test_dirty_iter_exact_size_with_pushed() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..50 {
            vec.push(i);
        }
        vec.write().unwrap();

        for i in 50..75 {
            vec.push(i);
        }

        let mut iter = vec.dirty_iter().unwrap();
        assert_eq!(iter.len(), 75);

        iter.next();
        assert_eq!(iter.len(), 74);

        iter.nth(49); // Cross boundary
        assert_eq!(iter.len(), 24);
    }

    #[test]
    fn test_dirty_iter_empty_stored_with_pushed() {
        let (_temp, _db, mut vec) = setup();

        // No flush, only pushed
        for i in 0..50 {
            vec.push(i);
        }

        let collected: Vec<i32> = vec.dirty_iter().unwrap().collect();
        assert_eq!(collected.len(), 50);

        for (i, &val) in collected.iter().enumerate() {
            assert_eq!(val, i as i32);
        }
    }
}
