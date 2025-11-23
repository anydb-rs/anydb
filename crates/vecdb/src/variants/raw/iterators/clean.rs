use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    iter::FusedIterator,
};

use parking_lot::RwLockReadGuard;
use rawdb::RegionMetadata;

use crate::{
    AnyStoredVec, GenericStoredVec, RawVec, Result, TypedVecIterator, VecIndex, VecIterator,
    VecValue, likely, unlikely, variants::HEADER_OFFSET,
};

/// Clean raw vec iterator, to read on disk data
pub struct CleanRawVecIterator<'a, I, T> {
    pub(crate) file: File,
    buffer: Vec<u8>,
    pub(crate) buffer_pos: usize,
    buffer_len: usize,
    file_offset: usize,
    end_offset: usize,
    start_offset: usize,
    pub(crate) _vec: &'a RawVec<I, T>,
    _lock: RwLockReadGuard<'a, RegionMetadata>,
}

impl<'a, I, T> CleanRawVecIterator<'a, I, T>
where
    I: VecIndex,
    T: VecValue,
{
    const SIZE_OF_T: usize = size_of::<T>();
    const NORMAL_BUFFER_SIZE: usize = RawVec::<I, T>::aligned_buffer_size();
    const _CHECK_T: () = assert!(Self::SIZE_OF_T > 0, "Can't have T with size_of() == 0");

    pub fn new(vec: &'a RawVec<I, T>) -> Result<Self> {
        let file = vec.region().open_db_read_only_file()?;

        let region_meta = vec.region().meta();
        let region_start = region_meta.start();
        let start_offset = region_start + HEADER_OFFSET;
        // Support truncated vecs
        let end_offset = region_start
            + (region_meta
                .len()
                .min(Self::index_to_bytes(vec.stored_len()) + HEADER_OFFSET));

        let mut this = Self {
            file,
            buffer: vec![0; Self::NORMAL_BUFFER_SIZE],
            buffer_pos: 0,
            buffer_len: 0,
            file_offset: start_offset,
            end_offset,
            start_offset,
            _vec: vec,
            _lock: region_meta,
        };

        this.seek(start_offset);

        Ok(this)
    }

    #[inline(always)]
    fn seek(&mut self, pos: usize) -> bool {
        self.file_offset = pos.min(self.end_offset).max(self.start_offset);
        self.buffer_pos = 0;
        self.buffer_len = 0;

        if likely(self.can_read_file()) {
            self.file
                .seek(SeekFrom::Start(self.file_offset as u64))
                .expect("Failed to seek to start position");
            true
        } else {
            false
        }
    }

    #[inline(always)]
    pub(crate) fn can_read_buffer(&self) -> bool {
        self.buffer_pos < self.buffer_len
    }

    #[inline(always)]
    pub(crate) fn cant_read_buffer(&self) -> bool {
        self.buffer_pos >= self.buffer_len
    }

    #[inline(always)]
    pub(crate) fn can_read_file(&self) -> bool {
        self.file_offset < self.end_offset
    }

    #[inline(always)]
    pub(crate) fn cant_read_file(&self) -> bool {
        self.file_offset >= self.end_offset
    }

    #[inline(always)]
    pub(crate) fn remaining_file_bytes(&self) -> usize {
        self.end_offset - self.file_offset
    }

    #[inline(always)]
    pub(crate) fn remaining_buffer_bytes(&self) -> usize {
        self.buffer_len - self.buffer_pos
    }

    #[inline(always)]
    pub(crate) fn remaining_bytes(&self) -> usize {
        self.remaining_file_bytes() + self.remaining_buffer_bytes()
    }

    #[inline(always)]
    pub(crate) fn remaining(&self) -> usize {
        self.remaining_bytes() / Self::SIZE_OF_T
    }

    #[inline(always)]
    pub(crate) fn refill_buffer(&mut self) {
        let buffer_len = self.remaining_file_bytes().min(Self::NORMAL_BUFFER_SIZE);

        self.file
            .read_exact(&mut self.buffer[..buffer_len])
            .expect("Failed to read file buffer");

        self.file_offset += buffer_len;
        self.buffer_len = buffer_len;
        self.buffer_pos = 0;
    }

    #[inline(always)]
    fn index_to_bytes(index: usize) -> usize {
        index.saturating_mul(Self::SIZE_OF_T)
    }

    #[inline(always)]
    fn skip_bytes(&mut self, skip_bytes: usize) -> bool {
        if skip_bytes == 0 {
            return true;
        }

        let buffer_remaining = self.remaining_buffer_bytes();
        if skip_bytes < buffer_remaining {
            // Fast path: skip within buffer
            self.buffer_pos += skip_bytes;
            true
        } else {
            // Slow path: seek file
            self.seek(
                self.file_offset
                    .saturating_add(skip_bytes - buffer_remaining),
            )
        }
    }
}

impl<I, T> Iterator for CleanRawVecIterator<'_, I, T>
where
    I: VecIndex,
    T: VecValue,
{
    type Item = T;

    #[inline(always)]
    fn next(&mut self) -> Option<T> {
        if likely(self.can_read_buffer()) {
            let value = unsafe {
                std::ptr::read_unaligned(self.buffer.as_ptr().add(self.buffer_pos) as *const T)
            };
            self.buffer_pos += Self::SIZE_OF_T;
            return Some(value);
        }

        if unlikely(self.cant_read_file()) {
            return None;
        }

        self.refill_buffer();

        self.buffer_pos = Self::SIZE_OF_T;
        Some(unsafe { std::ptr::read_unaligned(self.buffer.as_ptr() as *const T) })
    }

    #[inline]
    fn nth(&mut self, n: usize) -> Option<T> {
        if n == 0 {
            return self.next();
        }

        let skip_bytes = Self::index_to_bytes(n);
        if !self.skip_bytes(skip_bytes) {
            return None;
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

    #[inline]
    fn last(mut self) -> Option<T> {
        if unlikely(self.cant_read_file() || self.start_offset == self.end_offset) {
            return None;
        }

        self.seek(self.end_offset - Self::SIZE_OF_T);

        self.next()
    }
}

impl<I, T> VecIterator for CleanRawVecIterator<'_, I, T>
where
    I: VecIndex,
    T: VecValue,
{
    #[inline]
    fn set_position_to(&mut self, i: usize) {
        let target_offset = self.start_offset + Self::index_to_bytes(i);

        // Check if target is within current buffer
        if self.buffer_len > 0 {
            let buffer_start = self.file_offset - self.buffer_len;
            let buffer_end = self.file_offset;

            if target_offset >= buffer_start && target_offset < buffer_end {
                // Just adjust buffer position without seeking
                self.buffer_pos = target_offset - buffer_start;
                return;
            }
        }

        // Otherwise seek to new position
        self.seek(target_offset);
    }

    #[inline]
    fn set_end_to(&mut self, i: usize) {
        let byte_offset = self.start_offset + Self::index_to_bytes(i);
        self.end_offset = self.end_offset.min(byte_offset);
    }

    #[inline]
    fn vec_len(&self) -> usize {
        self._vec.len_()
    }
}

impl<I, T> TypedVecIterator for CleanRawVecIterator<'_, I, T>
where
    I: VecIndex,
    T: VecValue,
{
    type I = I;
    type T = T;
}

impl<I, T> ExactSizeIterator for CleanRawVecIterator<'_, I, T>
where
    I: VecIndex,
    T: VecValue,
{
    #[inline(always)]
    fn len(&self) -> usize {
        self.remaining()
    }
}

impl<I, T> FusedIterator for CleanRawVecIterator<'_, I, T>
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
    fn test_clean_iter_basic() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..100 {
            vec.push(i);
        }
        vec.write().unwrap();

        let collected: Vec<i32> = vec.clean_iter().unwrap().collect();
        assert_eq!(collected.len(), 100);
        assert_eq!(collected[0], 0);
        assert_eq!(collected[99], 99);
    }

    #[test]
    fn test_clean_iter_nth() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..100 {
            vec.push(i);
        }
        vec.write().unwrap();

        let mut iter = vec.clean_iter().unwrap();
        assert_eq!(iter.next(), Some(0));
        assert_eq!(iter.nth(9), Some(10));
        assert_eq!(iter.next(), Some(11));
    }

    #[test]
    fn test_clean_iter_skip() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..100 {
            vec.push(i);
        }
        vec.write().unwrap();

        let iter = vec.clean_iter().unwrap().skip(50);
        let collected: Vec<i32> = iter.collect();

        assert_eq!(collected.len(), 50);
        assert_eq!(collected[0], 50);
    }

    #[test]
    fn test_clean_iter_take() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..100 {
            vec.push(i);
        }
        vec.write().unwrap();

        let iter = vec.clean_iter().unwrap().take(25);
        let collected: Vec<i32> = iter.collect();

        assert_eq!(collected.len(), 25);
        assert_eq!(collected[24], 24);
    }

    #[test]
    fn test_clean_iter_set_position() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..100 {
            vec.push(i);
        }
        vec.write().unwrap();

        let mut iter = vec.clean_iter().unwrap();
        iter.set_position_to(50);
        assert_eq!(iter.next(), Some(50));
        assert_eq!(iter.next(), Some(51));
    }

    #[test]
    fn test_clean_iter_set_end() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..100 {
            vec.push(i);
        }
        vec.write().unwrap();

        let mut iter = vec.clean_iter().unwrap();
        iter.set_end_to(50);
        let collected: Vec<i32> = iter.collect();

        assert_eq!(collected.len(), 50);
    }

    #[test]
    fn test_clean_iter_last() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..100 {
            vec.push(i);
        }
        vec.write().unwrap();

        let iter = vec.clean_iter().unwrap();
        assert_eq!(iter.last(), Some(99));
    }

    #[test]
    fn test_clean_iter_last_empty() {
        let (_temp, _db, vec) = setup();

        let iter = vec.clean_iter().unwrap();
        assert_eq!(iter.last(), None);
    }

    #[test]
    fn test_clean_iter_exact_size() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..100 {
            vec.push(i);
        }
        vec.write().unwrap();

        let mut iter = vec.clean_iter().unwrap();
        assert_eq!(iter.len(), 100);

        iter.next();
        assert_eq!(iter.len(), 99);
    }

    #[test]
    fn test_clean_iter_buffer_crossing() {
        let (_temp, _db, mut vec) = setup();

        // Push enough to cross buffer boundaries
        for i in 0..10000 {
            vec.push(i);
        }
        vec.write().unwrap();

        let collected: Vec<i32> = vec.clean_iter().unwrap().collect();
        assert_eq!(collected.len(), 10000);

        for (i, &val) in collected.iter().enumerate() {
            assert_eq!(val, i as i32);
        }
    }

    #[test]
    fn test_clean_iter_multiple_skip_take() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..1000 {
            vec.push(i);
        }
        vec.write().unwrap();

        // Skip 100, take 200, skip 50 more, take 100 more
        let collected: Vec<i32> = vec
            .clean_iter()
            .unwrap()
            .skip(100)
            .take(200)
            .skip(50)
            .take(100)
            .collect();

        assert_eq!(collected.len(), 100);
        assert_eq!(collected[0], 150);
        assert_eq!(collected[99], 249);
    }

    #[test]
    fn test_clean_iter_set_position_multiple_times() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..1000 {
            vec.push(i);
        }
        vec.write().unwrap();

        let mut iter = vec.clean_iter().unwrap();

        iter.set_position_to(100);
        assert_eq!(iter.next(), Some(100));

        iter.set_position_to(500);
        assert_eq!(iter.next(), Some(500));

        iter.set_position_to(50);
        assert_eq!(iter.next(), Some(50));
    }

    #[test]
    fn test_clean_iter_skip_all() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..100 {
            vec.push(i);
        }
        vec.write().unwrap();

        let mut iter = vec.clean_iter().unwrap().skip(100);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_clean_iter_take_zero() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..100 {
            vec.push(i);
        }
        vec.write().unwrap();

        let collected: Vec<i32> = vec.clean_iter().unwrap().take(0).collect();
        assert_eq!(collected.len(), 0);
    }

    #[test]
    fn test_clean_iter_nth_beyond_end() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..10 {
            vec.push(i);
        }
        vec.write().unwrap();

        let mut iter = vec.clean_iter().unwrap();
        assert_eq!(iter.nth(20), None);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_clean_iter_size_hint_consistency() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..100 {
            vec.push(i);
        }
        vec.write().unwrap();

        let mut iter = vec.clean_iter().unwrap();

        for i in 0..100 {
            let (lower, upper) = iter.size_hint();
            assert_eq!(lower, 100 - i);
            assert_eq!(upper, Some(100 - i));
            assert_eq!(iter.len(), 100 - i);
            iter.next();
        }
    }
}
