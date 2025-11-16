use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    iter::FusedIterator,
};

use parking_lot::RwLockReadGuard;
use rawdb::RegionMetadata;

use crate::{
    AnyStoredVec, BUFFER_SIZE, Compressable, CompressedVec, GenericStoredVec, Result,
    TypedVecIterator, VecIndex, VecIterator, likely, unlikely,
    variants::MAX_UNCOMPRESSED_PAGE_SIZE,
};

use super::super::pages::Pages;

/// Clean compressed vec iterator, for reading stored compressed data
/// Uses dedicated file handle for sequential reads (better OS readahead than mmap)
pub struct CleanCompressedVecIterator<'a, I, T> {
    pub(crate) _vec: &'a CompressedVec<I, T>,
    file: File,         // Dedicated file handle for sequential reads
    file_position: u64, // Current position in the file
    // Compressed data buffer (to reduce syscalls)
    buffer: Vec<u8>,
    buffer_len: usize,
    buffer_page_start: usize, // First page index that buffer contains
    // Decoded page cache
    decoded_values: Vec<T>,
    decoded_page_index: usize, // usize::MAX means no page decoded
    decoded_len: usize,
    pages: RwLockReadGuard<'a, Pages>,
    pub(crate) stored_len: usize,
    index: usize,
    end_index: usize,
    _region_lock: RwLockReadGuard<'a, RegionMetadata>,
}

impl<'a, I, T> CleanCompressedVecIterator<'a, I, T>
where
    I: VecIndex,
    T: Compressable,
{
    const SIZE_OF_T: usize = size_of::<T>();
    const PER_PAGE: usize = MAX_UNCOMPRESSED_PAGE_SIZE / Self::SIZE_OF_T;
    const NO_PAGE: usize = usize::MAX;

    pub fn new(vec: &'a CompressedVec<I, T>) -> Result<Self> {
        let file = vec.inner.region().open_db_read_only_file()?;
        let region_lock = vec.inner.region().meta();

        let pages = vec.pages.read();
        let stored_len = vec.stored_len();

        Ok(Self {
            _vec: vec,
            file,
            file_position: 0,
            buffer: vec![0; BUFFER_SIZE],
            buffer_len: 0,
            buffer_page_start: 0,
            decoded_values: Vec::with_capacity(Self::PER_PAGE),
            decoded_page_index: Self::NO_PAGE,
            decoded_len: 0,
            pages,
            stored_len,
            index: 0,
            end_index: stored_len,
            _region_lock: region_lock,
        })
    }

    #[inline(always)]
    fn remaining(&self) -> usize {
        self.end_index.saturating_sub(self.index)
    }

    #[inline(always)]
    fn has_decoded_page(&self) -> bool {
        self.decoded_page_index != Self::NO_PAGE
    }

    #[inline(always)]
    fn clear_decoded_page(&mut self) {
        self.decoded_page_index = Self::NO_PAGE;
        self.decoded_len = 0;
    }

    /// Set the absolute end position, capped at stored_len and current end_index
    #[inline(always)]
    fn set_absolute_end(&mut self, absolute_end: usize) {
        self.end_index = absolute_end.min(self.stored_len).min(self.end_index);
    }

    /// Refill buffer starting from a specific page
    #[inline(always)]
    fn refill_buffer(&mut self, starting_page_index: usize) -> Option<()> {
        self.buffer_page_start = starting_page_index;

        let start_page = self.pages.get(starting_page_index)?;
        let start_offset = start_page.start;

        // Calculate the last page we need based on end_index
        let last_needed_page = if self.end_index == 0 {
            0
        } else {
            (self.end_index - 1) / Self::PER_PAGE
        };
        let max_page = last_needed_page.min(self.pages.len().saturating_sub(1));

        // Calculate how many pages we can fit in the buffer (respecting end_index)
        let mut total_bytes = 0usize;

        for i in starting_page_index..=max_page {
            let page = self.pages.get(i)?;
            let page_bytes = page.bytes as usize;

            if total_bytes + page_bytes > BUFFER_SIZE {
                break;
            }

            total_bytes += page_bytes;
        }

        if total_bytes == 0 {
            return None;
        }

        if self.file_position != start_offset {
            self.file_position = start_offset;
            self.file.seek(SeekFrom::Start(start_offset)).unwrap();
        }

        self.file
            .read_exact(&mut self.buffer[..total_bytes])
            .unwrap();
        self.buffer_len = total_bytes;

        Some(())
    }

    /// Helper to decompress a page from buffer (page metadata already fetched)
    #[inline(always)]
    fn decompress_from_buffer(
        &mut self,
        page_index: usize,
        compressed_offset: u64,
        compressed_size: usize,
        values_count: usize,
    ) -> Option<()> {
        let buffer_start_page = self.pages.get(self.buffer_page_start)?;
        let buffer_start_offset = buffer_start_page.start;
        let in_buffer_offset = (compressed_offset - buffer_start_offset) as usize;
        let compressed_data = &self.buffer[in_buffer_offset..in_buffer_offset + compressed_size];

        self.decoded_values =
            CompressedVec::<I, T>::decompress_bytes(compressed_data, values_count).ok()?;
        self.decoded_page_index = page_index;
        self.decoded_len = self.decoded_values.len();

        Some(())
    }

    /// Decode a specific page from buffer (or read more data if needed)
    fn decode_page(&mut self, page_index: usize) -> Option<()> {
        if page_index >= self.pages.len() {
            return None;
        }

        // Fetch page metadata once
        let page = self.pages.get(page_index)?;
        let compressed_size = page.bytes as usize;
        let compressed_offset = page.start;
        let values_count = page.values as usize;

        // Check if page data is already in buffer
        if self.buffer_len > 0 {
            let buffer_start_page = self.pages.get(self.buffer_page_start)?;
            let buffer_start_offset = buffer_start_page.start;
            let buffer_end_offset = buffer_start_offset + self.buffer_len as u64;

            if compressed_offset >= buffer_start_offset
                && compressed_offset + compressed_size as u64 <= buffer_end_offset
            {
                // Page is in buffer, decompress it
                return self.decompress_from_buffer(
                    page_index,
                    compressed_offset,
                    compressed_size,
                    values_count,
                );
            }
        }

        // Page not in buffer, refill starting from this page
        self.refill_buffer(page_index)?;

        // Now decompress from the newly filled buffer
        self.decompress_from_buffer(page_index, compressed_offset, compressed_size, values_count)
    }
}

impl<I, T> Iterator for CleanCompressedVecIterator<'_, I, T>
where
    I: VecIndex,
    T: Compressable,
{
    type Item = T;

    #[inline(always)]
    fn next(&mut self) -> Option<T> {
        let index = self.index;

        if unlikely(index >= self.end_index) {
            return None;
        }

        self.index += 1;

        let page_index = index / Self::PER_PAGE;
        let in_page_index = index % Self::PER_PAGE;

        // Fast path: read from current decoded page
        if likely(self.has_decoded_page() && self.decoded_page_index == page_index) {
            return self.decoded_values.get(in_page_index).copied();
        }

        // Slow path: decode new page
        self.decode_page(page_index)?;
        self.decoded_values.get(in_page_index).copied()
    }

    #[inline]
    fn nth(&mut self, n: usize) -> Option<T> {
        if n == 0 {
            return self.next();
        }

        let new_index = self.index.saturating_add(n);
        if new_index >= self.end_index {
            self.index = self.end_index;
            return None;
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

    #[inline]
    fn last(mut self) -> Option<T> {
        if unlikely(self.index >= self.end_index) {
            return None;
        }

        self.index = self.end_index - 1;
        self.next()
    }
}

impl<I, T> VecIterator for CleanCompressedVecIterator<'_, I, T>
where
    I: VecIndex,
    T: Compressable,
{
    #[inline]
    fn set_position_to(&mut self, i: usize) {
        let new_index = i.min(self.stored_len).min(self.end_index);

        // Check if new position is within the currently decoded page
        if self.has_decoded_page() {
            let page_start = self.decoded_page_index * Self::PER_PAGE;
            let page_end = page_start + Self::PER_PAGE;

            if new_index >= page_start && new_index < page_end {
                // Keep decoded page, just update index
                self.index = new_index;
                return;
            }
        }

        // New position is outside current page, invalidate cache
        self.clear_decoded_page();
        self.index = new_index;
    }

    #[inline]
    fn set_end_to(&mut self, i: usize) {
        self.set_absolute_end(i);
    }

    #[inline]
    fn vec_len(&self) -> usize {
        self._vec.len_()
    }
}

impl<I, T> TypedVecIterator for CleanCompressedVecIterator<'_, I, T>
where
    I: VecIndex,
    T: Compressable,
{
    type I = I;
    type T = T;
}

impl<I, T> ExactSizeIterator for CleanCompressedVecIterator<'_, I, T>
where
    I: VecIndex,
    T: Compressable,
{
    #[inline(always)]
    fn len(&self) -> usize {
        self.remaining()
    }
}

impl<I, T> FusedIterator for CleanCompressedVecIterator<'_, I, T>
where
    I: VecIndex,
    T: Compressable,
{
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CompressedVec, Version};
    use rawdb::Database;
    use tempfile::TempDir;

    fn setup() -> (TempDir, Database, CompressedVec<usize, i32>) {
        let temp = TempDir::new().unwrap();
        let db = Database::open(&temp.path().join("test.db")).unwrap();
        let vec = CompressedVec::import(&db, "test", Version::ONE).unwrap();
        (temp, db, vec)
    }

    #[test]
    fn test_compressed_clean_iter_basic() {
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
    fn test_compressed_clean_iter_large() {
        let (_temp, _db, mut vec) = setup();

        // Push enough to span multiple pages
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
    fn test_compressed_clean_iter_nth() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..1000 {
            vec.push(i);
        }
        vec.write().unwrap();

        let mut iter = vec.clean_iter().unwrap();
        assert_eq!(iter.next(), Some(0));
        assert_eq!(iter.nth(99), Some(100));
        assert_eq!(iter.next(), Some(101));
    }

    #[test]
    fn test_compressed_clean_iter_skip_across_pages() {
        let (_temp, _db, mut vec) = setup();

        // Push enough to span multiple pages
        for i in 0..10000 {
            vec.push(i);
        }
        vec.write().unwrap();

        let iter = vec.clean_iter().unwrap().skip(5000);
        let collected: Vec<i32> = iter.collect();

        assert_eq!(collected.len(), 5000);
        assert_eq!(collected[0], 5000);
        assert_eq!(collected[4999], 9999);
    }

    #[test]
    fn test_compressed_clean_iter_take() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..1000 {
            vec.push(i);
        }
        vec.write().unwrap();

        let iter = vec.clean_iter().unwrap().take(100);
        let collected: Vec<i32> = iter.collect();

        assert_eq!(collected.len(), 100);
        assert_eq!(collected[0], 0);
        assert_eq!(collected[99], 99);
    }

    #[test]
    fn test_compressed_clean_iter_skip_take_combined() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..5000 {
            vec.push(i);
        }
        vec.write().unwrap();

        let iter = vec.clean_iter().unwrap().skip(1000).take(2000);
        let collected: Vec<i32> = iter.collect();

        assert_eq!(collected.len(), 2000);
        assert_eq!(collected[0], 1000);
        assert_eq!(collected[1999], 2999);
    }

    #[test]
    fn test_compressed_clean_iter_set_position() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..5000 {
            vec.push(i);
        }
        vec.write().unwrap();

        let mut iter = vec.clean_iter().unwrap();
        iter.set_position_to(2500);
        assert_eq!(iter.next(), Some(2500));
        assert_eq!(iter.next(), Some(2501));
    }

    #[test]
    fn test_compressed_clean_iter_set_position_tosame_page() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..1000 {
            vec.push(i);
        }
        vec.write().unwrap();

        let mut iter = vec.clean_iter().unwrap();
        iter.next(); // Decode first page
        iter.set_position_to(50); // Should reuse same page
        assert_eq!(iter.next(), Some(50));
    }

    #[test]
    fn test_compressed_clean_iter_last() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..1000 {
            vec.push(i);
        }
        vec.write().unwrap();

        let iter = vec.clean_iter().unwrap();
        assert_eq!(iter.last(), Some(999));
    }

    #[test]
    fn test_compressed_clean_iter_last_empty() {
        let (_temp, _db, vec) = setup();

        let iter = vec.clean_iter().unwrap();
        assert_eq!(iter.last(), None);
    }

    #[test]
    fn test_compressed_clean_iter_exact_size() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..1000 {
            vec.push(i);
        }
        vec.write().unwrap();

        let mut iter = vec.clean_iter().unwrap();
        assert_eq!(iter.len(), 1000);

        iter.next();
        assert_eq!(iter.len(), 999);

        iter.nth(100);
        assert_eq!(iter.len(), 898);
    }

    #[test]
    fn test_compressed_clean_iter_page_boundaries() {
        let (_temp, _db, mut vec) = setup();

        // Push data that spans multiple pages
        for i in 0..10000 {
            vec.push(i);
        }
        vec.write().unwrap();

        // Iterate and ensure no gaps at page boundaries
        let mut iter = vec.clean_iter().unwrap();
        for i in 0..10000 {
            assert_eq!(iter.next(), Some(i));
        }
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_compressed_clean_iter_multiple_set_position() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..10000 {
            vec.push(i);
        }
        vec.write().unwrap();

        let mut iter = vec.clean_iter().unwrap();

        // Jump to different pages
        iter.set_position_to(1000);
        assert_eq!(iter.next(), Some(1000));

        iter.set_position_to(5000);
        assert_eq!(iter.next(), Some(5000));

        iter.set_position_to(100);
        assert_eq!(iter.next(), Some(100));
    }

    #[test]
    fn test_compressed_clean_iter_buffer_efficiency() {
        let (_temp, _db, mut vec) = setup();

        // Push enough data to fill multiple pages
        for i in 0..20000 {
            vec.push(i);
        }
        vec.write().unwrap();

        // Sequential iteration should reuse buffer efficiently
        let collected: Vec<i32> = vec.clean_iter().unwrap().collect();
        assert_eq!(collected.len(), 20000);

        for (i, &val) in collected.iter().enumerate() {
            assert_eq!(val, i as i32);
        }
    }

    #[test]
    fn test_compressed_clean_iter_skip_take_multiple() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..10000 {
            vec.push(i);
        }
        vec.write().unwrap();

        let collected: Vec<i32> = vec
            .clean_iter()
            .unwrap()
            .skip(1000)
            .take(5000)
            .skip(500)
            .take(2000)
            .collect();

        assert_eq!(collected.len(), 2000);
        assert_eq!(collected[0], 1500);
        assert_eq!(collected[1999], 3499);
    }

    #[test]
    fn test_compressed_clean_iter_nth_beyond_end() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..100 {
            vec.push(i);
        }
        vec.write().unwrap();

        let mut iter = vec.clean_iter().unwrap();
        assert_eq!(iter.nth(200), None);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_compressed_clean_iter_set_end_middle_of_page() {
        let (_temp, _db, mut vec) = setup();

        for i in 0..5000 {
            vec.push(i);
        }
        vec.write().unwrap();

        let mut iter = vec.clean_iter().unwrap();
        iter.set_end_to(2500); // Middle of a page

        let collected: Vec<i32> = iter.collect();
        assert_eq!(collected.len(), 2500);
        assert_eq!(collected[2499], 2499);
    }
}
