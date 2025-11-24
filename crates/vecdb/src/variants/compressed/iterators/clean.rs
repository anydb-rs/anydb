use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    iter::FusedIterator,
};

use parking_lot::RwLockReadGuard;
use rawdb::RegionMetadata;

use crate::{
    AnyStoredVec, BUFFER_SIZE, GenericStoredVec, Pages, Result, TypedVecIterator, VecIndex,
    VecIterator, VecValue, likely, unlikely,
};

use super::super::inner::{CompressedVecInner, CompressionStrategy, MAX_UNCOMPRESSED_PAGE_SIZE};

/// Clean compressed vec iterator, for reading stored compressed data
/// Uses dedicated file handle for sequential reads (better OS readahead than mmap)
pub struct CleanCompressedVecIterator<'a, I, T, S> {
    pub(crate) _vec: &'a CompressedVecInner<I, T, S>,
    file: File,         // Dedicated file handle for sequential reads
    file_position: u64, // Current position in the file
    region_start: u64,  // Absolute start offset of this region in the database file
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
    // _strategy: PhantomData<S>,
}

impl<'a, I, T, S> CleanCompressedVecIterator<'a, I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: CompressionStrategy<T>,
{
    const SIZE_OF_T: usize = size_of::<T>();
    const PER_PAGE: usize = MAX_UNCOMPRESSED_PAGE_SIZE / Self::SIZE_OF_T;
    const NO_PAGE: usize = usize::MAX;

    pub fn new(vec: &'a CompressedVecInner<I, T, S>) -> Result<Self> {
        let region_lock = vec.region().meta();
        let region_start = region_lock.start() as u64;
        let file = vec.region().open_db_read_only_file()?;

        let pages = vec.pages().read();
        let stored_len = vec.stored_len();

        Ok(Self {
            _vec: vec,
            file,
            file_position: 0,
            region_start,
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

        let absolute_offset = self.region_start + start_offset;
        if self.file_position != absolute_offset {
            self.file_position = absolute_offset;
            self.file.seek(SeekFrom::Start(absolute_offset)).unwrap();
        }

        self.file
            .read_exact(&mut self.buffer[..total_bytes])
            .unwrap();
        self.buffer_len = total_bytes;
        self.file_position += total_bytes as u64;

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

        self.decoded_values = S::decompress(compressed_data, values_count).ok()?;
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

impl<I, T, S> Iterator for CleanCompressedVecIterator<'_, I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: CompressionStrategy<T>,
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
            return self.decoded_values.get(in_page_index).cloned();
        }

        // Slow path: decode new page
        self.decode_page(page_index)?;
        self.decoded_values.get(in_page_index).cloned()
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

impl<I, T, S> VecIterator for CleanCompressedVecIterator<'_, I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: CompressionStrategy<T>,
{
    #[inline]
    fn set_position_to(&mut self, i: usize) {
        if self.index == i {
            return;
        }

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

impl<I, T, S> TypedVecIterator for CleanCompressedVecIterator<'_, I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: CompressionStrategy<T>,
{
    type I = I;
    type T = T;
}

impl<I, T, S> ExactSizeIterator for CleanCompressedVecIterator<'_, I, T, S>
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

impl<I, T, S> FusedIterator for CleanCompressedVecIterator<'_, I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: CompressionStrategy<T>,
{
}
