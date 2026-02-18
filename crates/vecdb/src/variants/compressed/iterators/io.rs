use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
};

use parking_lot::RwLockReadGuard;
use rawdb::RegionMetadata;

use crate::{AnyStoredVec, BUFFER_SIZE, Pages, VecIndex, VecValue, unlikely};

use super::super::inner::{CompressedVecInner, CompressionStrategy, MAX_UNCOMPRESSED_PAGE_SIZE};

/// Buffered file I/O source for reading stored compressed data.
///
/// Uses dedicated file handle for sequential reads (better OS readahead than mmap).
/// Only sees stored (persisted) values. Consumed by fold/try_fold/for_each.
pub struct CompressedIoSource<'a, I, T, S> {
    file: File,
    file_position: u64,
    region_start: u64,
    buffer: Vec<u8>,
    buffer_len: usize,
    buffer_start_offset: u64,
    decoded_values: Vec<T>,
    decoded_page_index: usize,
    pages: RwLockReadGuard<'a, Pages>,
    index: usize,
    end_index: usize,
    _region_lock: RwLockReadGuard<'a, RegionMetadata>,
    _marker: std::marker::PhantomData<(I, T, S)>,
}

impl<'a, I, T, S> CompressedIoSource<'a, I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: CompressionStrategy<T>,
{
    const SIZE_OF_T: usize = size_of::<T>();
    const PER_PAGE: usize = MAX_UNCOMPRESSED_PAGE_SIZE / Self::SIZE_OF_T;
    const NO_PAGE: usize = usize::MAX;

    pub(crate) fn new(vec: &'a CompressedVecInner<I, T, S>, from: usize, to: usize) -> Self {
        let region_lock = vec.region().meta();
        let region_start = region_lock.start() as u64;
        let file = vec.region().open_db_read_only_file().expect("open file");
        let pages = vec.pages().read();
        let stored_len = vec.stored_len();
        let from = from.min(stored_len);
        let to = to.min(stored_len);

        Self {
            file,
            file_position: 0,
            region_start,
            buffer: vec![0; BUFFER_SIZE],
            buffer_len: 0,
            buffer_start_offset: 0,
            decoded_values: Vec::with_capacity(Self::PER_PAGE),
            decoded_page_index: Self::NO_PAGE,
            pages,
            index: from,
            end_index: to,
            _region_lock: region_lock,
            _marker: std::marker::PhantomData,
        }
    }

    #[inline(always)]
    fn ensure_page_decoded(&mut self, page_index: usize) -> Option<()> {
        if unlikely(self.decoded_page_index != page_index) {
            self.decode_page(page_index)?;
        }
        Some(())
    }

    fn refill_buffer(&mut self, starting_page_index: usize) -> Option<()> {
        let start_page = self.pages.get(starting_page_index)?;
        let start_offset = start_page.start;

        let last_needed_page = if self.end_index == 0 {
            0
        } else {
            (self.end_index - 1) / Self::PER_PAGE
        };
        let max_page = last_needed_page.min(self.pages.len().saturating_sub(1));

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
        self.buffer_start_offset = start_offset;
        self.file_position += total_bytes as u64;

        Some(())
    }

    fn decompress_from_buffer(
        &mut self,
        page_index: usize,
        compressed_offset: u64,
        compressed_size: usize,
        values_count: usize,
    ) -> Option<()> {
        let in_buffer_offset = (compressed_offset - self.buffer_start_offset) as usize;
        let compressed_data = &self.buffer[in_buffer_offset..in_buffer_offset + compressed_size];

        S::decompress_into(compressed_data, values_count, &mut self.decoded_values).ok()?;
        self.decoded_page_index = page_index;

        Some(())
    }

    fn decode_page(&mut self, page_index: usize) -> Option<()> {
        if page_index >= self.pages.len() {
            return None;
        }

        let page = self.pages.get(page_index)?;
        let compressed_size = page.bytes as usize;
        let compressed_offset = page.start;
        let values_count = page.values as usize;

        if self.buffer_len > 0 {
            let buffer_end_offset = self.buffer_start_offset + self.buffer_len as u64;

            if compressed_offset >= self.buffer_start_offset
                && compressed_offset + compressed_size as u64 <= buffer_end_offset
            {
                return self.decompress_from_buffer(
                    page_index,
                    compressed_offset,
                    compressed_size,
                    values_count,
                );
            }
        }

        self.refill_buffer(page_index)?;
        self.decompress_from_buffer(page_index, compressed_offset, compressed_size, values_count)
    }

    /// Fold all remaining elements — tight pointer loop per page so LLVM can vectorize.
    #[inline]
    pub(crate) fn fold<B, F: FnMut(B, T) -> B>(mut self, init: B, mut f: F) -> B {
        let per_page = Self::PER_PAGE;
        let end_index = self.end_index;
        let mut page_index = self.index / per_page;
        let mut page_start = page_index * per_page;
        let mut in_page_offset = self.index - page_start;
        let mut accum = init;
        while self.index < end_index {
            if self.ensure_page_decoded(page_index).is_none() {
                break;
            }
            let page_end = (end_index - page_start).min(self.decoded_values.len());
            let ptr = self.decoded_values.as_ptr();
            let mut i = in_page_offset;
            while i < page_end {
                accum = f(accum, unsafe { ptr.add(i).read() });
                i += 1;
            }
            self.index = page_start + page_end;
            page_index += 1;
            page_start += per_page;
            in_page_offset = 0;
        }
        accum
    }

    /// Fallible fold with early exit on error.
    #[inline]
    pub(crate) fn try_fold<B, E, F: FnMut(B, T) -> std::result::Result<B, E>>(
        mut self,
        init: B,
        mut f: F,
    ) -> std::result::Result<B, E> {
        let per_page = Self::PER_PAGE;
        let end_index = self.end_index;
        let mut page_index = self.index / per_page;
        let mut page_start = page_index * per_page;
        let mut in_page_offset = self.index - page_start;
        let mut accum = init;
        while self.index < end_index {
            if self.ensure_page_decoded(page_index).is_none() {
                break;
            }
            let page_end = (end_index - page_start).min(self.decoded_values.len());
            for value in &self.decoded_values[in_page_offset..page_end] {
                accum = f(accum, value.clone())?;
            }
            self.index = page_start + page_end;
            page_index += 1;
            page_start += per_page;
            in_page_offset = 0;
        }
        Ok(accum)
    }

}
