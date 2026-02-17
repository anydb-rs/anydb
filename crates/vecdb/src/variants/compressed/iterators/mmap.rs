use std::marker::PhantomData;

use parking_lot::RwLockReadGuard;
use rawdb::Reader;

use crate::{AnyStoredVec, Pages, VecIndex, VecValue, unlikely};

use super::super::inner::{CompressedVecInner, CompressionStrategy, MAX_UNCOMPRESSED_PAGE_SIZE};

/// Read-only mmap-backed source over a compressed vector.
///
/// Only sees **stored** (persisted) values. Pages are decoded lazily —
/// only when fold/for_each reaches them. Consumed by fold/try_fold/for_each.
pub struct CompressedMmapSource<'a, I, T, S> {
    reader: Reader,
    pages: RwLockReadGuard<'a, Pages>,
    page_buf: Vec<T>,
    page_buf_idx: usize,
    pos: usize,
    end: usize,
    _marker: PhantomData<(I, T, S)>,
}

impl<'a, I, T, S> CompressedMmapSource<'a, I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: CompressionStrategy<T>,
{
    const SIZE_OF_T: usize = size_of::<T>();
    const PER_PAGE: usize = MAX_UNCOMPRESSED_PAGE_SIZE / Self::SIZE_OF_T;
    const NO_PAGE: usize = usize::MAX;

    pub(crate) fn new(vec: &'a CompressedVecInner<I, T, S>, from: usize, to: usize) -> Self {
        let stored_len = vec.stored_len();
        let from = from.min(stored_len);
        let to = to.min(stored_len);
        Self {
            reader: vec.region().create_reader(),
            pages: vec.pages().read(),
            page_buf: Vec::with_capacity(Self::PER_PAGE),
            page_buf_idx: Self::NO_PAGE,
            pos: from,
            end: to,
            _marker: PhantomData,
        }
    }

    /// Ensures the page at `page_index` is decoded in `page_buf`.
    #[inline(always)]
    fn ensure_page_decoded(&mut self, page_index: usize) -> Option<()> {
        if unlikely(self.page_buf_idx != page_index) {
            self.decode_page_into_buf(page_index)?;
        }
        Some(())
    }

    /// Decode a page into the internal buffer via mmap.
    #[inline(always)]
    fn decode_page_into_buf(&mut self, page_index: usize) -> Option<()> {
        let page = self.pages.get(page_index)?;
        let len = page.bytes as usize;
        let offset = page.start as usize;
        let values_count = page.values as usize;

        let compressed_data = self.reader.unchecked_read(offset, len);
        S::decompress_into(compressed_data, values_count, &mut self.page_buf).ok()?;
        self.page_buf_idx = page_index;

        Some(())
    }

    /// Fold all remaining elements — page-at-a-time bulk decode.
    #[inline]
    pub(crate) fn fold<B, F: FnMut(B, T) -> B>(mut self, init: B, mut f: F) -> B {
        let mut accum = init;
        while self.pos < self.end {
            let page_index = self.pos / Self::PER_PAGE;
            let in_page_offset = self.pos % Self::PER_PAGE;
            if self.ensure_page_decoded(page_index).is_none() {
                break;
            }
            let page_start = page_index * Self::PER_PAGE;
            let in_page_end = (self.end - page_start).min(self.page_buf.len());
            for value in &self.page_buf[in_page_offset..in_page_end] {
                accum = f(accum, value.clone());
            }
            self.pos = page_start + in_page_end;
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
        let mut accum = init;
        while self.pos < self.end {
            let page_index = self.pos / Self::PER_PAGE;
            let in_page_offset = self.pos % Self::PER_PAGE;
            if self.ensure_page_decoded(page_index).is_none() {
                break;
            }
            let page_start = page_index * Self::PER_PAGE;
            let in_page_end = (self.end - page_start).min(self.page_buf.len());
            for value in &self.page_buf[in_page_offset..in_page_end] {
                accum = f(accum, value.clone())?;
            }
            self.pos = page_start + in_page_end;
        }
        Ok(accum)
    }

}
