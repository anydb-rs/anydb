use std::{iter::FusedIterator, marker::PhantomData};

use parking_lot::RwLockReadGuard;
use rawdb::Reader;

use crate::{Error, Pages, Result, TypedVecIterator, VecIndex, VecIterator, VecValue, likely, unlikely};

use super::inner::{CompressionStrategy, MAX_UNCOMPRESSED_PAGE_SIZE};

/// Read-only view into a compressed vector.
///
/// Created via `vec.view()`. Provides page-based random access and range
/// iteration, decoding only the pages needed for each operation.
///
/// Only sees **stored** (persisted) values — pushed but unflushed values
/// are not visible.
///
/// Implements `TypedVecIterator` via `into_range_iter()`. Pages are decoded
/// lazily — only when iteration reaches them. Same pattern as
/// `CleanCompressedVecIterator` but uses mmap instead of buffered file I/O.
pub struct CompressedVecView<'a, I, T, S> {
    reader: Reader,
    stored_len: usize,
    pages: RwLockReadGuard<'a, Pages>,
    // Lazily decoded current page (same pattern as CleanCompressedVecIterator)
    page_buf: Vec<T>,
    page_buf_idx: usize,
    // Iteration bounds
    pos: usize,
    end: usize,
    _marker: PhantomData<(I, T, S)>,
}

impl<'a, I, T, S> CompressedVecView<'a, I, T, S>
where
    T: VecValue,
    S: CompressionStrategy<T>,
{
    const SIZE_OF_T: usize = size_of::<T>();
    const PER_PAGE: usize = MAX_UNCOMPRESSED_PAGE_SIZE / Self::SIZE_OF_T;
    const NO_PAGE: usize = usize::MAX;

    pub(crate) fn new(
        reader: Reader,
        stored_len: usize,
        pages: RwLockReadGuard<'a, Pages>,
    ) -> Self {
        Self {
            reader,
            stored_len,
            pages,
            page_buf: Vec::with_capacity(Self::PER_PAGE),
            page_buf_idx: Self::NO_PAGE,
            pos: 0,
            end: 0,
            _marker: PhantomData,
        }
    }

    /// Sets up iteration bounds for [from, to). Pages are decoded lazily.
    pub(crate) fn into_range_iter(mut self, from: usize, to: usize) -> Self {
        let to = to.min(self.stored_len);
        let from = from.min(to);
        self.pos = from;
        self.end = to;
        self
    }

    /// Returns the value at `index`.
    ///
    /// # Panics
    /// Panics if `index >= len()`.
    #[inline]
    pub fn get(&self, index: usize) -> T {
        self.try_get(index)
            .unwrap_or_else(|| panic!("index {index} out of bounds (len {})", self.stored_len))
    }

    /// Returns the value at `index`, or `None` if out of bounds.
    #[inline]
    pub fn try_get(&self, index: usize) -> Option<T> {
        if index >= self.stored_len {
            return None;
        }
        let page_index = index / Self::PER_PAGE;
        let in_page_index = index % Self::PER_PAGE;
        let decoded = self.decode_page(page_index).ok()?;
        Some(unsafe { decoded.get_unchecked(in_page_index).clone() })
    }

    /// Returns the number of stored values.
    #[inline]
    pub fn len(&self) -> usize {
        self.stored_len
    }

    /// Returns `true` if the view is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.stored_len == 0
    }

    /// Decodes and returns all values from a single page.
    pub fn decode_page(&self, page_index: usize) -> Result<Vec<T>> {
        let index = page_index * Self::PER_PAGE;

        if index >= self.stored_len {
            return Err(Error::IndexTooHigh {
                index,
                len: self.stored_len,
                name: "compressed_view_page".to_string(),
            });
        }

        if page_index >= self.pages.len() {
            return Err(Error::ExpectVecToHaveIndex);
        }

        let page = self
            .pages
            .get(page_index)
            .expect("page should exist after bounds check");
        let len = page.bytes as usize;
        let offset = page.start as usize;

        let compressed_data = self.reader.unchecked_read(offset, len);
        let vec = S::decompress(compressed_data, page.values as usize)?;

        if likely(vec.len() == page.values as usize) {
            return Ok(vec);
        }

        Err(Error::DecompressionMismatch {
            expected_len: page.values as usize,
            actual_len: vec.len(),
        })
    }

    /// Returns an iterator over values in the range `[from, to)`.
    ///
    /// Eagerly decodes all overlapping pages. For lazy decoding, use
    /// `into_range_iter()` instead.
    pub fn range(&self, from: usize, to: usize) -> impl Iterator<Item = T> {
        let to = to.min(self.stored_len);
        let from = from.min(to);
        let mut values = Vec::with_capacity(to - from);

        if from < to {
            let first_page = from / Self::PER_PAGE;
            let last_page = (to - 1) / Self::PER_PAGE;

            for page_idx in first_page..=last_page {
                if let Ok(page_values) = self.decode_page(page_idx) {
                    let page_start = page_idx * Self::PER_PAGE;
                    let overlap_start = from.max(page_start);
                    let overlap_end = to.min(page_start + page_values.len());
                    let skip = overlap_start - page_start;
                    let count = overlap_end - overlap_start;
                    values.extend(page_values.into_iter().skip(skip).take(count));
                }
            }
        }

        values.into_iter()
    }

    /// Returns an iterator over all stored values, page by page.
    pub fn iter(&self) -> impl Iterator<Item = T> {
        self.range(0, self.stored_len)
    }

    /// Decode a page into the internal buffer via mmap.
    /// Same pattern as `CleanCompressedVecIterator::decode_page` but without
    /// buffered I/O — mmap provides direct access to compressed data.
    #[inline(always)]
    fn decode_page_lazy(&mut self, page_index: usize) -> Option<()> {
        let page = self.pages.get(page_index)?;
        let len = page.bytes as usize;
        let offset = page.start as usize;
        let values_count = page.values as usize;

        let compressed_data = self.reader.unchecked_read(offset, len);
        S::decompress_into(compressed_data, values_count, &mut self.page_buf).ok()?;
        self.page_buf_idx = page_index;

        Some(())
    }
}

// --- TypedVecIterator implementation for CompressedVecView ---
// Lazy page-by-page decoding, same pattern as CleanCompressedVecIterator.

impl<I, T, S> Iterator for CompressedVecView<'_, I, T, S>
where
    T: VecValue,
    S: CompressionStrategy<T>,
{
    type Item = T;

    #[inline(always)]
    fn next(&mut self) -> Option<T> {
        let pos = self.pos;

        if unlikely(pos >= self.end) {
            return None;
        }

        self.pos += 1;

        let page_index = pos / Self::PER_PAGE;
        let in_page_index = pos % Self::PER_PAGE;

        // Fast path: read from current decoded page
        if likely(self.page_buf_idx == page_index) {
            return self.page_buf.get(in_page_index).cloned();
        }

        // Slow path: decode new page
        self.decode_page_lazy(page_index)?;
        self.page_buf.get(in_page_index).cloned()
    }

    #[inline]
    fn nth(&mut self, n: usize) -> Option<T> {
        if n == 0 {
            return self.next();
        }

        let new_pos = self.pos.saturating_add(n);
        if new_pos >= self.end {
            self.pos = self.end;
            return None;
        }

        self.pos = new_pos;
        self.next()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.end.saturating_sub(self.pos);
        (remaining, Some(remaining))
    }

    #[inline]
    fn count(self) -> usize {
        self.len()
    }

    #[inline]
    fn last(mut self) -> Option<T> {
        if unlikely(self.pos >= self.end) {
            return None;
        }

        self.pos = self.end - 1;
        self.next()
    }
}

impl<I, T, S> ExactSizeIterator for CompressedVecView<'_, I, T, S>
where
    T: VecValue,
    S: CompressionStrategy<T>,
{
    #[inline(always)]
    fn len(&self) -> usize {
        self.end.saturating_sub(self.pos)
    }
}

impl<I, T, S> FusedIterator for CompressedVecView<'_, I, T, S>
where
    T: VecValue,
    S: CompressionStrategy<T>,
{
}

impl<I, T, S> VecIterator for CompressedVecView<'_, I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: CompressionStrategy<T>,
{
    #[inline]
    fn set_position_to(&mut self, i: usize) {
        let new_pos = i.min(self.stored_len).min(self.end);

        if self.pos == new_pos {
            return;
        }

        // Check if new position is within the currently decoded page
        if self.page_buf_idx != Self::NO_PAGE {
            let page_start = self.page_buf_idx * Self::PER_PAGE;
            let page_end = page_start + Self::PER_PAGE;

            if new_pos >= page_start && new_pos < page_end {
                self.pos = new_pos;
                return;
            }
        }

        // New position outside current page, invalidate cache
        self.page_buf_idx = Self::NO_PAGE;
        self.pos = new_pos;
    }

    #[inline]
    fn set_end_to(&mut self, i: usize) {
        self.end = i.min(self.stored_len).min(self.end);
    }

    #[inline]
    fn vec_len(&self) -> usize {
        self.stored_len
    }
}

impl<I, T, S> TypedVecIterator for CompressedVecView<'_, I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: CompressionStrategy<T>,
{
    type I = I;
    type T = T;
}
