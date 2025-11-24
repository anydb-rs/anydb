use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    iter::FusedIterator,
    marker::PhantomData,
};

use parking_lot::RwLockReadGuard;
use rawdb::RegionMetadata;

use crate::{
    AnyStoredVec, HEADER_OFFSET, Result, TypedVecIterator, VecIndex, VecIterator, VecValue, likely,
    unlikely,
};

use super::{RawVecInner, SerializeStrategy};

/// Clean raw vec iterator, to read on disk data
pub struct CleanRawVecIterator<'a, I, T, S> {
    pub(crate) file: File,
    buffer: Vec<u8>,
    pub(crate) buffer_pos: usize,
    buffer_len: usize,
    file_offset: usize,
    end_offset: usize,
    start_offset: usize,
    pub(crate) _vec: &'a RawVecInner<I, T, S>,
    _lock: RwLockReadGuard<'a, RegionMetadata>,
    _marker: PhantomData<S>,
}

impl<'a, I, T, S> CleanRawVecIterator<'a, I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: SerializeStrategy<T>,
{
    const SIZE_OF_T: usize = size_of::<T>();
    const NORMAL_BUFFER_SIZE: usize = RawVecInner::<I, T, S>::aligned_buffer_size();
    const _CHECK_T: () = assert!(Self::SIZE_OF_T > 0, "Can't have T with size_of() == 0");

    pub fn new(vec: &'a RawVecInner<I, T, S>) -> Result<Self> {
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
            _marker: PhantomData,
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

impl<I, T, S> Iterator for CleanRawVecIterator<'_, I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: SerializeStrategy<T>,
{
    type Item = T;

    #[inline(always)]
    fn next(&mut self) -> Option<T> {
        if likely(self.can_read_buffer()) {
            let bytes = &self.buffer[self.buffer_pos..self.buffer_pos + Self::SIZE_OF_T];
            let value = S::read(bytes).expect("Failed to deserialize value");
            self.buffer_pos += Self::SIZE_OF_T;
            return Some(value);
        }

        if unlikely(self.cant_read_file()) {
            return None;
        }

        self.refill_buffer();

        let bytes = &self.buffer[..Self::SIZE_OF_T];
        let value = S::read(bytes).expect("Failed to deserialize value");
        self.buffer_pos = Self::SIZE_OF_T;
        Some(value)
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

impl<I, T, S> VecIterator for CleanRawVecIterator<'_, I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: SerializeStrategy<T>,
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

impl<I, T, S> TypedVecIterator for CleanRawVecIterator<'_, I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: SerializeStrategy<T>,
{
    type I = I;
    type T = T;
}

impl<I, T, S> ExactSizeIterator for CleanRawVecIterator<'_, I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: SerializeStrategy<T>,
{
    #[inline(always)]
    fn len(&self) -> usize {
        self.remaining()
    }
}

impl<I, T, S> FusedIterator for CleanRawVecIterator<'_, I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: SerializeStrategy<T>,
{
}
