use std::iter::FusedIterator;

use crate::{Result, TypedVecIterator, VecIndex, VecIterator, VecValue};

use super::inner::CompressionStrategy;

mod clean;
mod dirty;

pub use clean::*;
pub use dirty::*;

/// Automatically selected iterator for compressed vectors based on their state.
///
/// - Clean: No pushed values - decompresses pages directly from disk
/// - Dirty: Has pushed values - combines stored compressed data with in-memory pushes
pub enum CompressedVecIterator<'a, I, T, S> {
    Clean(CleanCompressedVecIterator<'a, I, T, S>),
    Dirty(DirtyCompressedVecIterator<'a, I, T, S>),
}

impl<'a, I, T, S> CompressedVecIterator<'a, I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: CompressionStrategy<T>,
{
    #[inline]
    pub fn new(vec: &'a super::inner::CompressedVecInner<I, T, S>) -> Result<Self> {
        Ok(if vec.is_dirty() {
            Self::Dirty(DirtyCompressedVecIterator::new(vec)?)
        } else {
            Self::Clean(CleanCompressedVecIterator::new(vec)?)
        })
    }
}

impl<I, T, S> Iterator for CompressedVecIterator<'_, I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: CompressionStrategy<T>,
{
    type Item = T;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Clean(iter) => iter.next(),
            Self::Dirty(iter) => iter.next(),
        }
    }

    #[inline]
    fn nth(&mut self, n: usize) -> Option<T> {
        match self {
            Self::Clean(iter) => iter.nth(n),
            Self::Dirty(iter) => iter.nth(n),
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Clean(iter) => iter.size_hint(),
            Self::Dirty(iter) => iter.size_hint(),
        }
    }

    #[inline]
    fn count(self) -> usize {
        match self {
            Self::Clean(iter) => iter.count(),
            Self::Dirty(iter) => iter.count(),
        }
    }

    #[inline]
    fn last(self) -> Option<T> {
        match self {
            Self::Clean(iter) => iter.last(),
            Self::Dirty(iter) => iter.last(),
        }
    }
}

impl<I, T, S> VecIterator for CompressedVecIterator<'_, I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: CompressionStrategy<T>,
{
    #[inline]
    fn set_position_to(&mut self, i: usize) {
        match self {
            Self::Clean(iter) => iter.set_position_to(i),
            Self::Dirty(iter) => iter.set_position_to(i),
        };
    }

    #[inline]
    fn set_end_to(&mut self, i: usize) {
        match self {
            Self::Clean(iter) => iter.set_end_to(i),
            Self::Dirty(iter) => iter.set_end_to(i),
        };
    }

    #[inline]
    fn vec_len(&self) -> usize {
        match self {
            Self::Clean(iter) => iter.vec_len(),
            Self::Dirty(iter) => iter.vec_len(),
        }
    }
}

impl<I, T, S> TypedVecIterator for CompressedVecIterator<'_, I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: CompressionStrategy<T>,
{
    type I = I;
    type T = T;
}

impl<I, T, S> ExactSizeIterator for CompressedVecIterator<'_, I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: CompressionStrategy<T>,
{
    #[inline(always)]
    fn len(&self) -> usize {
        match self {
            Self::Clean(iter) => iter.len(),
            Self::Dirty(iter) => iter.len(),
        }
    }
}

impl<I, T, S> FusedIterator for CompressedVecIterator<'_, I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: CompressionStrategy<T>,
{
}
