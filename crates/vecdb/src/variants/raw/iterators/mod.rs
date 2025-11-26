use std::iter::FusedIterator;

use crate::{GenericStoredVec, Result, TypedVecIterator, VecIndex, VecIterator, VecValue};

use super::{RawVecInner, RawStrategy};

mod clean;
mod dirty;

pub use clean::*;
pub use dirty::*;

/// Automatically selected iterator for raw vectors based on their state.
///
/// - Clean: No holes, updates, or pushed values - faster direct file reading
/// - Dirty: Has holes, updates, or pushed values - slower but handles all features
pub enum RawVecIterator<'a, I, T, S> {
    Clean(CleanRawVecIterator<'a, I, T, S>),
    Dirty(DirtyRawVecIterator<'a, I, T, S>),
}

impl<'a, I, T, S> RawVecIterator<'a, I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: RawStrategy<T>,
{
    #[inline]
    pub fn new(vec: &'a RawVecInner<I, T, S>) -> Result<Self> {
        Ok(if vec.is_dirty() {
            Self::Dirty(DirtyRawVecIterator::new(vec)?)
        } else {
            Self::Clean(CleanRawVecIterator::new(vec)?)
        })
    }

    pub fn is_clean(&self) -> bool {
        matches!(self, Self::Clean(_))
    }

    pub fn is_dirty(&self) -> bool {
        matches!(self, Self::Dirty(_))
    }
}

impl<I, T, S> Iterator for RawVecIterator<'_, I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: RawStrategy<T>,
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

impl<I, T, S> VecIterator for RawVecIterator<'_, I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: RawStrategy<T>,
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

    fn vec_len(&self) -> usize {
        match self {
            Self::Clean(iter) => iter.vec_len(),
            Self::Dirty(iter) => iter.vec_len(),
        }
    }
}

impl<I, T, S> TypedVecIterator for RawVecIterator<'_, I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: RawStrategy<T>,
{
    type I = I;
    type T = T;
}

impl<I, T, S> ExactSizeIterator for RawVecIterator<'_, I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: RawStrategy<T>,
{
    #[inline(always)]
    fn len(&self) -> usize {
        match self {
            Self::Clean(iter) => iter.len(),
            Self::Dirty(iter) => iter.len(),
        }
    }
}

impl<I, T, S> FusedIterator for RawVecIterator<'_, I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: RawStrategy<T>,
{
}
