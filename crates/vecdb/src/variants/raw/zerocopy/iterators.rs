use super::super::{CleanRawVecIterator, DirtyRawVecIterator, RawVecIterator};

use super::ZeroCopyStrategy;

/// Type alias for ZeroCopyVec iterator
pub type ZeroCopyVecIterator<'a, I, T> = RawVecIterator<'a, I, T, ZeroCopyStrategy<T>>;

/// Type alias for clean ZeroCopyVec iterator
pub type CleanZeroCopyVecIterator<'a, I, T> = CleanRawVecIterator<'a, I, T, ZeroCopyStrategy<T>>;

/// Type alias for dirty ZeroCopyVec iterator
pub type DirtyZeroCopyVecIterator<'a, I, T> = DirtyRawVecIterator<'a, I, T, ZeroCopyStrategy<T>>;
