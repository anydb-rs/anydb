use super::super::{
    CleanRawVecIterator, DirtyRawVecIterator, RawVecIterator, BytesStrategy,
};

/// Type alias for BytesVec iterator
pub type BytesVecIterator<'a, I, T> = RawVecIterator<'a, I, T, BytesStrategy<T>>;

/// Type alias for clean BytesVec iterator
pub type CleanBytesVecIterator<'a, I, T> = CleanRawVecIterator<'a, I, T, BytesStrategy<T>>;

/// Type alias for dirty BytesVec iterator
pub type DirtyBytesVecIterator<'a, I, T> = DirtyRawVecIterator<'a, I, T, BytesStrategy<T>>;
