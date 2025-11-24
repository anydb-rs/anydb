use crate::{
    CleanCompressedVecIterator, CompressedVecIterator, DirtyCompressedVecIterator, ZstdStrategy,
};

/// Type alias for ZstdVec iterator
pub type ZstdVecIterator<'a, I, T> = CompressedVecIterator<'a, I, T, ZstdStrategy<T>>;

/// Type alias for clean ZstdVec iterator
pub type CleanZstdVecIterator<'a, I, T> = CleanCompressedVecIterator<'a, I, T, ZstdStrategy<T>>;

/// Type alias for dirty ZstdVec iterator
pub type DirtyZstdVecIterator<'a, I, T> = DirtyCompressedVecIterator<'a, I, T, ZstdStrategy<T>>;
