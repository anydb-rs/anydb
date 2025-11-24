use crate::{
    CleanCompressedVecIterator, CompressedVecIterator, DirtyCompressedVecIterator, LZ4Strategy,
};

/// Type alias for LZ4Vec iterator
pub type LZ4VecIterator<'a, I, T> = CompressedVecIterator<'a, I, T, LZ4Strategy<T>>;

/// Type alias for clean LZ4Vec iterator
pub type CleanLZ4VecIterator<'a, I, T> = CleanCompressedVecIterator<'a, I, T, LZ4Strategy<T>>;

/// Type alias for dirty LZ4Vec iterator
pub type DirtyLZ4VecIterator<'a, I, T> = DirtyCompressedVecIterator<'a, I, T, LZ4Strategy<T>>;
