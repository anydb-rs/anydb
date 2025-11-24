use crate::{
    CleanCompressedVecIterator, CompressedVecIterator, DirtyCompressedVecIterator, PcodecStrategy,
};

/// Type alias for PcodecVec iterator
pub type PcodecVecIterator<'a, I, T> = CompressedVecIterator<'a, I, T, PcodecStrategy<T>>;

/// Type alias for clean PcodecVec iterator
pub type CleanPcodecVecIterator<'a, I, T> = CleanCompressedVecIterator<'a, I, T, PcodecStrategy<T>>;

/// Type alias for dirty PcodecVec iterator
pub type DirtyPcodecVecIterator<'a, I, T> = DirtyCompressedVecIterator<'a, I, T, PcodecStrategy<T>>;
