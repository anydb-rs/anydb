use crate::{RawStrategy, Result};

/// Trait for compression strategies used by CompressedVecInner.
pub trait CompressionStrategy<T>: RawStrategy<T> {
    /// Compress a slice of values into bytes.
    fn compress(values: &[T]) -> Result<Vec<u8>>;

    /// Decompress bytes into a vector of values.
    fn decompress(bytes: &[u8], expected_len: usize) -> Result<Vec<T>>;
}
