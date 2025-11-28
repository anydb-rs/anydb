use rawdb::likely;

use crate::{Error, RawStrategy, Result};

/// Trait for compression strategies used by CompressedVecInner.
pub trait CompressionStrategy<T>: RawStrategy<T> {
    /// Compress a slice of values into bytes.
    fn compress(values: &[T]) -> Result<Vec<u8>>;

    /// Decompress bytes into a vector of values.
    fn decompress(bytes: &[u8], expected_len: usize) -> Result<Vec<T>>;

    /// Serializes a slice of values to bytes.
    #[inline]
    fn values_to_bytes(values: &[T]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(size_of_val(values));
        for v in values {
            bytes.extend_from_slice(&Self::write(v));
        }
        bytes
    }

    /// Deserializes bytes to a vector of values, validating the expected length.
    #[inline]
    fn bytes_to_values(bytes: &[u8], expected_len: usize) -> Result<Vec<T>> {
        let mut vec = Vec::with_capacity(expected_len);
        for chunk in bytes.chunks_exact(size_of::<T>()) {
            vec.push(Self::read(chunk)?);
        }

        if likely(vec.len() == expected_len) {
            return Ok(vec);
        }

        Err(Error::DecompressionMismatch {
            expected_len,
            actual_len: vec.len(),
        })
    }
}
