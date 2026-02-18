use rawdb::likely;

use crate::{Error, ValueStrategy, Result};

/// Trait for compression strategies used by CompressedVecInner.
pub trait CompressionStrategy<T>: ValueStrategy<T> {
    /// Compress a slice of values into bytes.
    fn compress(values: &[T]) -> Result<Vec<u8>>;

    /// Decompress bytes into a vector of values.
    fn decompress(bytes: &[u8], expected_len: usize) -> Result<Vec<T>>;

    /// Decompress bytes into an existing buffer.
    /// Implementations should reuse dst's allocation when possible (see PcodecStrategy).
    /// Default implementation replaces dst with a new Vec from `decompress`.
    #[inline]
    fn decompress_into(bytes: &[u8], expected_len: usize, dst: &mut Vec<T>) -> Result<()> {
        *dst = Self::decompress(bytes, expected_len)?;
        Ok(())
    }

    /// Serializes a slice of values to bytes.
    #[inline]
    fn values_to_bytes(values: &[T]) -> Vec<u8> {
        let byte_len = size_of_val(values);
        let mut bytes = Vec::with_capacity(byte_len);
        if Self::IS_NATIVE_LAYOUT {
            unsafe {
                std::ptr::copy_nonoverlapping(
                    values.as_ptr() as *const u8,
                    bytes.as_mut_ptr(),
                    byte_len,
                );
                bytes.set_len(byte_len);
            }
        } else {
            for v in values {
                Self::write_to_vec(v, &mut bytes);
            }
        }
        bytes
    }

    /// Deserializes bytes to a vector of values, validating the expected length.
    #[inline]
    fn bytes_to_values(bytes: &[u8], expected_len: usize) -> Result<Vec<T>> {
        let mut vec = Vec::with_capacity(expected_len);
        Self::bytes_to_values_into(bytes, expected_len, &mut vec)?;
        Ok(vec)
    }

    /// Deserializes bytes into an existing buffer, reusing its allocation.
    #[inline]
    fn bytes_to_values_into(bytes: &[u8], expected_len: usize, dst: &mut Vec<T>) -> Result<()> {
        let expected_bytes = expected_len * size_of::<T>();
        dst.clear();
        dst.reserve(expected_len);
        if Self::IS_NATIVE_LAYOUT {
            if likely(bytes.len() >= expected_bytes) {
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        bytes.as_ptr(),
                        dst.as_mut_ptr() as *mut u8,
                        expected_bytes,
                    );
                    dst.set_len(expected_len);
                }
                return Ok(());
            }
        } else {
            for chunk in bytes.chunks_exact(size_of::<T>()) {
                dst.push(Self::read(chunk)?);
            }
            if likely(dst.len() == expected_len) {
                return Ok(());
            }
        }

        Err(Error::DecompressionMismatch {
            expected_len,
            actual_len: if Self::IS_NATIVE_LAYOUT { bytes.len() / size_of::<T>() } else { dst.len() },
        })
    }
}
