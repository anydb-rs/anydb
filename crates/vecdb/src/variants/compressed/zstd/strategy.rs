use std::marker::PhantomData;

use zstd::{decode_all, encode_all};

use crate::{RawStrategy, Result};

use super::super::inner::CompressionStrategy;
use super::value::ZstdVecValue;

/// Zstd compression level (1-22). Level 3 provides a good balance
/// between compression ratio and speed for most workloads.
const ZSTD_COMPRESSION_LEVEL: i32 = 3;

/// Zstd compression strategy for high compression ratios.
#[derive(Debug, Clone, Copy)]
pub struct ZstdStrategy<T>(PhantomData<T>);

impl<T> RawStrategy<T> for ZstdStrategy<T>
where
    T: ZstdVecValue,
{
    #[inline(always)]
    fn read(bytes: &[u8]) -> Result<T> {
        T::from_bytes(bytes)
    }

    #[inline(always)]
    fn write(value: &T) -> Vec<u8> {
        value.to_bytes()
    }
}

impl<T> CompressionStrategy<T> for ZstdStrategy<T>
where
    T: ZstdVecValue,
{
    fn compress(values: &[T]) -> Result<Vec<u8>> {
        let bytes = Self::values_to_bytes(values);
        Ok(encode_all(bytes.as_slice(), ZSTD_COMPRESSION_LEVEL)?)
    }

    fn decompress(bytes: &[u8], expected_len: usize) -> Result<Vec<T>> {
        let decompressed = decode_all(bytes)?;
        Self::bytes_to_values(&decompressed, expected_len)
    }
}
