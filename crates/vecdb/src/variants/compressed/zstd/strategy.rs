use std::marker::PhantomData;

use rawdb::likely;

use crate::{Error, RawStrategy, Result};

use super::super::inner::CompressionStrategy;
use super::value::ZstdVecValue;

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
        let mut bytes = Vec::with_capacity(values.len() * size_of::<T>());
        for v in values {
            bytes.extend_from_slice(&v.to_bytes());
        }
        Ok(zstd::encode_all(bytes.as_slice(), ZSTD_COMPRESSION_LEVEL)?)
    }

    fn decompress(bytes: &[u8], expected_len: usize) -> Result<Vec<T>> {
        let decompressed = zstd::decode_all(bytes)?;

        let mut vec = Vec::with_capacity(expected_len);
        for chunk in decompressed.chunks_exact(size_of::<T>()) {
            vec.push(T::from_bytes(chunk)?);
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
