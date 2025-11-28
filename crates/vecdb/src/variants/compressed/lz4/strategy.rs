use std::marker::PhantomData;

use rawdb::likely;

use crate::{Error, RawStrategy, Result};

use super::super::inner::CompressionStrategy;
use super::value::LZ4VecValue;

/// LZ4 compression strategy for fast compression/decompression.
#[derive(Debug, Clone, Copy)]
pub struct LZ4Strategy<T>(PhantomData<T>);

impl<T> RawStrategy<T> for LZ4Strategy<T>
where
    T: LZ4VecValue,
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

impl<T> CompressionStrategy<T> for LZ4Strategy<T>
where
    T: LZ4VecValue,
{
    fn compress(values: &[T]) -> Result<Vec<u8>> {
        let mut bytes = Vec::with_capacity(size_of_val(values));
        for v in values {
            bytes.extend_from_slice(&v.to_bytes());
        }
        Ok(lz4_flex::compress_prepend_size(&bytes))
    }

    fn decompress(bytes: &[u8], expected_len: usize) -> Result<Vec<T>> {
        let decompressed = lz4_flex::decompress_size_prepended(bytes)?;

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
