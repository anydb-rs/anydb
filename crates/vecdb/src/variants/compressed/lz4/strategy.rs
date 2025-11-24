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
        let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_bytes()).collect();
        Ok(lz4_flex::compress_prepend_size(&bytes))
    }

    fn decompress(bytes: &[u8], expected_len: usize) -> Result<Vec<T>> {
        let decompressed = lz4_flex::decompress_size_prepended(bytes)?;

        let vec = decompressed
            .chunks_exact(T::SIZE)
            .map(T::from_bytes)
            .collect::<Result<Vec<T>>>()?;

        if likely(vec.len() == expected_len) {
            return Ok(vec);
        }

        Err(Error::DecompressionMismatch {
            expected_len,
            actual_len: vec.len(),
        })
    }
}
