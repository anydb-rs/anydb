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
        let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_bytes()).collect();
        Ok(zstd::encode_all(bytes.as_slice(), ZSTD_COMPRESSION_LEVEL)?)
    }

    fn decompress(bytes: &[u8], expected_len: usize) -> Result<Vec<T>> {
        let decompressed = zstd::decode_all(bytes)?;

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
