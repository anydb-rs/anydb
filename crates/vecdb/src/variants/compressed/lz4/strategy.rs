use std::marker::PhantomData;

use lz4_flex::{compress_prepend_size, decompress_size_prepended};

use crate::{RawStrategy, Result};

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
    fn write_to(value: &T, buf: &mut Vec<u8>) {
        buf.extend_from_slice(value.to_bytes().as_ref());
    }
}

impl<T> CompressionStrategy<T> for LZ4Strategy<T>
where
    T: LZ4VecValue,
{
    fn compress(values: &[T]) -> Result<Vec<u8>> {
        Ok(compress_prepend_size(&Self::values_to_bytes(values)))
    }

    fn decompress(bytes: &[u8], expected_len: usize) -> Result<Vec<T>> {
        let decompressed = decompress_size_prepended(bytes)?;
        Self::bytes_to_values(&decompressed, expected_len)
    }
}
