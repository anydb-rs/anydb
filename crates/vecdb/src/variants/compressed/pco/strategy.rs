use std::marker::PhantomData;

use pco::standalone::{simple_decompress, simpler_compress};

use crate::{Error, RawStrategy, Result, likely};

use super::super::inner::CompressionStrategy;
use super::value::{AsInnerSlice, FromInnerSlice, PcoVecValue};

const PCO_COMPRESSION_LEVEL: usize = 4;

/// Pcodec compression strategy for numerical data.
#[derive(Debug, Clone, Copy)]
pub struct PcodecStrategy<T>(PhantomData<T>);

impl<T> RawStrategy<T> for PcodecStrategy<T>
where
    T: PcoVecValue,
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

impl<T> CompressionStrategy<T> for PcodecStrategy<T>
where
    T: PcoVecValue,
{
    fn compress(values: &[T]) -> Result<Vec<u8>> {
        Ok(simpler_compress(values.as_inner_slice(), PCO_COMPRESSION_LEVEL).unwrap())
    }

    fn decompress(bytes: &[u8], expected_len: usize) -> Result<Vec<T>> {
        let vec: Vec<T::NumberType> = simple_decompress(bytes)?;
        let vec = T::from_inner_slice(vec);

        if likely(vec.len() == expected_len) {
            return Ok(vec);
        }

        Err(Error::DecompressionMismatch {
            expected_len,
            actual_len: vec.len(),
        })
    }
}
