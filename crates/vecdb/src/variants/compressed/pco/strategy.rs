use std::marker::PhantomData;

use crate::{Error, Result, likely};

use super::value::{AsInnerSlice, FromInnerSlice, PcodecVecValue};
use super::super::inner::CompressionStrategy;

const PCO_COMPRESSION_LEVEL: usize = 4;

/// Pcodec compression strategy for numerical data.
#[derive(Debug, Clone, Copy)]
pub struct PcodecStrategy<T>(PhantomData<T>);

impl<T> CompressionStrategy<T> for PcodecStrategy<T>
where
    T: PcodecVecValue,
{
    fn compress(values: &[T]) -> Result<Vec<u8>> {
        Ok(pco::standalone::simpler_compress(values.as_inner_slice(), PCO_COMPRESSION_LEVEL).unwrap())
    }

    fn decompress(bytes: &[u8], expected_len: usize) -> Result<Vec<T>> {
        let vec: Vec<T::NumberType> = pco::standalone::simple_decompress(bytes)?;
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
