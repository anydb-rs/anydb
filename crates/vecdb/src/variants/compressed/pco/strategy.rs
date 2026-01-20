use std::marker::PhantomData;

use pco::ChunkConfig;
use pco::standalone::{simple_compress, simple_decompress, simple_decompress_into};

use crate::{Error, RawStrategy, Result, likely};

use super::{
    super::inner::CompressionStrategy,
    value::{AsInnerSlice, FromInnerSlice, PcoVecValue},
};

/// Returns the default ChunkConfig for pcodec compression.
fn chunk_config() -> ChunkConfig {
    ChunkConfig::default().with_enable_8_bit(true)
}

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
    fn write_to_vec(value: &T, buf: &mut Vec<u8>) {
        buf.extend_from_slice(value.to_bytes().as_ref());
    }

    #[inline(always)]
    fn write_to_slice(value: &T, dst: &mut [u8]) {
        dst.copy_from_slice(value.to_bytes().as_ref());
    }
}

impl<T> CompressionStrategy<T> for PcodecStrategy<T>
where
    T: PcoVecValue,
{
    fn compress(values: &[T]) -> Result<Vec<u8>> {
        Ok(simple_compress(values.as_inner_slice(), &chunk_config())?)
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

    fn decompress_into(bytes: &[u8], expected_len: usize, dst: &mut Vec<T>) -> Result<()> {
        dst.clear();
        dst.reserve(expected_len);

        // SAFETY: MaybeUninit<T::NumberType> has the same layout as T::NumberType.
        // simple_decompress_into will initialize the memory, and we only set_len
        // after initialization succeeds.
        let spare = dst.spare_capacity_mut();
        let slice = unsafe {
            std::slice::from_raw_parts_mut(spare.as_mut_ptr().cast::<T::NumberType>(), expected_len)
        };

        let progress = simple_decompress_into(bytes, slice)?;

        // SAFETY: simple_decompress_into initialized progress.n_processed elements.
        unsafe {
            dst.set_len(progress.n_processed);
        }

        if likely(progress.n_processed == expected_len) {
            return Ok(());
        }

        Err(Error::DecompressionMismatch {
            expected_len,
            actual_len: progress.n_processed,
        })
    }
}
