use std::marker::PhantomData;

use crate::{BytesVecValue, Result, variants::raw::RawStrategy};

/// Serialization strategy using the Bytes trait with portable byte order.
///
/// Implements little-endian serialization for cross-platform compatibility.
#[derive(Debug, Clone, Copy)]
pub struct BytesStrategy<T>(PhantomData<T>);

impl<T: BytesVecValue> RawStrategy<T> for BytesStrategy<T> {
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
