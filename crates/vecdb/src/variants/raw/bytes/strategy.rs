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
    fn write(value: &T) -> Vec<u8> {
        value.to_bytes()
    }
}
