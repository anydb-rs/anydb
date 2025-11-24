use std::marker::PhantomData;

use crate::{BytesVecValue, Result, variants::raw::inner::SerializeStrategy};

/// Bytes trait-based serialization strategy.
/// Uses the Bytes trait for custom serialization.
#[derive(Debug, Clone, Copy)]
pub struct BytesStrategy<T>(PhantomData<T>);

impl<T: BytesVecValue> SerializeStrategy<T> for BytesStrategy<T> {
    const SIZE: usize = T::SIZE;

    #[inline]
    fn read(bytes: &[u8]) -> Result<T> {
        T::from_bytes(bytes)
    }

    #[inline]
    fn write(value: &T) -> Vec<u8> {
        value.to_bytes()
    }
}
