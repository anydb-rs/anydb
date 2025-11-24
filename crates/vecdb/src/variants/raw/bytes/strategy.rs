use std::marker::PhantomData;

use crate::{BytesVecValue, Result, variants::raw::RawStrategy};

/// Bytes trait-based serialization strategy.
/// Uses the Bytes trait for custom serialization.
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
