use std::marker::PhantomData;

use crate::{Error, Result, ZeroCopyVecValue, variants::raw::inner::SerializeStrategy};

/// Zerocopy-based serialization strategy.
/// Uses zerocopy traits for direct memory mapping without copying.
#[derive(Debug, Clone, Copy)]
pub struct ZeroCopyStrategy<T>(PhantomData<T>);

impl<T: ZeroCopyVecValue> SerializeStrategy<T> for ZeroCopyStrategy<T> {
    const SIZE: usize = size_of::<T>();

    #[inline]
    fn read(bytes: &[u8]) -> Result<T> {
        T::read_from_prefix(bytes)
            .map(|(v, _)| v)
            .map_err(|_| Error::ZeroCopyError)
    }

    #[inline]
    fn write(value: &T) -> Vec<u8> {
        value.as_bytes().to_vec()
    }
}
