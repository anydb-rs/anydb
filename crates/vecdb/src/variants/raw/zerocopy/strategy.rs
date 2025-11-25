use std::marker::PhantomData;

use crate::{Error, Result, ZeroCopyVecValue, variants::raw::RawStrategy};

/// Serialization strategy using zerocopy for native byte order access.
///
/// Uses direct memory mapping in native byte order - not portable across endianness.
#[derive(Debug, Clone, Copy)]
pub struct ZeroCopyStrategy<T>(PhantomData<T>);

impl<T: ZeroCopyVecValue> RawStrategy<T> for ZeroCopyStrategy<T> {
    #[inline(always)]
    fn read(bytes: &[u8]) -> Result<T> {
        T::read_from_prefix(bytes)
            .map(|(v, _)| v)
            .map_err(|_| Error::ZeroCopyError)
    }

    #[inline(always)]
    fn write(value: &T) -> Vec<u8> {
        value.as_bytes().to_vec()
    }
}
