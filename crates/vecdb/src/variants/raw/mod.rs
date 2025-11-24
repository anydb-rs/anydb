mod bytes;
mod inner;
mod iterators;
mod zerocopy;

pub use bytes::*;
use inner::*;
pub use iterators::*;
pub use zerocopy::*;

use crate::{AnyVec, BoxedVecIterator, IterableVec, Result, TypedVec, VecIndex, VecValue, Version};

/// Enum wrapper for raw storage vectors, supporting both zerocopy and bytes formats.
///
/// This allows runtime selection between ZeroCopyVec and BytesVec storage formats
/// based on the value type's capabilities.
#[derive(Debug, Clone)]
#[must_use = "Vector should be stored to keep data accessible"]
pub enum RawVec<I, T> {
    ZeroCopy(ZeroCopyVec<I, T>),
    Bytes(BytesVec<I, T>),
}

impl<I, T> AnyVec for RawVec<I, T>
where
    I: VecIndex,
    T: VecValue,
{
    #[inline]
    fn version(&self) -> Version {
        match self {
            RawVec::ZeroCopy(v) => v.version(),
            RawVec::Bytes(v) => v.version(),
        }
    }

    #[inline]
    fn name(&self) -> &str {
        match self {
            RawVec::ZeroCopy(v) => v.name(),
            RawVec::Bytes(v) => v.name(),
        }
    }

    #[inline]
    fn len(&self) -> usize {
        match self {
            RawVec::ZeroCopy(v) => v.len(),
            RawVec::Bytes(v) => v.len(),
        }
    }

    #[inline]
    fn index_type_to_string(&self) -> &'static str {
        match self {
            RawVec::ZeroCopy(v) => v.index_type_to_string(),
            RawVec::Bytes(v) => v.index_type_to_string(),
        }
    }

    #[inline]
    fn value_type_to_size_of(&self) -> usize {
        match self {
            RawVec::ZeroCopy(v) => v.value_type_to_size_of(),
            RawVec::Bytes(v) => v.value_type_to_size_of(),
        }
    }

    #[inline]
    fn region_names(&self) -> Vec<String> {
        match self {
            RawVec::ZeroCopy(v) => v.region_names(),
            RawVec::Bytes(v) => v.region_names(),
        }
    }
}

impl<I, T> TypedVec for RawVec<I, T>
where
    I: VecIndex,
    T: VecValue,
{
    type I = I;
    type T = T;
}

impl<I, T> IterableVec<I, T> for RawVec<I, T>
where
    I: VecIndex,
    T: VecValue,
{
    fn iter(&self) -> BoxedVecIterator<'_, I, T> {
        match self {
            RawVec::ZeroCopy(v) => v.iter(),
            RawVec::Bytes(v) => v.iter(),
        }
    }
}
