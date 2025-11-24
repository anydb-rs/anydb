mod inner;
mod iterators;
#[cfg(feature = "lz4")]
mod lz4;
#[cfg(feature = "pco")]
mod pco;
#[cfg(feature = "zstd")]
mod zstd;

pub(crate) use inner::*;
pub use iterators::*;
#[cfg(feature = "lz4")]
pub use lz4::*;
#[cfg(feature = "pco")]
pub use pco::*;
#[cfg(feature = "zstd")]
pub use zstd::*;

use crate::{AnyVec, BoxedVecIterator, IterableVec, TypedVec, VecIndex, VecValue, Version};

/// Enum wrapper for compressed storage vectors.
///
/// Supports Pcodec, LZ4, and Zstd compression algorithms.
#[derive(Debug, Clone)]
#[must_use = "Vector should be stored to keep data accessible"]
pub enum CompressedVec<I, T> {
    #[cfg(feature = "pco")]
    Pco(PcoVec<I, T>),
    #[cfg(feature = "lz4")]
    LZ4(LZ4Vec<I, T>),
    #[cfg(feature = "zstd")]
    Zstd(ZstdVec<I, T>),
}

impl<I, T> AnyVec for CompressedVec<I, T>
where
    I: VecIndex,
    T: VecValue,
{
    #[inline]
    fn version(&self) -> Version {
        match self {
            #[cfg(feature = "pco")]
            CompressedVec::Pco(v) => v.version(),
            #[cfg(feature = "lz4")]
            CompressedVec::LZ4(v) => v.version(),
            #[cfg(feature = "zstd")]
            CompressedVec::Zstd(v) => v.version(),
        }
    }

    #[inline]
    fn name(&self) -> &str {
        match self {
            #[cfg(feature = "pco")]
            CompressedVec::Pco(v) => v.name(),
            #[cfg(feature = "lz4")]
            CompressedVec::LZ4(v) => v.name(),
            #[cfg(feature = "zstd")]
            CompressedVec::Zstd(v) => v.name(),
        }
    }

    #[inline]
    fn len(&self) -> usize {
        match self {
            #[cfg(feature = "pco")]
            CompressedVec::Pco(v) => v.len(),
            #[cfg(feature = "lz4")]
            CompressedVec::LZ4(v) => v.len(),
            #[cfg(feature = "zstd")]
            CompressedVec::Zstd(v) => v.len(),
        }
    }

    #[inline]
    fn index_type_to_string(&self) -> &'static str {
        match self {
            #[cfg(feature = "pco")]
            CompressedVec::Pco(v) => v.index_type_to_string(),
            #[cfg(feature = "lz4")]
            CompressedVec::LZ4(v) => v.index_type_to_string(),
            #[cfg(feature = "zstd")]
            CompressedVec::Zstd(v) => v.index_type_to_string(),
        }
    }

    #[inline]
    fn value_type_to_size_of(&self) -> usize {
        match self {
            #[cfg(feature = "pco")]
            CompressedVec::Pco(v) => v.value_type_to_size_of(),
            #[cfg(feature = "lz4")]
            CompressedVec::LZ4(v) => v.value_type_to_size_of(),
            #[cfg(feature = "zstd")]
            CompressedVec::Zstd(v) => v.value_type_to_size_of(),
        }
    }

    #[inline]
    fn region_names(&self) -> Vec<String> {
        match self {
            #[cfg(feature = "pco")]
            CompressedVec::Pco(v) => v.region_names(),
            #[cfg(feature = "lz4")]
            CompressedVec::LZ4(v) => v.region_names(),
            #[cfg(feature = "zstd")]
            CompressedVec::Zstd(v) => v.region_names(),
        }
    }
}

impl<I, T> TypedVec for CompressedVec<I, T>
where
    I: VecIndex,
    T: VecValue,
{
    type I = I;
    type T = T;
}

impl<I, T> IterableVec<I, T> for CompressedVec<I, T>
where
    I: VecIndex,
    T: VecValue,
{
    fn iter(&self) -> BoxedVecIterator<'_, I, T> {
        match self {
            #[cfg(feature = "pco")]
            CompressedVec::Pco(v) => v.iter(),
            #[cfg(feature = "lz4")]
            CompressedVec::LZ4(v) => v.iter(),
            #[cfg(feature = "zstd")]
            CompressedVec::Zstd(v) => v.iter(),
        }
    }
}
