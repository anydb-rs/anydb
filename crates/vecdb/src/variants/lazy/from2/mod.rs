use std::sync::Arc;

mod any_vec;
mod readable;
mod transform;
mod typed;

pub use transform::*;

use crate::{ReadableBoxedVec, VecIndex, VecValue, Version};

pub type ComputeFrom2<I, T, S1T, S2T> = fn(I, S1T, S2T) -> T;

/// Lazily computed vector deriving values from two source vectors.
///
/// Values are computed on-the-fly during iteration using a provided function.
/// Nothing is stored on disk - all values are recomputed each time they're accessed.
#[derive(Clone)]
pub struct LazyVecFrom2<I, T, S1I, S1T, S2I, S2T>
where
    S1I: VecIndex,
    S1T: VecValue,
    S2I: VecIndex,
    S2T: VecValue,
{
    pub(super) name: Arc<str>,
    pub(super) base_version: Version,
    pub(super) source1: ReadableBoxedVec<S1I, S1T>,
    pub(super) source2: ReadableBoxedVec<S2I, S2T>,
    pub(super) compute: ComputeFrom2<I, T, S1T, S2T>,
    pub(super) s1_counts: bool,
    pub(super) s2_counts: bool,
}

impl<I, T, S1I, S1T, S2I, S2T> LazyVecFrom2<I, T, S1I, S1T, S2I, S2T>
where
    I: VecIndex,
    T: VecValue,
    S1I: VecIndex,
    S1T: VecValue,
    S2I: VecIndex,
    S2T: VecValue,
{
    pub fn init(
        name: &str,
        version: Version,
        source1: ReadableBoxedVec<S1I, S1T>,
        source2: ReadableBoxedVec<S2I, S2T>,
        compute: ComputeFrom2<I, T, S1T, S2T>,
    ) -> Self {
        let target = I::to_string();
        let s1 = source1.index_type_to_string();
        let s2 = source2.index_type_to_string();

        assert!(
            s1 == target || s2 == target,
            "LazyVecFrom2: at least one source must have index type {}, got {} and {}",
            target,
            s1,
            s2
        );

        let s1_counts = s1 == target;
        let s2_counts = s2 == target;

        Self {
            name: Arc::from(name),
            base_version: version,
            source1,
            source2,
            compute,
            s1_counts,
            s2_counts,
        }
    }
}

impl<I, T, S1T, S2T> LazyVecFrom2<I, T, I, S1T, I, S2T>
where
    I: VecIndex,
    T: VecValue,
    S1T: VecValue,
    S2T: VecValue,
{
    /// Create a lazy vec with a generic binary transform.
    /// Usage: `LazyVecFrom2::transformed::<Divide>(name, v, source1, source2)`
    pub fn transformed<F: BinaryTransform<S1T, S2T, T>>(
        name: &str,
        version: Version,
        source1: ReadableBoxedVec<I, S1T>,
        source2: ReadableBoxedVec<I, S2T>,
    ) -> Self {
        Self::init(name, version, source1, source2, |_, a, b| F::apply(a, b))
    }
}
