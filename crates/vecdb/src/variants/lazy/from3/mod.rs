use std::sync::Arc;

mod any_vec;
mod readable;
mod typed;

use crate::{ReadableBoxedVec, VecIndex, VecValue, Version};

pub type ComputeFrom3<I, T, S1T, S2T, S3T> = fn(I, S1T, S2T, S3T) -> T;

/// Lazily computed vector deriving values from three source vectors.
///
/// Values are computed on-the-fly during iteration using a provided function.
/// Nothing is stored on disk - all values are recomputed each time they're accessed.
#[derive(Clone)]
pub struct LazyVecFrom3<I, T, S1I, S1T, S2I, S2T, S3I, S3T>
where
    S1I: VecIndex,
    S1T: VecValue,
    S2I: VecIndex,
    S2T: VecValue,
    S3I: VecIndex,
    S3T: VecValue,
{
    pub(super) name: Arc<str>,
    pub(super) base_version: Version,
    pub(super) source1: ReadableBoxedVec<S1I, S1T>,
    pub(super) source2: ReadableBoxedVec<S2I, S2T>,
    pub(super) source3: ReadableBoxedVec<S3I, S3T>,
    pub(super) compute: ComputeFrom3<I, T, S1T, S2T, S3T>,
    pub(super) s1_counts: bool,
    pub(super) s2_counts: bool,
    pub(super) s3_counts: bool,
}

impl<I, T, S1I, S1T, S2I, S2T, S3I, S3T> LazyVecFrom3<I, T, S1I, S1T, S2I, S2T, S3I, S3T>
where
    I: VecIndex,
    T: VecValue,
    S1I: VecIndex,
    S1T: VecValue,
    S2I: VecIndex,
    S2T: VecValue,
    S3I: VecIndex,
    S3T: VecValue,
{
    pub fn init(
        name: &str,
        version: Version,
        source1: ReadableBoxedVec<S1I, S1T>,
        source2: ReadableBoxedVec<S2I, S2T>,
        source3: ReadableBoxedVec<S3I, S3T>,
        compute: ComputeFrom3<I, T, S1T, S2T, S3T>,
    ) -> Self {
        let target = I::to_string();
        let s1 = source1.index_type_to_string();
        let s2 = source2.index_type_to_string();
        let s3 = source3.index_type_to_string();

        assert!(
            s1 == target || s2 == target || s3 == target,
            "LazyVecFrom3: at least one source must have index type {}, got {}, {}, and {}",
            target,
            s1,
            s2,
            s3
        );

        let s1_counts = s1 == target;
        let s2_counts = s2 == target;
        let s3_counts = s3 == target;

        Self {
            name: Arc::from(name),
            base_version: version,
            source1,
            source2,
            source3,
            compute,
            s1_counts,
            s2_counts,
            s3_counts,
        }
    }
}
