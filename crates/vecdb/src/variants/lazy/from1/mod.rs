use std::sync::Arc;

mod any_vec;
mod read_only_clone;
mod readable;
mod transform;
mod typed;

pub use transform::*;

use crate::{ReadableBoxedVec, VecIndex, VecValue, Version};

pub type ComputeFrom1<I, T, S1T> = fn(I, S1T) -> T;

/// Lazily computed vector deriving values on-the-fly from one source vector.
///
/// Unlike `EagerVec`, no data is stored on disk. Values are computed during
/// iteration by applying a function to the source vector's elements. Use when:
/// - Storage space is limited
/// - Computation is cheap relative to disk I/O
/// - Values are only accessed once or infrequently
///
/// For frequently accessed derived data, prefer `EagerVec` for better performance.
#[derive(Clone)]
pub struct LazyVecFrom1<I, T, S1I, S1T>
where
    S1I: VecIndex,
    S1T: VecValue,
{
    pub(super) name: Arc<str>,
    pub(super) base_version: Version,
    pub(super) source: ReadableBoxedVec<S1I, S1T>,
    pub(super) compute: ComputeFrom1<I, T, S1T>,
}

impl<I, T, S1I, S1T> LazyVecFrom1<I, T, S1I, S1T>
where
    I: VecIndex,
    T: VecValue,
    S1I: VecIndex,
    S1T: VecValue,
{
    pub fn init(
        name: &str,
        version: Version,
        source: ReadableBoxedVec<S1I, S1T>,
        compute: ComputeFrom1<I, T, S1T>,
    ) -> Self {
        assert_eq!(
            I::to_string(),
            S1I::to_string(),
            "LazyVecFrom1 index type mismatch: expected {}, got {}",
            I::to_string(),
            S1I::to_string()
        );

        Self {
            name: Arc::from(name),
            base_version: version,
            source,
            compute,
        }
    }
}

impl<I, T, S1T> LazyVecFrom1<I, T, I, S1T>
where
    I: VecIndex,
    T: VecValue,
    S1T: VecValue,
{
    /// Create a lazy vec with a generic transform.
    /// Usage: `LazyVecFrom1::transformed::<Negate>(name, v, source)`
    pub fn transformed<F: UnaryTransform<S1T, T>>(
        name: &str,
        version: Version,
        source: ReadableBoxedVec<I, S1T>,
    ) -> Self {
        Self::init(name, version, source, |_, v| F::apply(v))
    }
}
