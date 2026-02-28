use std::sync::Arc;

use crate::{
    AnyVec, ReadOnlyClone, ReadableBoxedVec, ReadableVec, TypedVec, VecIndex, VecValue, Version,
    short_type_name,
};

mod transform;

pub use transform::*;

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
    name: Arc<str>,
    base_version: Version,
    source: ReadableBoxedVec<S1I, S1T>,
    compute: ComputeFrom1<I, T, S1T>,
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

impl<I, T, S1I, S1T> ReadOnlyClone for LazyVecFrom1<I, T, S1I, S1T>
where
    I: VecIndex,
    T: VecValue,
    S1I: VecIndex,
    S1T: VecValue,
{
    type ReadOnly = Self;

    fn read_only_clone(&self) -> Self {
        self.clone()
    }
}

impl<I, T, S1I, S1T> AnyVec for LazyVecFrom1<I, T, S1I, S1T>
where
    I: VecIndex,
    T: VecValue,
    S1I: VecIndex,
    S1T: VecValue,
{
    fn version(&self) -> Version {
        self.base_version + self.source.version()
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn index_type_to_string(&self) -> &'static str {
        I::to_string()
    }

    fn len(&self) -> usize {
        self.source.len()
    }

    #[inline]
    fn value_type_to_size_of(&self) -> usize {
        size_of::<T>()
    }

    #[inline]
    fn value_type_to_string(&self) -> &'static str {
        short_type_name::<T>()
    }

    #[inline]
    fn region_names(&self) -> Vec<String> {
        Vec::new()
    }
}

impl<I, T, S1I, S1T> ReadableVec<I, T> for LazyVecFrom1<I, T, S1I, S1T>
where
    I: VecIndex,
    T: VecValue,
    S1I: VecIndex,
    S1T: VecValue,
{
    #[inline]
    fn read_into_at(&self, from: usize, to: usize, buf: &mut Vec<T>) {
        let to = to.min(self.len());
        let compute = self.compute;
        buf.reserve(to.saturating_sub(from));
        let mut offset = from;
        self.source.for_each_range_dyn_at(from, to, &mut |v| {
            buf.push(compute(I::from(offset), v));
            offset += 1;
        });
    }

    #[inline]
    fn for_each_range_dyn_at(&self, from: usize, to: usize, f: &mut dyn FnMut(T)) {
        let to = to.min(self.len());
        let compute = self.compute;
        let mut offset = from;
        self.source.for_each_range_dyn_at(from, to, &mut |v| {
            f(compute(I::from(offset), v));
            offset += 1;
        });
    }

    #[inline]
    fn fold_range_at<B, F: FnMut(B, T) -> B>(&self, from: usize, to: usize, init: B, mut f: F) -> B
    where
        Self: Sized,
    {
        let to = to.min(self.len());
        if from >= to {
            return init;
        }
        let compute = self.compute;
        let mut offset = from;
        let mut acc = Some(init);
        self.source.for_each_range_dyn_at(from, to, &mut |v| {
            acc = Some(f(acc.take().unwrap(), compute(I::from(offset), v)));
            offset += 1;
        });
        acc.unwrap()
    }

    #[inline]
    fn try_fold_range_at<B, E, F: FnMut(B, T) -> std::result::Result<B, E>>(
        &self,
        from: usize,
        to: usize,
        init: B,
        mut f: F,
    ) -> std::result::Result<B, E>
    where
        Self: Sized,
    {
        let to = to.min(self.len());
        if from >= to {
            return Ok(init);
        }
        let compute = self.compute;
        let mut offset = from;
        let mut acc: Option<std::result::Result<B, E>> = Some(Ok(init));
        self.source.for_each_range_dyn_at(from, to, &mut |v| {
            if let Some(Ok(a)) = acc.take() {
                acc = Some(f(a, compute(I::from(offset), v)));
            }
            offset += 1;
        });
        acc.unwrap()
    }

    #[inline]
    fn collect_one_at(&self, index: usize) -> Option<T> {
        let v = self.source.collect_one_at(index)?;
        Some((self.compute)(I::from(index), v))
    }
}

impl<I, T, S1I, S1T> TypedVec for LazyVecFrom1<I, T, S1I, S1T>
where
    I: VecIndex,
    T: VecValue,
    S1I: VecIndex,
    S1T: VecValue,
{
    type I = I;
    type T = T;
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
