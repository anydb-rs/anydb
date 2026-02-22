use std::sync::Arc;

use crate::{
    AnyVec, READ_CHUNK_SIZE, ReadableBoxedVec, ReadableVec, TypedVec, VecIndex, VecValue, Version,
    short_type_name,
};

mod transform;

pub use transform::*;

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
    name: Arc<str>,
    base_version: Version,
    source1: ReadableBoxedVec<S1I, S1T>,
    source2: ReadableBoxedVec<S2I, S2T>,
    compute: ComputeFrom2<I, T, S1T, S2T>,
    s1_counts: bool,
    s2_counts: bool,
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

impl<I, T, S1I, S1T, S2I, S2T> AnyVec for LazyVecFrom2<I, T, S1I, S1T, S2I, S2T>
where
    I: VecIndex,
    T: VecValue,
    S1I: VecIndex,
    S1T: VecValue,
    S2I: VecIndex,
    S2T: VecValue,
{
    fn version(&self) -> Version {
        self.base_version + self.source1.version() + self.source2.version()
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn index_type_to_string(&self) -> &'static str {
        I::to_string()
    }

    fn len(&self) -> usize {
        let len1 = if self.s1_counts {
            self.source1.len()
        } else {
            usize::MAX
        };
        let len2 = if self.s2_counts {
            self.source2.len()
        } else {
            usize::MAX
        };
        len1.min(len2)
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

impl<I, T, S1I, S1T, S2I, S2T> LazyVecFrom2<I, T, S1I, S1T, S2I, S2T>
where
    I: VecIndex,
    T: VecValue,
    S1I: VecIndex,
    S1T: VecValue,
    S2I: VecIndex,
    S2T: VecValue,
{
    /// Chunked iteration over both sources with reusable buffers.
    /// Only 2 small buffers are allocated total (capacity min(4096, range)),
    /// reused across chunks via `clear()`.
    #[inline]
    fn chunked_for_each(&self, from: usize, to: usize, mut each: impl FnMut(T)) {
        let compute = self.compute;
        let cap = READ_CHUNK_SIZE.min(to.saturating_sub(from));
        let mut buf1 = Vec::with_capacity(cap);
        let mut buf2 = Vec::with_capacity(cap);
        let mut pos = from;
        while pos < to {
            let end = (pos + READ_CHUNK_SIZE).min(to);
            buf1.clear();
            buf2.clear();
            self.source1.read_into_at(pos, end, &mut buf1);
            self.source2.read_into_at(pos, end, &mut buf2);
            for (local, (v1, v2)) in buf1.drain(..).zip(buf2.drain(..)).enumerate() {
                each(compute(I::from(pos + local), v1, v2));
            }
            pos = end;
        }
    }
}

impl<I, T, S1I, S1T, S2I, S2T> ReadableVec<I, T> for LazyVecFrom2<I, T, S1I, S1T, S2I, S2T>
where
    I: VecIndex,
    T: VecValue,
    S1I: VecIndex,
    S1T: VecValue,
    S2I: VecIndex,
    S2T: VecValue,
{
    #[inline]
    fn read_into_at(&self, from: usize, to: usize, buf: &mut Vec<T>) {
        let to = to.min(self.len());
        buf.reserve(to.saturating_sub(from));
        self.chunked_for_each(from, to, |v| buf.push(v));
    }

    #[inline]
    fn for_each_range_dyn_at(&self, from: usize, to: usize, f: &mut dyn FnMut(T)) {
        self.chunked_for_each(from, to.min(self.len()), f);
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
        let mut acc = Some(init);
        self.chunked_for_each(from, to, |v| {
            acc = Some(f(acc.take().unwrap(), v));
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
        let mut acc: Option<std::result::Result<B, E>> = Some(Ok(init));
        self.chunked_for_each(from, to, |v| {
            if let Some(Ok(a)) = acc.take() {
                acc = Some(f(a, v));
            }
        });
        acc.unwrap()
    }

    #[inline]
    fn collect_one_at(&self, index: usize) -> Option<T> {
        if index >= self.len() {
            return None;
        }
        let v1 = self.source1.collect_one_at(index)?;
        let v2 = self.source2.collect_one_at(index)?;
        Some((self.compute)(I::from(index), v1, v2))
    }
}

impl<I, T, S1I, S1T, S2I, S2T> TypedVec for LazyVecFrom2<I, T, S1I, S1T, S2I, S2T>
where
    I: VecIndex,
    T: VecValue,
    S1I: VecIndex,
    S1T: VecValue,
    S2I: VecIndex,
    S2T: VecValue,
{
    type I = I;
    type T = T;
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
