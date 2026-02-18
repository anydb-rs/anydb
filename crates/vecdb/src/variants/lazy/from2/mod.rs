use crate::{
    AnyVec, ReadableBoxedVec, ReadableVec, TypedVec,
    VecIndex, VecValue, Version, short_type_name,
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
    name: String,
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
            name: name.to_string(),
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
        self.name.as_str()
    }

    fn index_type_to_string(&self) -> &'static str {
        I::to_string()
    }

    fn len(&self) -> usize {
        let len1 = if self.s1_counts { self.source1.len() } else { usize::MAX };
        let len2 = if self.s2_counts { self.source2.len() } else { usize::MAX };
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
    fn for_each_range_dyn(&self, from: usize, to: usize, f: &mut dyn FnMut(T)) {
        let to = to.min(self.len());
        let compute = self.compute;
        let s2_vals = self.source2.collect_range_dyn(from, to);
        let mut s2_iter = s2_vals.into_iter();
        let mut i = from;
        self.source1.for_each_range_dyn(from, to, &mut |v1| {
            let v2 = s2_iter.next().unwrap();
            f(compute(I::from(i), v1, v2));
            i += 1;
        });
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
