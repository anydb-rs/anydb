use crate::{
    AnyVec, BoxedVecIterator, IterableBoxedVec, IterableVec, TypedVec, TypedVecIterator, VecIndex,
    VecValue, Version, short_type_name,
};

mod iterator;

pub use iterator::*;

pub type ComputeFrom3<I, T, S1I, S1T, S2I, S2T, S3I, S3T> = for<'a> fn(
    I,
    &mut dyn TypedVecIterator<I = S1I, T = S1T, Item = S1T>,
    &mut dyn TypedVecIterator<I = S2I, T = S2T, Item = S2T>,
    &mut dyn TypedVecIterator<I = S3I, T = S3T, Item = S3T>,
) -> Option<T>;

/// Lazily computed vector deriving values from three source vectors.
///
/// Values are computed on-the-fly during iteration using a provided function.
/// Nothing is stored on disk - all values are recomputed each time they're accessed.
#[derive(Clone)]
pub struct LazyVecFrom3<I, T, S1I, S1T, S2I, S2T, S3I, S3T>
where
    S1T: Clone,
    S2T: Clone,
    S3T: Clone,
{
    name: String,
    version: Version,
    source1: IterableBoxedVec<S1I, S1T>,
    source2: IterableBoxedVec<S2I, S2T>,
    source3: IterableBoxedVec<S3I, S3T>,
    compute: ComputeFrom3<I, T, S1I, S1T, S2I, S2T, S3I, S3T>,
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
        source1: IterableBoxedVec<S1I, S1T>,
        source2: IterableBoxedVec<S2I, S2T>,
        source3: IterableBoxedVec<S3I, S3T>,
        compute: ComputeFrom3<I, T, S1I, S1T, S2I, S2T, S3I, S3T>,
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

        Self {
            name: name.to_string(),
            version,
            source1,
            source2,
            source3,
            compute,
        }
    }

    fn version(&self) -> Version {
        self.version
    }
}

impl<'a, I, T, S1I, S1T, S2I, S2T, S3I, S3T> IntoIterator
    for &'a LazyVecFrom3<I, T, S1I, S1T, S2I, S2T, S3I, S3T>
where
    I: VecIndex,
    T: VecValue + 'a,
    S1I: VecIndex,
    S1T: VecValue,
    S2I: VecIndex,
    S2T: VecValue,
    S3I: VecIndex,
    S3T: VecValue,
{
    type Item = T;
    type IntoIter = LazyVecFrom3Iterator<'a, I, T, S1I, S1T, S2I, S2T, S3I, S3T>;

    fn into_iter(self) -> Self::IntoIter {
        LazyVecFrom3Iterator::new(self)
    }
}

impl<I, T, S1I, S1T, S2I, S2T, S3I, S3T> AnyVec for LazyVecFrom3<I, T, S1I, S1T, S2I, S2T, S3I, S3T>
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
    fn version(&self) -> Version {
        self.version()
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn index_type_to_string(&self) -> &'static str {
        I::to_string()
    }

    fn len(&self) -> usize {
        let len1 = if self.source1.index_type_to_string() == I::to_string() {
            self.source1.len()
        } else {
            usize::MAX
        };
        let len2 = if self.source2.index_type_to_string() == I::to_string() {
            self.source2.len()
        } else {
            usize::MAX
        };
        let len3 = if self.source3.index_type_to_string() == I::to_string() {
            self.source3.len()
        } else {
            usize::MAX
        };
        len1.min(len2).min(len3)
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
        vec![]
    }
}

impl<I, T, S1I, S1T, S2I, S2T, S3I, S3T> IterableVec<I, T>
    for LazyVecFrom3<I, T, S1I, S1T, S2I, S2T, S3I, S3T>
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
    fn iter(&self) -> BoxedVecIterator<'_, I, T> {
        Box::new(self.into_iter())
    }
}

impl<I, T, S1I, S1T, S2I, S2T, S3I, S3T> TypedVec
    for LazyVecFrom3<I, T, S1I, S1T, S2I, S2T, S3I, S3T>
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
    type I = I;
    type T = T;
}
