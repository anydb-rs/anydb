use crate::{
    AnyVec, BoxedVecIterator, Error, IterableBoxedVec, IterableVec, Result, TypedVec,
    TypedVecIterator, VecIndex, VecValue, Version, short_type_name,
};

mod iterator;
mod transform;

pub use iterator::*;
pub use transform::*;

pub type ComputeFrom2<I, T, S1I, S1T, S2I, S2T> = for<'a> fn(
    I,
    &mut dyn TypedVecIterator<I = S1I, T = S1T, Item = S1T>,
    &mut dyn TypedVecIterator<I = S2I, T = S2T, Item = S2T>,
) -> Option<T>;

/// Lazily computed vector deriving values from two source vectors.
///
/// Values are computed on-the-fly during iteration using a provided function.
/// Nothing is stored on disk - all values are recomputed each time they're accessed.
#[derive(Clone)]
pub struct LazyVecFrom2<I, T, S1I, S1T, S2I, S2T>
where
    S1T: Clone,
    S2T: Clone,
{
    name: String,
    version: Version,
    source1: IterableBoxedVec<S1I, S1T>,
    source2: IterableBoxedVec<S2I, S2T>,
    compute: ComputeFrom2<I, T, S1I, S1T, S2I, S2T>,
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
        source1: IterableBoxedVec<S1I, S1T>,
        source2: IterableBoxedVec<S2I, S2T>,
        compute: ComputeFrom2<I, T, S1I, S1T, S2I, S2T>,
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

        Self {
            name: name.to_string(),
            version: version + source1.version() + source2.version(),
            source1,
            source2,
            compute,
        }
    }

    fn version(&self) -> Version {
        self.version
    }

    /// Read a single value at the given index.
    /// Creates iterators internally, so prefer `into_iter()` for multiple reads.
    #[inline]
    pub fn read_once(&self, index: I) -> Result<T> {
        self.into_iter()
            .get(index)
            .ok_or(Error::IndexTooHigh {
                index: index.to_usize(),
                len: self.len(),
            })
    }
}

impl<'a, I, T, S1I, S1T, S2I, S2T> IntoIterator for &'a LazyVecFrom2<I, T, S1I, S1T, S2I, S2T>
where
    I: VecIndex,
    T: VecValue + 'a,
    S1I: VecIndex,
    S1T: VecValue,
    S2I: VecIndex,
    S2T: VecValue,
{
    type Item = T;
    type IntoIter = LazyVecFrom2Iterator<'a, I, T, S1I, S1T, S2I, S2T>;

    fn into_iter(self) -> Self::IntoIter {
        LazyVecFrom2Iterator::new(self)
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
        vec![]
    }
}

impl<I, T, S1I, S1T, S2I, S2T> IterableVec<I, T> for LazyVecFrom2<I, T, S1I, S1T, S2I, S2T>
where
    I: VecIndex,
    T: VecValue,
    S1I: VecIndex,
    S1T: VecValue,
    S2I: VecIndex,
    S2T: VecValue,
{
    fn iter(&self) -> BoxedVecIterator<'_, I, T> {
        Box::new(self.into_iter())
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
        source1: IterableBoxedVec<I, S1T>,
        source2: IterableBoxedVec<I, S2T>,
    ) -> Self {
        Self::init(name, version, source1, source2, |i, iter1, iter2| {
            match (iter1.get(i), iter2.get(i)) {
                (Some(v1), Some(v2)) => Some(F::apply(v1, v2)),
                _ => None,
            }
        })
    }
}
