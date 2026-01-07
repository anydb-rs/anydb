use crate::{
    AnyVec, BoxedVecIterator, Error, IterableBoxedVec, IterableVec, Result, TypedVec,
    TypedVecIterator, VecIndex, VecValue, Version, short_type_name,
};

mod iterator;
mod transform;

pub use iterator::*;
pub use transform::*;

pub type ComputeFrom1<I, T, S1I, S1T> =
    for<'a> fn(I, &mut dyn TypedVecIterator<I = S1I, T = S1T, Item = S1T>) -> Option<T>;

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
    S1T: Clone,
{
    name: String,
    base_version: Version,
    source: IterableBoxedVec<S1I, S1T>,
    compute: ComputeFrom1<I, T, S1I, S1T>,
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
        source: IterableBoxedVec<S1I, S1T>,
        compute: ComputeFrom1<I, T, S1I, S1T>,
    ) -> Self {
        assert_eq!(
            I::to_string(),
            S1I::to_string(),
            "LazyVecFrom1 index type mismatch: expected {}, got {}",
            I::to_string(),
            S1I::to_string()
        );

        Self {
            name: name.to_string(),
            base_version: version,
            source,
            compute,
        }
    }

    fn version(&self) -> Version {
        self.base_version + self.source.version()
    }

    /// Read a single value at the given index.
    /// Creates an iterator internally, so prefer `into_iter()` for multiple reads.
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

impl<'a, I, T, S1I, S1T> IntoIterator for &'a LazyVecFrom1<I, T, S1I, S1T>
where
    I: VecIndex,
    T: VecValue + 'a,
    S1I: VecIndex,
    S1T: VecValue,
{
    type Item = T;
    type IntoIter = LazyVecFrom1Iterator<'a, I, T, S1I, S1T>;

    fn into_iter(self) -> Self::IntoIter {
        LazyVecFrom1Iterator::new(self)
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
        self.version()
    }

    fn name(&self) -> &str {
        self.name.as_str()
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
        vec![]
    }
}

impl<I, T, S1I, S1T> IterableVec<I, T> for LazyVecFrom1<I, T, S1I, S1T>
where
    I: VecIndex,
    T: VecValue,
    S1I: VecIndex,
    S1T: VecValue,
{
    fn iter(&self) -> BoxedVecIterator<'_, I, T> {
        Box::new(self.into_iter())
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
        source: IterableBoxedVec<I, S1T>,
    ) -> Self {
        Self::init(name, version, source, |i, iter| iter.get(i).map(F::apply))
    }
}
