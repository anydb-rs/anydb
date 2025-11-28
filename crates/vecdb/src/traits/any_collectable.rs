use crate::{AnyVec, CollectableVec, TypedVec, i64_to_usize};

/// Type-erased trait for collectable vectors.
pub trait AnyCollectableVec: AnyVec {
    /// Returns the number of items in the specified range.
    fn range_count(&self, from: Option<i64>, to: Option<i64>) -> usize {
        let len = self.len();
        let from = from.map(|i| i64_to_usize(i, len));
        let to = to.map(|i| i64_to_usize(i, len));
        (from.unwrap_or_default()..to.unwrap_or(len)).count()
    }

    /// Returns the total size in bytes of items in the specified range.
    fn range_weight(&self, from: Option<i64>, to: Option<i64>) -> usize {
        self.range_count(from, to) * self.value_type_to_size_of()
    }
}

impl<V> AnyCollectableVec for V
where
    V: TypedVec,
    V: CollectableVec<V::I, V::T>,
{
}
