use crate::Result;

use super::AnyCollectableVec;

/// Type-erased trait for serializable vectors.
pub trait AnySerializableVec: AnyCollectableVec {
    fn collect_range_json_bytes(&self, from: Option<usize>, to: Option<usize>) -> Result<Vec<u8>>;
}

#[cfg(feature = "serde")]
impl<V> AnySerializableVec for V
where
    V: crate::TypedVec,
    V: crate::CollectableVec<V::I, V::T>,
    V::T: serde::Serialize,
{
    fn collect_range_json_bytes(&self, from: Option<usize>, to: Option<usize>) -> Result<Vec<u8>> {
        <Self as crate::CollectableVec<V::I, V::T>>::collect_range_json_bytes(self, from, to)
    }
}
