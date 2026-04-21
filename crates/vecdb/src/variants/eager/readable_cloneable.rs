use crate::{ReadableBoxedVec, ReadableCloneableVec, StoredVec};

use super::EagerVec;

impl<V> ReadableCloneableVec<V::I, V::T> for EagerVec<V>
where
    V: StoredVec,
{
    #[inline]
    fn read_only_boxed_clone(&self) -> ReadableBoxedVec<V::I, V::T> {
        self.0.read_only_boxed_clone()
    }
}
