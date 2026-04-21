use crate::StoredVec;

use super::EagerVec;

impl<V> StoredVec for EagerVec<V>
where
    V: StoredVec,
{
    type ReadOnly = V::ReadOnly;

    #[inline]
    fn read_only_clone(&self) -> Self::ReadOnly {
        self.0.read_only_clone()
    }
}
