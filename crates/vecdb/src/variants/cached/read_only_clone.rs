use crate::{ReadOnlyClone, StoredVec};

use super::CachedVec;

impl<V: StoredVec> ReadOnlyClone for CachedVec<V> {
    type ReadOnly = CachedVec<V::ReadOnly>;

    #[inline]
    fn read_only_clone(&self) -> Self::ReadOnly {
        CachedVec {
            inner: self.inner.read_only_clone(),
            cache: self.cache.clone(),
            budget: self.budget,
            access_count: self.access_count.clone(),
        }
    }
}
