use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering::Relaxed},
};

use parking_lot::RwLock;

mod any_vec;
mod budget;
mod clone;
mod read_only_clone;
mod readable;
mod typed;

pub use budget::{CachedVecBudget, NoBudget};

use crate::{ReadOnlyClone, ReadableVec, StoredVec, TypedVec, VecIndex, Version};

static NO_BUDGET: NoBudget = NoBudget;

/// Cached wrapper around any readable vec, refreshed when len or version changes.
///
/// Wraps a concrete vec `V` and adds an in-memory cache layer.
/// Reads check the cache first; on miss, the inner vec is read and cached.
///
/// For writes, access the inner vec directly via the `inner` field.
///
/// When constructed with a budget, materialization is gated: if the budget
/// is exhausted, reads fall through to the inner vec without caching.
pub struct CachedVec<V: TypedVec> {
    pub inner: V,
    #[allow(clippy::type_complexity)]
    pub(super) cache: Arc<RwLock<(usize, Version, Arc<[V::T]>)>>,
    pub(super) budget: &'static dyn CachedVecBudget,
    pub(super) access_count: Option<Arc<AtomicU64>>,
}

impl<V: TypedVec> CachedVec<V> {
    fn empty() -> (usize, Version, Arc<[V::T]>) {
        (0, Version::ZERO, Arc::from(&[] as &[V::T]))
    }

    pub fn wrap(inner: V) -> Self {
        Self {
            inner,
            cache: Arc::new(RwLock::new(Self::empty())),
            budget: &NO_BUDGET,
            access_count: None,
        }
    }

    pub fn wrap_budgeted(
        inner: V,
        budget: &'static dyn CachedVecBudget,
        access_count: Arc<AtomicU64>,
    ) -> Self {
        Self {
            inner,
            cache: Arc::new(RwLock::new(Self::empty())),
            budget,
            access_count: Some(access_count),
        }
    }

    #[inline(always)]
    pub fn version(&self) -> Version {
        self.inner.version()
    }

    pub fn clear(&self) {
        *self.cache.write() = Self::empty();
        if let Some(c) = &self.access_count {
            c.store(0, Relaxed);
        }
    }
}

impl<V: TypedVec + ReadableVec<V::I, V::T>> CachedVec<V> {
    /// Returns the full cached snapshot. Always materializes on miss (ignores budget).
    #[inline(always)]
    pub fn cached(&self) -> Arc<[V::T]> {
        self.materialize(false).unwrap()
    }

    /// Returns the value at the given typed index from the cached snapshot.
    #[inline(always)]
    pub fn get(&self, index: V::I) -> Option<V::T> {
        self.get_at(index.to_usize())
    }

    /// Returns the value at the given raw index from the cached snapshot.
    #[inline(always)]
    pub fn get_at(&self, index: usize) -> Option<V::T> {
        self.cached().get(index).cloned()
    }

    /// Returns `None` when budget is exhausted or below min access threshold.
    #[inline]
    pub(super) fn try_cached(&self) -> Option<Arc<[V::T]>> {
        self.materialize(true)
    }

    fn materialize(&self, check_budget: bool) -> Option<Arc<[V::T]>> {
        let len = self.inner.len();
        let version = self.inner.version();

        let count = self
            .access_count
            .as_ref()
            .map(|c| c.fetch_add(1, Relaxed) + 1)
            .unwrap_or(0);

        let cache = self.cache.read();
        if cache.0 == len && cache.1 == version {
            return Some(cache.2.clone());
        }
        drop(cache);

        if check_budget && !self.budget.try_reserve(count) {
            return None;
        }

        let data: Arc<[V::T]> = self.inner.collect_range_dyn(0, len).into();
        let mut cache = self.cache.write();
        if cache.0 == len && cache.1 == version {
            return Some(cache.2.clone());
        }
        *cache = (len, version, data.clone());

        Some(data)
    }
}

impl<V: StoredVec> CachedVec<V> {
    /// Boxes a read-only clone for use with type-erased APIs (e.g. LazyVecFrom1).
    #[inline]
    pub fn read_only_boxed_clone(&self) -> crate::ReadableBoxedVec<V::I, V::T> {
        Box::new(self.read_only_clone())
    }
}
