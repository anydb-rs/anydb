use std::sync::{
    Arc,
    atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed},
};

use parking_lot::RwLock;

use crate::{AnyVec, ReadOnlyClone, ReadableVec, TypedVec, VecIndex, Version, short_type_name};

/// Budget gate for [`CachedVec`] materialization.
///
/// When the budget is exhausted, reads fall through to the inner vec without caching.
pub trait CachedVecBudget: Send + Sync {
    /// Attempts to reserve one cache slot given the entry's access count.
    /// Implementations may enforce a minimum access threshold or evict entries.
    fn try_reserve(&self, access_count: u64) -> bool;
}

impl CachedVecBudget for AtomicUsize {
    #[inline]
    fn try_reserve(&self, _: u64) -> bool {
        self.fetch_update(Relaxed, Relaxed, |n| if n > 0 { Some(n - 1) } else { None })
            .is_ok()
    }
}

/// Budget that always allows materialization (used by [`CachedVec::wrap`]).
struct NoBudget;

impl CachedVecBudget for NoBudget {
    #[inline]
    fn try_reserve(&self, _: u64) -> bool {
        true
    }
}

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
    cache: Arc<RwLock<(usize, Version, Arc<[V::T]>)>>,
    budget: &'static dyn CachedVecBudget,
    access_count: Option<Arc<AtomicU64>>,
}

impl<V: TypedVec + Clone> Clone for CachedVec<V> {
    #[inline(always)]
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            cache: self.cache.clone(),
            budget: self.budget,
            access_count: self.access_count.clone(),
        }
    }
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
    fn try_cached(&self) -> Option<Arc<[V::T]>> {
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

impl<V: TypedVec> AnyVec for CachedVec<V> {
    #[inline(always)]
    fn version(&self) -> Version {
        self.inner.version()
    }

    #[inline(always)]
    fn name(&self) -> &str {
        self.inner.name()
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline(always)]
    fn index_type_to_string(&self) -> &'static str {
        self.inner.index_type_to_string()
    }

    #[inline(always)]
    fn region_names(&self) -> Vec<String> {
        Vec::new()
    }

    #[inline(always)]
    fn value_type_to_size_of(&self) -> usize {
        size_of::<V::T>()
    }

    #[inline(always)]
    fn value_type_to_string(&self) -> &'static str {
        short_type_name::<V::T>()
    }
}

impl<V: TypedVec> TypedVec for CachedVec<V> {
    type I = V::I;
    type T = V::T;
}

impl<V> ReadOnlyClone for CachedVec<V>
where
    V: TypedVec + ReadOnlyClone,
    V::ReadOnly: TypedVec,
{
    type ReadOnly = CachedVec<V::ReadOnly>;

    #[inline]
    fn read_only_clone(&self) -> Self::ReadOnly {
        CachedVec::wrap(self.inner.read_only_clone())
    }
}

impl<V: TypedVec + ReadableVec<V::I, V::T>> ReadableVec<V::I, V::T> for CachedVec<V> {
    #[inline]
    fn read_into_at(&self, from: usize, to: usize, buf: &mut Vec<V::T>) {
        if let Some(data) = self.try_cached() {
            let to = to.min(data.len());
            if from < to {
                buf.extend_from_slice(&data[from..to]);
            }
        } else {
            self.inner.read_into_at(from, to, buf);
        }
    }

    #[inline]
    fn for_each_range_dyn_at(&self, from: usize, to: usize, f: &mut dyn FnMut(V::T)) {
        if let Some(data) = self.try_cached() {
            let to = to.min(data.len());
            let from = from.min(to);
            for v in &data[from..to] {
                f(v.clone());
            }
        } else {
            self.inner.for_each_range_dyn_at(from, to, f);
        }
    }

    #[inline]
    fn fold_range_at<B, F: FnMut(B, V::T) -> B>(
        &self,
        from: usize,
        to: usize,
        init: B,
        mut f: F,
    ) -> B
    where
        Self: Sized,
    {
        if let Some(data) = self.try_cached() {
            let to = to.min(data.len());
            let from = from.min(to);
            let mut acc = init;
            for v in &data[from..to] {
                acc = f(acc, v.clone());
            }
            acc
        } else {
            self.inner.fold_range_at(from, to, init, f)
        }
    }

    #[inline]
    fn try_fold_range_at<B, E, F: FnMut(B, V::T) -> Result<B, E>>(
        &self,
        from: usize,
        to: usize,
        init: B,
        mut f: F,
    ) -> Result<B, E>
    where
        Self: Sized,
    {
        if let Some(data) = self.try_cached() {
            let to = to.min(data.len());
            let from = from.min(to);
            let mut acc = init;
            for v in &data[from..to] {
                acc = f(acc, v.clone())?;
            }
            Ok(acc)
        } else {
            self.inner.try_fold_range_at(from, to, init, f)
        }
    }

    #[inline]
    fn collect_one_at(&self, index: usize) -> Option<V::T> {
        if let Some(data) = self.try_cached() {
            data.get(index).cloned()
        } else {
            self.inner.collect_one_at(index)
        }
    }

    #[inline]
    fn read_sorted_into_at(&self, indices: &[usize], out: &mut Vec<V::T>) {
        if let Some(data) = self.try_cached() {
            out.reserve(indices.len());
            for &i in indices {
                if let Some(v) = data.get(i) {
                    out.push(v.clone());
                }
            }
        } else {
            self.inner.read_sorted_into_at(indices, out);
        }
    }
}
