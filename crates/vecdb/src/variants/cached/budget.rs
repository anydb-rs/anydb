use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

/// Budget gate for [`super::CachedVec`] materialization.
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

/// Budget that always allows materialization (used by [`super::CachedVec::wrap`]).
pub struct NoBudget;

impl CachedVecBudget for NoBudget {
    #[inline]
    fn try_reserve(&self, _: u64) -> bool {
        true
    }
}
