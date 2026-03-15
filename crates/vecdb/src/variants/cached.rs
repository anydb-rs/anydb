use std::sync::Arc;

use parking_lot::RwLock;

use crate::{ReadableBoxedVec, ReadableCloneableVec, VecIndex, VecValue, Version};

/// Cached snapshot of a readable vec, refreshed when len or version changes.
///
/// Cloning is cheap (Arc). All clones share the same cache.
#[derive(Clone)]
pub struct CachedVec<I: VecIndex, T: VecValue> {
    source: ReadableBoxedVec<I, T>,
    #[allow(clippy::type_complexity)]
    cache: Arc<RwLock<(usize, Version, Arc<[T]>)>>,
}

impl<I: VecIndex, T: VecValue> CachedVec<I, T> {
    pub fn new(source: &(impl ReadableCloneableVec<I, T> + 'static)) -> Self {
        Self {
            source: source.read_only_boxed_clone(),
            cache: Arc::new(RwLock::new((0, Version::ZERO, Arc::from(&[] as &[T])))),
        }
    }

    pub fn version(&self) -> Version {
        self.source.version()
    }

    pub fn get(&self) -> Arc<[T]> {
        let len = self.source.len();
        let version = self.source.version();
        {
            let cache = self.cache.read();
            if cache.0 == len && cache.1 == version {
                return cache.2.clone();
            }
        }
        let data: Arc<[T]> = self.source.collect_range_dyn(0, len).into();
        let mut cache = self.cache.write();
        *cache = (len, version, data.clone());
        data
    }
}
