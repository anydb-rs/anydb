use std::sync::{
    Arc,
    atomic::{AtomicU8, Ordering},
};

/// Metadata tracking a region's location, size, and identity.
#[derive(Debug, Clone)]
pub struct RegionState(Arc<AtomicU8>);

impl RegionState {
    pub const IS_CLEAN: u8 = 0;
    pub const NEEDS_FLUSH: u8 = 1;
    pub const NEEDS_WRITE: u8 = 2;

    #[inline(always)]
    pub fn new() -> Self {
        Self(Arc::new(AtomicU8::new(Self::IS_CLEAN)))
    }

    #[inline(always)]
    fn load(&self) -> u8 {
        self.0.load(Ordering::Relaxed)
    }

    #[inline(always)]
    pub fn is_clean(&self) -> bool {
        self.load() == Self::IS_CLEAN
    }

    #[inline(always)]
    pub fn set_is_clean(&self) {
        self.0.store(Self::IS_CLEAN, Ordering::Relaxed);
    }

    #[inline(always)]
    #[allow(unused)]
    pub fn needs_flush(&self) -> bool {
        self.load() == Self::NEEDS_FLUSH
    }

    #[inline(always)]
    pub fn set_needs_flush(&self) {
        self.0.store(Self::NEEDS_FLUSH, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn needs_write(&self) -> bool {
        self.load() == Self::NEEDS_WRITE
    }

    #[inline(always)]
    pub fn set_needs_write(&self) {
        self.0.store(Self::NEEDS_WRITE, Ordering::Relaxed);
    }
}
