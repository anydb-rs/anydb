mod bytes;

/// Metadata for a page in a CompressedVec.
///
/// Each page stores a chunk of values, either compressed or raw.
/// The high bit of `values` encodes whether the page is raw (uncompressed).
/// Raw pages are used for the last partial page to avoid recompression on every write.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct Page {
    /// Absolute byte offset in the region where page data starts
    pub start: u64,
    /// Number of bytes on disk (compressed or raw)
    pub bytes: u32,
    /// Number of values in this page. High bit encodes raw flag.
    pub(super) values: u32,
}

impl Page {
    const RAW_FLAG: u32 = 1 << 31;

    pub fn compressed(start: u64, bytes: u32, values: u32) -> Self {
        debug_assert!(values & Self::RAW_FLAG == 0, "values too large");
        Self {
            start,
            bytes,
            values,
        }
    }

    pub fn raw(start: u64, bytes: u32, values: u32) -> Self {
        debug_assert!(values & Self::RAW_FLAG == 0, "values too large");
        Self {
            start,
            bytes,
            values: values | Self::RAW_FLAG,
        }
    }

    #[inline]
    pub fn is_raw(&self) -> bool {
        self.values & Self::RAW_FLAG != 0
    }

    #[inline]
    pub fn values_count(&self) -> u32 {
        self.values & !Self::RAW_FLAG
    }

    #[inline]
    pub fn end(&self) -> u64 {
        self.start + self.bytes as u64
    }
}
