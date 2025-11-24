/// Storage format selection for stored vectors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Format {
    // ============================================================================
    // Raw formats (uncompressed)
    // ============================================================================
    /// ZeroCopy raw storage using zerocopy for direct memory mapping.
    /// Best for random access and types that implement zerocopy traits.
    ZeroCopy,
    /// Bytes raw storage using custom Bytes trait serialization.
    /// Best for types that need custom serialization but still want raw storage.
    Bytes,

    // ============================================================================
    // Compressed formats
    // ============================================================================
    /// Pcodec compressed storage (best for numerical data with sequential access).
    Pcodec,
    /// LZ4 compressed storage (fast compression/decompression).
    LZ4,
    /// Zstd compressed storage (high compression ratio).
    Zstd,
}

impl Format {
    #[inline]
    pub fn is_raw(&self) -> bool {
        matches!(self, Self::ZeroCopy | Self::Bytes)
    }

    #[inline]
    pub fn is_compressed(&self) -> bool {
        matches!(self, Self::Pcodec | Self::LZ4 | Self::Zstd)
    }

    #[inline]
    pub fn is_zerocopy(&self) -> bool {
        *self == Self::ZeroCopy
    }

    #[inline]
    pub fn is_bytes(&self) -> bool {
        *self == Self::Bytes
    }

    #[inline]
    pub fn is_pcodec(&self) -> bool {
        *self == Self::Pcodec
    }

    #[inline]
    pub fn is_lz4(&self) -> bool {
        *self == Self::LZ4
    }

    #[inline]
    pub fn is_zstd(&self) -> bool {
        *self == Self::Zstd
    }
}
