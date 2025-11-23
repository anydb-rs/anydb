/// Storage format selection for stored vectors.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Format {
    /// Pcodec compressed storage (best for numerical data with sequential access).
    Compressed,
    /// Raw uncompressed storage (best for random access or non-compressible data).
    #[default]
    Raw,
}

impl Format {
    #[inline]
    pub fn is_raw(&self) -> bool {
        *self == Self::Raw
    }

    #[inline]
    pub fn is_compressed(&self) -> bool {
        *self == Self::Compressed
    }
}
