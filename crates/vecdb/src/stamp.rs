use crate::{Bytes, Error, Result, SIZE_OF_U64};

/// Marker for tracking when data was last modified.
///
/// Used for change tracking, rollback support, and ETag generation.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Stamp(u64);

impl Stamp {
    pub fn new(stamp: u64) -> Self {
        Self(stamp)
    }
}

impl From<u64> for Stamp {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<Stamp> for u64 {
    fn from(value: Stamp) -> Self {
        value.0
    }
}

impl Bytes for Stamp {
    #[inline]
    fn to_bytes(&self) -> Vec<u8> {
        self.0.to_le_bytes().to_vec()
    }

    #[inline]
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let arr: [u8; SIZE_OF_U64] = bytes.try_into().map_err(|_| Error::WrongLength)?;
        Ok(Self(u64::from_le_bytes(arr)))
    }
}
