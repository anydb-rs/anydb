use std::{fs, io, path::Path};

mod add;
mod bytes;
mod conversions;
mod display;
mod sum;
mod try_from_path;

use crate::Bytes;

/// Version tracking for data schema and computed values.
///
/// Used to detect when stored data needs to be recomputed due to changes
/// in computation logic or source data versions. Supports validation
/// against persisted versions to ensure compatibility.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[must_use = "Version values should be used for compatibility checks"]
pub struct Version(pub(super) u32);

impl Version {
    pub const ZERO: Self = Self(0);
    pub const ONE: Self = Self(1);
    pub const TWO: Self = Self(2);

    pub const fn new(v: u32) -> Self {
        Self(v)
    }

    pub fn write(&self, path: &Path) -> Result<(), io::Error> {
        fs::write(path, self.to_bytes().as_ref())
    }

    pub fn swap_bytes(self) -> Self {
        Self(self.0.swap_bytes())
    }
}
