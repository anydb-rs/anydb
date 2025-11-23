use std::{fs, io, path::Path};

use crate::{Error, Result};

/// Storage format selection for stored vectors.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Format {
    /// Pcodec compressed storage (best for numerical data with sequential access).
    Compressed,
    /// Raw uncompressed storage (best for random access or non-compressible data).
    #[default]
    Raw,
}

impl Format {
    pub fn write(&self, path: &Path) -> Result<(), io::Error> {
        fs::write(path, self.as_bytes())
    }

    pub fn is_raw(&self) -> bool {
        *self == Self::Raw
    }

    pub fn is_compressed(&self) -> bool {
        *self == Self::Compressed
    }

    fn as_bytes(&self) -> Vec<u8> {
        if self.is_compressed() {
            vec![1]
        } else {
            vec![0]
        }
    }

    pub fn validate(&self, path: &Path) -> Result<()> {
        let Ok(bytes) = fs::read(path) else {
            return Ok(()); // File doesn't exist yet
        };

        let prev_format = match bytes.as_slice() {
            [0] => Self::Raw,
            [1] => Self::Compressed,
            _ => return Err(Error::CorruptedFormatFile),
        };

        if prev_format != *self {
            return Err(Error::DifferentCompressionMode);
        }

        Ok(())
    }
}
