use std::{
    fmt::{self, Debug, Display},
    fs, io, result,
};

pub type Result<T, E = Error> = result::Result<T, E>;

/// Error types for rawdb operations.
#[derive(Debug)]
pub enum Error {
    IO(io::Error),
    TryLock(fs::TryLockError),

    // Region errors
    RegionNotFound,
    RegionAlreadyExists,
    RegionStillReferenced {
        ref_count: usize,
    },

    // Write errors
    WriteOutOfBounds {
        position: usize,
        region_len: usize,
    },

    // Truncate errors
    TruncateInvalid {
        from: usize,
        current_len: usize,
    },

    // Metadata errors
    InvalidRegionId,
    InvalidMetadataSize {
        expected: usize,
        actual: usize,
    },
    EmptyMetadata,

    // Layout errors
    RegionIndexMismatch,

    // Hole punching errors
    HolePunchFailed {
        start: usize,
        len: usize,
        source: io::Error,
    },
    HolePunchUnsupported,
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::IO(value)
    }
}

impl From<fs::TryLockError> for Error {
    fn from(value: fs::TryLockError) -> Self {
        Self::TryLock(value)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::IO(error) => Display::fmt(&error, f),
            Error::TryLock(_) => write!(f, "Database is locked by another process"),

            Error::RegionNotFound => write!(f, "Region not found"),
            Error::RegionAlreadyExists => write!(f, "Region already exists"),
            Error::RegionStillReferenced { ref_count } => write!(
                f,
                "Cannot remove region: still held by {} reference(s)",
                ref_count - 1
            ),

            Error::WriteOutOfBounds {
                position,
                region_len,
            } => write!(
                f,
                "Write position {} is beyond region length {}",
                position, region_len
            ),

            Error::TruncateInvalid { from, current_len } => write!(
                f,
                "Cannot truncate to {} bytes (current length: {})",
                from, current_len
            ),

            Error::InvalidRegionId => write!(f, "Invalid region ID"),
            Error::InvalidMetadataSize { expected, actual } => write!(
                f,
                "Invalid metadata size: expected {} bytes, got {}",
                expected, actual
            ),
            Error::EmptyMetadata => write!(f, "Empty region metadata"),

            Error::RegionIndexMismatch => write!(f, "Region index mismatch in layout"),

            Error::HolePunchFailed { start, len, source } => write!(
                f,
                "Failed to punch hole at offset {} (length {}): {}",
                start, len, source
            ),
            Error::HolePunchUnsupported => {
                write!(f, "Hole punching is not supported on this platform")
            }
        }
    }
}

impl std::error::Error for Error {}
