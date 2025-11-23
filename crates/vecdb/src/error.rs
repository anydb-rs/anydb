use std::{fmt, fs, io, result, time};

use thiserror::Error;

use crate::{Stamp, Version};

pub type Result<T, E = Error> = result::Result<T, E>;

/// Error types for vecdb operations.
#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    IO(#[from] io::Error),
    #[error(transparent)]
    Format(#[from] fmt::Error),
    #[error("Couldn't lock file. It must be already opened by another process.")]
    TryLockError(#[from] fs::TryLockError),
    #[error("ZeroCopy error")]
    ZeroCopyError,
    #[error(transparent)]
    SystemTimeError(#[from] time::SystemTimeError),
    #[error(transparent)]
    PCO(#[from] pco::errors::PcoError),
    #[error(transparent)]
    RawDB(#[from] rawdb::Error),
    #[cfg(feature = "serde_json")]
    #[error(transparent)]
    SerdeJSON(#[from] serde_json::Error),
    #[cfg(feature = "sonic-rs")]
    #[error(transparent)]
    SonicRS(#[from] sonic_rs::Error),

    #[error("Wrong length")]
    WrongLength,
    #[error("Wrong endian")]
    WrongEndian,
    #[error("Different version found: {found:?}, expected: {expected:?}")]
    DifferentVersion { found: Version, expected: Version },
    #[error("Index too high: index: {index}, len: {len}")]
    IndexTooHigh { index: usize, len: usize },
    #[error("Expect vec to have index")]
    ExpectVecToHaveIndex,
    #[error("Failed to convert key to usize")]
    FailedKeyTryIntoUsize,
    #[error("Different compression mode chosen")]
    DifferentCompressionMode,
    #[error("Corrupted format file")]
    CorruptedFormatFile,
    #[error("Version cannot be zero, can't verify endianness otherwise")]
    VersionCannotBeZero,
    #[error("Stamp mismatch: file stamp {file:?} != vec stamp {vec:?}")]
    StampMismatch { file: Stamp, vec: Stamp },
    #[error("Corrupted region: invalid length {region_len}")]
    CorruptedRegion { region_len: usize },
    #[error("Decompression mismatch: expected {expected_len} values, got {actual_len}")]
    DecompressionMismatch {
        expected_len: usize,
        actual_len: usize,
    },
    #[error("Cannot remove CompressedVec: pages still referenced")]
    PagesStillReferenced,
}

impl<A, B, C> From<zerocopy::error::ConvertError<A, B, C>> for Error {
    fn from(_: zerocopy::error::ConvertError<A, B, C>) -> Self {
        Self::ZeroCopyError
    }
}

impl<A, B> From<zerocopy::error::SizeError<A, B>> for Error {
    fn from(_: zerocopy::error::SizeError<A, B>) -> Self {
        Self::ZeroCopyError
    }
}
