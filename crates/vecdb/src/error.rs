use std::{
    fmt::{self, Debug, Display},
    fs, io, result, time,
};

use crate::Version;

pub type Result<T, E = Error> = result::Result<T, E>;

/// Error types for vecdb operations.
#[derive(Debug)]
pub enum Error {
    IO(io::Error),
    Format(fmt::Error),
    TryLockError(fs::TryLockError),
    ZeroCopyError,
    SystemTimeError(time::SystemTimeError),
    PCO(pco::errors::PcoError),
    RawDB(rawdb::Error),
    #[cfg(feature = "serde_json")]
    SerdeJSON(serde_json::Error),
    #[cfg(feature = "sonic-rs")]
    SonicRS(sonic_rs::Error),

    Str(&'static str),
    String(String),

    WrongLength,
    WrongEndian,
    DifferentVersion {
        found: Version,
        expected: Version,
    },
    IndexTooHigh {
        index: usize,
        len: usize,
    },
    ExpectVecToHaveIndex,
    FailedKeyTryIntoUsize,
    DifferentCompressionMode,
}

impl From<time::SystemTimeError> for Error {
    fn from(value: time::SystemTimeError) -> Self {
        Self::SystemTimeError(value)
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::IO(value)
    }
}

impl From<fmt::Error> for Error {
    fn from(value: fmt::Error) -> Self {
        Self::Format(value)
    }
}

#[cfg(feature = "serde_json")]
impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self::SerdeJSON(value)
    }
}

#[cfg(feature = "sonic-rs")]
impl From<sonic_rs::Error> for Error {
    fn from(value: sonic_rs::Error) -> Self {
        Self::SonicRS(value)
    }
}

impl From<rawdb::Error> for Error {
    fn from(value: rawdb::Error) -> Self {
        Self::RawDB(value)
    }
}

impl From<fs::TryLockError> for Error {
    fn from(value: fs::TryLockError) -> Self {
        Self::TryLockError(value)
    }
}

impl From<pco::errors::PcoError> for Error {
    fn from(value: pco::errors::PcoError) -> Self {
        Self::PCO(value)
    }
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

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::IO(error) => Display::fmt(&error, f),
            Error::Format(error) => Display::fmt(&error, f),
            #[cfg(feature = "serde_json")]
            Error::SerdeJSON(error) => Display::fmt(&error, f),
            #[cfg(feature = "sonic-rs")]
            Error::SonicRS(error) => Display::fmt(&error, f),
            Error::RawDB(error) => Display::fmt(&error, f),
            Error::TryLockError(_) => write!(
                f,
                "Couldn't lock file. It must be already opened by another process."
            ),
            Error::PCO(error) => Display::fmt(&error, f),
            Error::SystemTimeError(error) => Display::fmt(&error, f),
            Error::ZeroCopyError => write!(f, "ZeroCopy error"),

            Error::WrongEndian => write!(f, "Wrong endian"),
            Error::DifferentVersion { found, expected } => {
                write!(
                    f,
                    "Different version found: {found:?}, expected: {expected:?}"
                )
            }
            Error::IndexTooHigh { index, len } => {
                write!(f, "Index too high: index: {index}, len: {len}")
            }
            Error::ExpectVecToHaveIndex => write!(f, "Expect vec to have index"),
            Error::FailedKeyTryIntoUsize => write!(f, "Failed to convert key to usize"),
            Error::DifferentCompressionMode => write!(f, "Different compression mode chosen"),
            Error::WrongLength => write!(f, "Wrong length"),
            Error::Str(s) => write!(f, "{s}"),
            Error::String(s) => write!(f, "{s}"),
        }
    }
}

impl std::error::Error for Error {}
