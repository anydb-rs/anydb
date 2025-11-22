use std::fs::File;

use crate::Result;

/// Represents actual disk usage of a file (accounting for sparse files and holes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiskUsage(u64);

impl DiskUsage {
    /// Creates a DiskUsage from a file handle.
    #[cfg(unix)]
    pub fn from_file(file: &File) -> Result<Self> {
        use std::os::unix::io::AsRawFd;

        let mut stat: libc::stat = unsafe { std::mem::zeroed() };
        let result = unsafe { libc::fstat(file.as_raw_fd(), &mut stat) };
        if result == -1 {
            return Err(std::io::Error::last_os_error().into());
        }
        // st_blocks is in 512-byte units
        Ok(Self(stat.st_blocks as u64 * 512))
    }

    /// Creates a DiskUsage from a file handle.
    /// On non-Unix platforms, falls back to logical file size.
    #[cfg(not(unix))]
    pub fn from_file(file: &File) -> Result<Self> {
        Ok(Self(file.metadata()?.len()))
    }

    /// Returns the disk usage in bytes.
    #[inline]
    pub fn bytes(&self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for DiskUsage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        const KIB: u64 = 1024;
        const MIB: u64 = KIB * 1024;
        const GIB: u64 = MIB * 1024;

        let bytes = self.0;
        if bytes >= GIB {
            write!(f, "{:.1} GiB", bytes as f64 / GIB as f64)
        } else if bytes >= MIB {
            write!(f, "{:.1} MiB", bytes as f64 / MIB as f64)
        } else if bytes >= KIB {
            write!(f, "{:.1} KiB", bytes as f64 / KIB as f64)
        } else {
            write!(f, "{} B", bytes)
        }
    }
}
