use std::sync::Arc;

use parking_lot::RwLock;
use rawdb::Region;

use crate::{Bytes, Error, Result, Stamp, Version};

use super::Format;

const HEADER_VERSION: Version = Version::ONE;
pub(crate) const HEADER_OFFSET: usize = size_of::<HeaderInner>();

#[derive(Debug, Clone)]
pub struct Header {
    inner: Arc<RwLock<HeaderInner>>,
    modified: bool,
}

impl Header {
    pub fn create_and_write(region: &Region, vec_version: Version, format: Format) -> Result<Self> {
        let inner = HeaderInner::create_and_write(region, vec_version, format)?;
        Ok(Self {
            inner: Arc::new(RwLock::new(inner)),
            modified: false,
        })
    }

    pub fn import_and_verify(
        region: &Region,
        vec_version: Version,
        format: Format,
    ) -> Result<Self> {
        let inner = HeaderInner::import_and_verify(region, vec_version, format)?;
        Ok(Self {
            inner: Arc::new(RwLock::new(inner)),
            modified: false,
        })
    }

    pub fn update_stamp(&mut self, stamp: Stamp) {
        self.modified = true;
        self.inner.write().stamp = stamp;
    }

    pub fn update_computed_version(&mut self, computed_version: Version) {
        self.modified = true;
        self.inner.write().computed_version = computed_version;
    }

    #[inline(always)]
    pub fn modified(&self) -> bool {
        self.modified
    }

    #[inline(always)]
    pub fn vec_version(&self) -> Version {
        self.inner.read().vec_version
    }

    #[inline(always)]
    pub fn computed_version(&self) -> Version {
        self.inner.read().computed_version
    }

    #[inline(always)]
    pub fn stamp(&self) -> Stamp {
        self.inner.read().stamp
    }

    pub fn write(&mut self, region: &Region) -> Result<()> {
        self.inner.read().write(region)?;
        self.modified = false;
        Ok(())
    }
}

#[derive(Debug, Clone)]
#[repr(C)]
struct HeaderInner {
    pub header_version: Version,
    pub vec_version: Version,
    pub computed_version: Version,
    pub stamp: Stamp,
    pub format: Format,
    pub padding: [u8; 31],
}

impl HeaderInner {
    pub fn create_and_write(region: &Region, vec_version: Version, format: Format) -> Result<Self> {
        let header = Self {
            header_version: HEADER_VERSION,
            vec_version,
            computed_version: Version::default(),
            stamp: Stamp::default(),
            format,
            padding: Default::default(),
        };
        header.write(region)?;
        Ok(header)
    }

    pub fn write(&self, region: &Region) -> Result<()> {
        region.write_at(&self.to_bytes(), 0)?;
        Ok(())
    }

    pub fn import_and_verify(
        region: &Region,
        vec_version: Version,
        format: Format,
    ) -> Result<Self> {
        let len = region.meta().len();

        if len < HEADER_OFFSET {
            return Err(Error::WrongLength);
        }

        let reader = region.create_reader();
        let vec = reader.unchecked_read(0, HEADER_OFFSET);
        let header = HeaderInner::from_bytes(vec)?;

        if header.header_version != HEADER_VERSION {
            return Err(Error::DifferentVersion {
                found: header.header_version,
                expected: HEADER_VERSION,
            });
        }
        if header.vec_version != vec_version {
            return Err(Error::DifferentVersion {
                found: header.vec_version,
                expected: vec_version,
            });
        }

        if header.format != format {
            return Err(Error::DifferentFormat {
                found: header.format,
                expected: format,
            });
        }

        Ok(header)
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(HEADER_OFFSET);
        bytes.extend(self.header_version.to_bytes());
        bytes.extend(self.vec_version.to_bytes());
        bytes.extend(self.computed_version.to_bytes());
        bytes.extend(self.stamp.to_bytes());
        bytes.extend(self.format.to_bytes());
        bytes.extend_from_slice(&self.padding);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < HEADER_OFFSET {
            return Err(Error::WrongLength);
        }
        let header_version = Version::from_bytes(&bytes[0..8])?;
        let vec_version = Version::from_bytes(&bytes[8..16])?;
        let computed_version = Version::from_bytes(&bytes[16..24])?;
        let stamp = Stamp::from_bytes(&bytes[24..32])?;
        let format = Format::from_bytes(&bytes[32..33])?;
        let padding = [0u8; 31];
        Ok(Self {
            header_version,
            vec_version,
            computed_version,
            stamp,
            format,
            padding,
        })
    }
}
