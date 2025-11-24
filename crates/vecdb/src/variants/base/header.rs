use std::sync::Arc;

use parking_lot::RwLock;
use rawdb::Region;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::{Error, Result, Stamp, Version};

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

    pub fn update_format(&mut self, format: Format) {
        self.modified = true;
        self.inner.write().compressed = ZeroCopyBool::from(format);
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

#[derive(Debug, Clone, FromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(C)]
struct HeaderInner {
    pub header_version: Version,
    pub vec_version: Version,
    pub computed_version: Version,
    pub stamp: Stamp,
    pub compressed: ZeroCopyBool,
    pub padding: [u8; 31],
}

impl HeaderInner {
    pub fn create_and_write(region: &Region, vec_version: Version, format: Format) -> Result<Self> {
        let header = Self {
            header_version: HEADER_VERSION,
            vec_version,
            computed_version: Version::default(),
            stamp: Stamp::default(),
            compressed: ZeroCopyBool::from(format),
            padding: Default::default(),
        };
        header.write(region)?;
        Ok(header)
    }

    pub fn write(&self, region: &Region) -> Result<()> {
        region.write_at(self.as_bytes(), 0)?;
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
        let header = HeaderInner::read_from_bytes(vec)?;

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
        if header.compressed.is_broken() {
            return Err(Error::WrongEndian);
        }
        if (header.compressed.is_true() && format.is_raw())
            || (header.compressed.is_false() && format.is_compressed())
        {
            return Err(Error::DifferentCompressionMode);
        }

        Ok(header)
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    FromBytes,
    IntoBytes,
    Immutable,
    KnownLayout,
)]
pub struct ZeroCopyBool(u8);

impl ZeroCopyBool {
    pub const TRUE: Self = Self(1);
    pub const FALSE: Self = Self(0);

    pub fn is_true(&self) -> bool {
        *self == Self::TRUE
    }

    pub fn is_false(&self) -> bool {
        *self == Self::FALSE
    }

    pub fn is_broken(&self) -> bool {
        *self > Self::TRUE
    }
}

impl From<Format> for ZeroCopyBool {
    fn from(value: Format) -> Self {
        if value.is_raw() {
            Self::FALSE
        } else {
            Self::TRUE
        }
    }
}
