use crate::{Error, GiB, PAGE_SIZE, Result, regions::Regions};

pub const SIZE_OF_REGION_METADATA: usize = PAGE_SIZE; // 4096 bytes for atomic writes
const SIZE_OF_U64: usize = std::mem::size_of::<u64>();
const MAX_REGION_ID_LEN: usize = 1024;
const MAX_RESERVED_SIZE: usize = 1024 * GiB; // 1 TiB

/// Metadata tracking a region's location, size, and identity.
#[derive(Debug, Clone)]
pub struct RegionMetadata {
    /// Starting offset in the database file (must be multiple of 4096).
    start: usize,
    /// Current length of data in the region.
    len: usize,
    /// Reserved space for the region (must be multiple of 4096, >= len).
    reserved: usize,
    /// Unique identifier for the region.
    id: String,
}

impl RegionMetadata {
    fn validate_id(id: &str) {
        assert!(!id.is_empty(), "Region id must not be empty");
        assert!(
            id.len() <= MAX_REGION_ID_LEN,
            "Region id must be <= {} bytes",
            MAX_REGION_ID_LEN
        );
        assert!(
            !id.chars().any(|c| c.is_control()),
            "Region id must not contain control characters"
        );
    }

    pub fn new(id: String, start: usize, len: usize, reserved: usize) -> Self {
        assert!(start.is_multiple_of(PAGE_SIZE));
        assert!(reserved >= PAGE_SIZE);
        assert!(reserved.is_multiple_of(PAGE_SIZE));
        assert!(len <= reserved);
        Self::validate_id(&id);

        Self {
            id,
            len,
            reserved,
            start,
        }
    }

    #[inline(always)]
    pub fn start(&self) -> usize {
        self.start
    }

    #[inline]
    pub fn set_start(&mut self, start: usize) {
        assert!(start.is_multiple_of(PAGE_SIZE));
        self.start = start;
    }

    #[allow(clippy::len_without_is_empty)]
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn set_len(&mut self, len: usize) {
        assert!(len <= self.reserved());
        self.len = len;
    }

    #[inline(always)]
    pub fn reserved(&self) -> usize {
        self.reserved
    }

    #[inline(always)]
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn set_id(&mut self, id: String) {
        Self::validate_id(&id);
        self.id = id;
    }

    pub fn set_reserved(&mut self, reserved: usize) {
        assert!(self.len() <= reserved);
        assert!(reserved >= PAGE_SIZE);
        assert!(reserved.is_multiple_of(PAGE_SIZE));
        assert!(reserved <= MAX_RESERVED_SIZE);

        self.reserved = reserved;
    }

    /// Returns the amount of reserved space not yet used by data.
    #[inline(always)]
    pub fn remaining(&self) -> usize {
        self.reserved - self.len
    }

    pub(crate) fn write(&self, index: usize, regions: &Regions) {
        regions.write_at(index, &self.to_bytes())
    }

    pub(crate) fn flush(&self, index: usize, regions: &Regions) -> Result<()> {
        regions
            .mmap()
            .flush_range(index * SIZE_OF_REGION_METADATA, SIZE_OF_REGION_METADATA)?;
        Ok(())
    }

    /// Serialize to bytes using little endian encoding
    fn to_bytes(&self) -> [u8; SIZE_OF_REGION_METADATA] {
        let mut pos = 0;
        let mut bytes = [0u8; SIZE_OF_REGION_METADATA];

        bytes[pos..pos + SIZE_OF_U64].copy_from_slice(&(self.start as u64).to_le_bytes());
        pos += SIZE_OF_U64;

        bytes[pos..pos + SIZE_OF_U64].copy_from_slice(&(self.len as u64).to_le_bytes());
        pos += SIZE_OF_U64;

        bytes[pos..pos + SIZE_OF_U64].copy_from_slice(&(self.reserved as u64).to_le_bytes());
        pos += SIZE_OF_U64;

        let id_bytes = self.id.as_bytes();
        let id_len = id_bytes.len();
        bytes[pos..pos + SIZE_OF_U64].copy_from_slice(&(id_len as u64).to_le_bytes());
        pos += SIZE_OF_U64;

        bytes[pos..pos + id_len].copy_from_slice(id_bytes);

        bytes
    }

    /// Deserialize from bytes using little endian encoding
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != SIZE_OF_REGION_METADATA {
            return Err(Error::InvalidMetadataSize {
                expected: SIZE_OF_REGION_METADATA,
                actual: bytes.len(),
            });
        }

        let start = u64::from_le_bytes(bytes[0..8].try_into().unwrap()) as usize;
        let len = u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as usize;
        let reserved = u64::from_le_bytes(bytes[16..24].try_into().unwrap()) as usize;
        let id_len = u64::from_le_bytes(bytes[24..32].try_into().unwrap()) as usize;

        let id = String::from_utf8(bytes[32..32 + id_len].to_vec())
            .map_err(|_| Error::InvalidRegionId)?;

        if start == 0 && len == 0 && reserved == 0 && id_len == 0 {
            return Err(Error::EmptyMetadata);
        }

        // Loaded from disk, so not dirty
        Ok(Self {
            id,
            start,
            len,
            reserved,
        })
    }
}
