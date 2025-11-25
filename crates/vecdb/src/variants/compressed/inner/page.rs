use crate::{Bytes, Error, Result};

#[derive(Debug, Clone)]
#[repr(C)]
pub struct Page {
    pub start: u64,
    pub bytes: u32,
    pub values: u32,
}

impl Page {
    pub fn new(start: u64, bytes: u32, values: u32) -> Self {
        Self {
            start,
            bytes,
            values,
        }
    }
}

impl Bytes for Page {
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(16);
        bytes.extend_from_slice(&self.start.to_bytes());
        bytes.extend_from_slice(&self.bytes.to_bytes());
        bytes.extend_from_slice(&self.values.to_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 16 {
            return Err(Error::WrongLength);
        }

        let start = u64::from_bytes(&bytes[0..8])?;
        let bytes_val = u32::from_bytes(&bytes[8..12])?;
        let values = u32::from_bytes(&bytes[12..16])?;

        Ok(Self {
            start,
            bytes: bytes_val,
            values,
        })
    }
}
