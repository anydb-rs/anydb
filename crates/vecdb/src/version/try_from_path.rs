use std::{fs, io::Read, path::Path};

use crate::{Bytes, Error};

use super::Version;

impl TryFrom<&Path> for Version {
    type Error = Error;
    fn try_from(value: &Path) -> Result<Self, Self::Error> {
        let mut buf = [0u8; size_of::<Self>()];
        fs::read(value)?.as_slice().read_exact(&mut buf)?;
        Self::from_bytes(&buf)
    }
}
