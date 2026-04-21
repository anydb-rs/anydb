use super::Version;

impl From<Version> for u32 {
    fn from(value: Version) -> u32 {
        value.0
    }
}

impl From<Version> for usize {
    fn from(value: Version) -> usize {
        value.0 as usize
    }
}

impl From<u32> for Version {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

impl From<usize> for Version {
    fn from(value: usize) -> Self {
        assert!(value <= u32::MAX as usize, "Version overflow: {value}");
        Self(value as u32)
    }
}
