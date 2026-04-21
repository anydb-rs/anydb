use super::Stamp;

impl From<u64> for Stamp {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<Stamp> for u64 {
    fn from(value: Stamp) -> Self {
        value.0
    }
}
