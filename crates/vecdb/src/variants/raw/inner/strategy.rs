use crate::Result;

/// Trait for serialization strategies used by RawVecInner.
pub trait RawStrategy<T>: Send + Sync {
    /// Deserialize a value from bytes.
    fn read(bytes: &[u8]) -> Result<T>;

    /// Serialize a value to bytes.
    fn write(value: &T) -> Vec<u8>;
}
