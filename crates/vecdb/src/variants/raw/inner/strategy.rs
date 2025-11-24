use crate::Result;

/// Trait for serialization strategies used by RawVecInner.
pub trait SerializeStrategy<T>: Send + Sync {
    /// The fixed size in bytes of the serialized value.
    const SIZE: usize;

    /// Deserialize a value from bytes.
    fn read(bytes: &[u8]) -> Result<T>;

    /// Serialize a value to bytes.
    fn write(value: &T) -> Vec<u8>;
}
