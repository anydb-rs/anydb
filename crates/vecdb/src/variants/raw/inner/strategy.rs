use crate::Result;

/// Serialization strategy for raw storage vectors.
///
/// Abstracts the serialization mechanism to support both zerocopy-based
/// memory mapping and custom Bytes trait serialization.
pub trait RawStrategy<T>: Send + Sync + Clone {
    /// Deserializes a value from its byte representation.
    fn read(bytes: &[u8]) -> Result<T>;

    /// Serializes a value to its byte representation.
    fn write(value: &T) -> Vec<u8>;
}
