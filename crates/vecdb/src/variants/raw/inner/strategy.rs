use crate::Result;

/// Serialization strategy for raw storage vectors.
///
/// Abstracts the serialization mechanism to support both zerocopy-based
/// memory mapping and custom Bytes trait serialization.
pub trait RawStrategy<T>: Send + Sync + Clone {
    /// Deserializes a value from its byte representation.
    fn read(bytes: &[u8]) -> Result<T>;

    /// Serializes a value by appending its byte representation to the buffer.
    fn write_to_vec(value: &T, buf: &mut Vec<u8>);

    /// Serializes a value directly into a fixed-size slice.
    fn write_to_slice(value: &T, dst: &mut [u8]);
}
