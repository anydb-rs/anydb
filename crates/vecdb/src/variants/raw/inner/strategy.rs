use crate::Result;

/// Implements `RawStrategy` for a Bytes-based strategy type.
/// All Bytes-based strategies (BytesStrategy, LZ4Strategy, ZstdStrategy, PcodecStrategy)
/// share identical read/write implementations using the Bytes trait.
macro_rules! impl_bytes_raw_strategy {
    ($strategy:ident, $value_trait:path) => {
        impl<T> $crate::RawStrategy<T> for $strategy<T>
        where
            T: $value_trait,
        {
            #[inline(always)]
            fn read(bytes: &[u8]) -> $crate::Result<T> {
                T::from_bytes(bytes)
            }

            #[inline(always)]
            fn write_to_vec(value: &T, buf: &mut Vec<u8>) {
                buf.extend_from_slice(value.to_bytes().as_ref());
            }

            #[inline(always)]
            fn write_to_slice(value: &T, dst: &mut [u8]) {
                dst.copy_from_slice(value.to_bytes().as_ref());
            }
        }
    };
}

pub(crate) use impl_bytes_raw_strategy;

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

    /// Reads a single T from a raw byte pointer at the given byte offset.
    ///
    /// For native-layout types, this compiles to a single `mov` instruction,
    /// bypassing slice creation, bounds checking, and Result overhead.
    ///
    /// Only called by raw vec iterators (mmap/io). Compressed strategies
    /// use page-based decoding instead — this default is never reached.
    ///
    /// # Safety
    /// - `ptr.add(byte_offset)` must be valid for reading `size_of::<T>()` bytes.
    /// - The bytes at that location must be a valid serialized T.
    unsafe fn read_from_ptr(_ptr: *const u8, _byte_offset: usize) -> T {
        unimplemented!("read_from_ptr is only implemented for raw strategies")
    }

    /// Bulk-decode values from contiguous bytes into `buf`.
    ///
    /// Only called by raw vec iterators (mmap/io). Compressed strategies
    /// use page-based decoding instead — this default is never reached.
    fn read_bulk(_bytes: &[u8], _buf: &mut Vec<T>, _count: usize) {
        unimplemented!("read_bulk is only implemented for raw strategies")
    }
}
