use std::marker::PhantomData;

use crate::{Error, Result, ZeroCopyVecValue, variants::raw::RawStrategy};

/// Serialization strategy using zerocopy for native byte order access.
///
/// Uses direct memory mapping in native byte order - not portable across endianness.
#[derive(Debug, Clone, Copy)]
pub struct ZeroCopyStrategy<T>(PhantomData<T>);

impl<T: ZeroCopyVecValue> RawStrategy<T> for ZeroCopyStrategy<T> {
    #[inline(always)]
    fn read(bytes: &[u8]) -> Result<T> {
        T::read_from_prefix(bytes)
            .map(|(v, _)| v)
            .map_err(|_| Error::ZeroCopyError)
    }

    #[inline(always)]
    unsafe fn read_from_ptr(ptr: *const u8, byte_offset: usize) -> T {
        // ZeroCopy: byte layout == memory layout. Single mov instruction.
        unsafe { (ptr.add(byte_offset) as *const T).read_unaligned() }
    }

    #[inline(always)]
    fn write_to_vec(value: &T, buf: &mut Vec<u8>) {
        buf.extend_from_slice(value.as_bytes());
    }

    #[inline(always)]
    fn write_to_slice(value: &T, dst: &mut [u8]) {
        dst.copy_from_slice(value.as_bytes());
    }

    #[inline]
    fn read_bulk(bytes: &[u8], buf: &mut Vec<T>, count: usize) {
        debug_assert_eq!(bytes.len(), count * size_of::<T>());
        buf.reserve(count);
        // ZeroCopy: byte layout == memory layout. Always memcpy.
        unsafe {
            let base = buf.len();
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                buf.as_mut_ptr().add(base) as *mut u8,
                bytes.len(),
            );
            buf.set_len(base + count);
        }
    }
}
