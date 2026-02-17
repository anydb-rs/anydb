use std::marker::PhantomData;

use crate::{BytesVecValue, Result, variants::raw::RawStrategy};

/// Serialization strategy using the Bytes trait with portable byte order.
///
/// Implements little-endian serialization for cross-platform compatibility.
#[derive(Debug, Clone, Copy)]
pub struct BytesStrategy<T>(PhantomData<T>);

impl<T: BytesVecValue> RawStrategy<T> for BytesStrategy<T> {
    #[inline(always)]
    fn read(bytes: &[u8]) -> Result<T> {
        T::from_bytes(bytes)
    }

    #[inline(always)]
    unsafe fn read_from_ptr(ptr: *const u8, byte_offset: usize) -> T {
        unsafe {
            if T::IS_NATIVE_LAYOUT {
                // Byte representation == memory representation. Single mov instruction.
                (ptr.add(byte_offset) as *const T).read_unaligned()
            } else {
                // Non-native layout (e.g. LE data on BE platform): decode through from_bytes.
                let slice = std::slice::from_raw_parts(ptr.add(byte_offset), size_of::<T>());
                Self::read(slice).unwrap_unchecked()
            }
        }
    }

    #[inline(always)]
    fn write_to_vec(value: &T, buf: &mut Vec<u8>) {
        buf.extend_from_slice(value.to_bytes().as_ref());
    }

    #[inline(always)]
    fn write_to_slice(value: &T, dst: &mut [u8]) {
        dst.copy_from_slice(value.to_bytes().as_ref());
    }

    #[inline]
    fn read_bulk(bytes: &[u8], buf: &mut Vec<T>, count: usize) {
        debug_assert_eq!(bytes.len(), count * size_of::<T>());
        buf.reserve(count);
        let base = buf.len();
        if T::IS_NATIVE_LAYOUT {
            // On LE, from_le_bytes is a no-op — bytes ARE the T values. Memcpy.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    bytes.as_ptr(),
                    buf.as_mut_ptr().add(base) as *mut u8,
                    bytes.len(),
                );
                buf.set_len(base + count);
            }
        } else {
            // Non-native layout: per-element decode, no push/expect overhead.
            let dst = buf.as_mut_ptr();
            for (i, chunk) in bytes.chunks_exact(size_of::<T>()).enumerate() {
                unsafe {
                    dst.add(base + i).write(Self::read(chunk).unwrap_unchecked());
                }
            }
            unsafe { buf.set_len(base + count); }
        }
    }
}
