use crate::{ZeroCopyVecValue, variants::raw::RawStrategy};

use super::ZeroCopyStrategy;

impl<T: ZeroCopyVecValue> RawStrategy<T> for ZeroCopyStrategy<T> {
    #[inline(always)]
    unsafe fn read_from_ptr(ptr: *const u8, byte_offset: usize) -> T {
        unsafe { (ptr.add(byte_offset) as *const T).read_unaligned() }
    }
}
