use std::mem::{align_of, size_of};

use pco::data_types::Number;

use super::PcoVecValue;

/// Convert a slice of PcoVecValue to a slice of the underlying Number type.
///
/// # Safety
/// This trait uses unsafe pointer casting that relies on compile-time size/alignment checks.
/// The const assertions ensure T and T::NumberType have identical layout.
pub trait AsInnerSlice<T>
where
    T: Number,
{
    const _SIZE_CHECK: ();
    const _ALIGN_CHECK: ();

    fn as_inner_slice(&self) -> &[T];
}

impl<T> AsInnerSlice<T::NumberType> for [T]
where
    T: PcoVecValue,
{
    const _SIZE_CHECK: () = assert!(size_of::<T>() == size_of::<T::NumberType>());
    const _ALIGN_CHECK: () = assert!(align_of::<T>() == align_of::<T::NumberType>());

    fn as_inner_slice(&self) -> &[T::NumberType] {
        // SAFETY: Compile-time assertions ensure T and T::NumberType have identical layout
        unsafe { std::slice::from_raw_parts(self.as_ptr() as *const T::NumberType, self.len()) }
    }
}
