use std::mem::{align_of, size_of};

use pco::data_types::Number;

use super::PcoVecValue;

/// Convert a mutable slice of PcoVecValue to a mutable slice of the underlying Number type.
///
/// # Safety
/// This trait uses unsafe pointer casting that relies on compile-time size/alignment checks.
/// The const assertions ensure T and T::NumberType have identical layout.
pub trait AsInnerSliceMut<T>
where
    T: Number,
{
    const _SIZE_CHECK: ();
    const _ALIGN_CHECK: ();

    fn as_inner_slice_mut(&mut self) -> &mut [T];
}

impl<T> AsInnerSliceMut<T::NumberType> for [T]
where
    T: PcoVecValue,
{
    const _SIZE_CHECK: () = assert!(size_of::<T>() == size_of::<T::NumberType>());
    const _ALIGN_CHECK: () = assert!(align_of::<T>() == align_of::<T::NumberType>());

    fn as_inner_slice_mut(&mut self) -> &mut [T::NumberType] {
        // SAFETY: Compile-time assertions ensure T and T::NumberType have identical layout
        unsafe {
            std::slice::from_raw_parts_mut(self.as_mut_ptr() as *mut T::NumberType, self.len())
        }
    }
}
