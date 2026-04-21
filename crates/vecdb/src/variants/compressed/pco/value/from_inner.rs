use std::mem::{ManuallyDrop, align_of, size_of};

use super::PcoVecValue;

/// Convert a Vec of Number type to a Vec of PcoVecValue.
///
/// # Safety
/// This trait uses unsafe pointer casting that relies on compile-time size/alignment checks.
/// The const assertions ensure T and T::NumberType have identical layout.
pub trait FromInnerSlice<T> {
    const _SIZE_CHECK: ();
    const _ALIGN_CHECK: ();

    fn from_inner_slice(slice: Vec<T>) -> Vec<Self>
    where
        Self: Sized;
}

impl<T> FromInnerSlice<T::NumberType> for T
where
    T: PcoVecValue,
{
    const _SIZE_CHECK: () = assert!(size_of::<T>() == size_of::<T::NumberType>());
    const _ALIGN_CHECK: () = assert!(align_of::<T>() == align_of::<T::NumberType>());

    fn from_inner_slice(vec: Vec<T::NumberType>) -> Vec<T> {
        let mut vec = ManuallyDrop::new(vec);
        unsafe { Vec::from_raw_parts(vec.as_mut_ptr() as *mut T, vec.len(), vec.capacity()) }
    }
}
