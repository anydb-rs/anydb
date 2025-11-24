use std::mem::{align_of, size_of};

use pco::data_types::Number;

use crate::VecValue;

pub trait TransparentPcodecVecValue<T> {}

pub trait PcodecVecValue
where
    Self: VecValue + Copy + 'static + TransparentPcodecVecValue<Self::NumberType>,
{
    type NumberType: pco::data_types::Number;
}

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
    T: PcodecVecValue,
{
    const _SIZE_CHECK: () = assert!(size_of::<T>() == size_of::<T::NumberType>());
    const _ALIGN_CHECK: () = assert!(align_of::<T>() == align_of::<T::NumberType>());

    fn as_inner_slice(&self) -> &[T::NumberType] {
        unsafe { std::slice::from_raw_parts(self.as_ptr() as *const T::NumberType, self.len()) }
    }
}

pub trait FromInnerSlice<T> {
    const _SIZE_CHECK: ();
    const _ALIGN_CHECK: ();

    fn from_inner_slice(slice: Vec<T>) -> Vec<Self>
    where
        Self: Sized;
}

impl<T> FromInnerSlice<T::NumberType> for T
where
    T: PcodecVecValue,
{
    const _SIZE_CHECK: () = assert!(size_of::<T>() == size_of::<T::NumberType>());
    const _ALIGN_CHECK: () = assert!(align_of::<T>() == align_of::<T::NumberType>());

    fn from_inner_slice(vec: Vec<T::NumberType>) -> Vec<T> {
        let mut vec = std::mem::ManuallyDrop::new(vec);
        unsafe { Vec::from_raw_parts(vec.as_mut_ptr() as *mut T, vec.len(), vec.capacity()) }
    }
}

macro_rules! impl_stored_compressed {
    ($($t:ty),*) => {
        $(
            impl
TransparentPcodecVecValue<$t> for $t {}
impl PcodecVecValue for $t {
                type NumberType = $t;
            }
        )*
    };
}

impl_stored_compressed!(u16, u32, u64, i16, i32, i64, f32, f64);
