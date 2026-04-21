mod as_inner;
mod as_inner_mut;
mod from_inner;

pub use as_inner::AsInnerSlice;
pub use as_inner_mut::AsInnerSliceMut;
pub use from_inner::FromInnerSlice;

use crate::{BytesVecValue, Pco, TransparentPco};

/// Marker trait for values storable in a `PcoVec`: must be `Copy`, `Pco`,
/// and serializable via the `Bytes` path used by `BytesVec`.
pub trait PcoVecValue: Pco + BytesVecValue + Copy {}

impl<T> PcoVecValue for T where T: Pco + BytesVecValue + Copy {}

macro_rules! impl_stored_compressed {
    ($($t:ty),*) => {
        $(
            impl TransparentPco<$t> for $t {}
            impl Pco for $t {
                type NumberType = $t;
            }
        )*
    };
}

impl_stored_compressed!(u8, u16, u32, u64, i8, i16, i32, i64, f32, f64);
