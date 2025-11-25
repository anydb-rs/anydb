use crate::{Error, Result};

/// Trait for types that can be serialized to/from bytes.
/// This is an alternative to zerocopy for types that need custom serialization.
pub trait Bytes: Sized {
    /// Serialize this value to bytes.
    /// The returned slice must be exactly `SIZE` bytes.
    fn to_bytes(&self) -> Vec<u8>;

    /// Deserialize a value from bytes.
    /// The input slice must be exactly `SIZE` bytes.
    fn from_bytes(bytes: &[u8]) -> Result<Self>;
}

// Implement Bytes for common numeric types
macro_rules! impl_bytes_for_numeric {
    ($($t:ty),*) => {
        $(
            impl Bytes for $t {
                #[inline]
                fn to_bytes(&self) -> Vec<u8> {
                    self.to_le_bytes().to_vec()
                }

                #[inline]
                fn from_bytes(bytes: &[u8]) -> Result<Self> {
                    let arr: [u8; std::mem::size_of::<$t>()] = bytes
                        .try_into()
                        .map_err(|_| Error::WrongLength)?;
                    Ok(<$t>::from_ne_bytes(arr))
                }
            }
        )*
    };
}

impl_bytes_for_numeric!(
    u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize, f32, f64
);

// Implement Bytes for fixed-size byte arrays
macro_rules! impl_bytes_for_array {
    ($($n:expr),*) => {
        $(
            impl Bytes for [u8; $n] {
                #[inline]
                fn to_bytes(&self) -> Vec<u8> {
                    self.to_vec()
                }

                #[inline]
                fn from_bytes(bytes: &[u8]) -> Result<Self> {
                    bytes.try_into().map_err(|_| Error::WrongLength)
                }
            }
        )*
    };
}

impl_bytes_for_array!(
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
    27, 28, 29, 30, 31, 32, 33, 64, 65
);

// Extension trait to add to_bytes() method for slices and Vec
pub trait BytesExt {
    fn to_bytes(&self) -> Vec<u8>;
}

impl<T: Bytes> BytesExt for [T] {
    fn to_bytes(&self) -> Vec<u8> {
        self.iter().flat_map(|item| item.to_bytes()).collect()
    }
}
