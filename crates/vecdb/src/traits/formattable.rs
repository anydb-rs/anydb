use std::fmt::{self, Write};

pub trait Formattable {
    /// Write the value in CSV format (with escaping if needed)
    fn fmt_csv(&self, f: &mut String) -> fmt::Result;
}

// Implement for numeric types (no escaping needed)
macro_rules! impl_formattable_numeric {
    ($($t:ty),*) => {
        $(
            impl Formattable for $t {
                #[inline]
                fn fmt_csv(&self, f: &mut String) -> fmt::Result {
                    write!(f, "{}", self)
                }
            }
        )*
    };
}

impl_formattable_numeric!(
    bool, u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize, f32, f64
);

impl<T: Formattable> Formattable for Option<T> {
    #[inline]
    fn fmt_csv(&self, f: &mut String) -> fmt::Result {
        match self {
            Some(v) => v.fmt_csv(f),
            None => Ok(()),
        }
    }
}
