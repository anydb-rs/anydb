use std::ops::Neg;

use super::UnaryTransform;

/// v -> -v
pub struct Negate;

impl<T: Neg<Output = T>> UnaryTransform<T> for Negate {
    #[inline(always)]
    fn apply(value: T) -> T {
        -value
    }
}
