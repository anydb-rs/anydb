use std::ops::Sub;

use super::BinaryTransform;

/// (a, b) -> a - b
pub struct Minus;

impl<T: Sub<U, Output = O>, U, O> BinaryTransform<T, U, O> for Minus {
    #[inline(always)]
    fn apply(lhs: T, rhs: U) -> O {
        lhs - rhs
    }
}
