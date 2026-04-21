use std::ops::Mul;

use super::BinaryTransform;

/// (a, b) -> a * b
pub struct Times;

impl<T: Mul<U, Output = O>, U, O> BinaryTransform<T, U, O> for Times {
    #[inline(always)]
    fn apply(lhs: T, rhs: U) -> O {
        lhs * rhs
    }
}
