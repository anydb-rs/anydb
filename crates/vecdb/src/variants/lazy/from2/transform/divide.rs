use std::ops::Div;

use super::BinaryTransform;

/// (a, b) -> a / b
pub struct Divide;

impl<T: Div<U, Output = O>, U, O> BinaryTransform<T, U, O> for Divide {
    #[inline(always)]
    fn apply(lhs: T, rhs: U) -> O {
        lhs / rhs
    }
}
