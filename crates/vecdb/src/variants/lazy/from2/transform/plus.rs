use std::ops::Add;

use super::BinaryTransform;

/// (a, b) -> a + b
pub struct Plus;

impl<T: Add<U, Output = O>, U, O> BinaryTransform<T, U, O> for Plus {
    #[inline(always)]
    fn apply(lhs: T, rhs: U) -> O {
        lhs + rhs
    }
}
