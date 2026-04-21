use crate::{TypedVec, VecIndex, VecValue};

use super::LazyVecFrom1;

impl<I, T, S1I, S1T> TypedVec for LazyVecFrom1<I, T, S1I, S1T>
where
    I: VecIndex,
    T: VecValue,
    S1I: VecIndex,
    S1T: VecValue,
{
    type I = I;
    type T = T;
}
