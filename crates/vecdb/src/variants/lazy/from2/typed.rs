use crate::{TypedVec, VecIndex, VecValue};

use super::LazyVecFrom2;

impl<I, T, S1I, S1T, S2I, S2T> TypedVec for LazyVecFrom2<I, T, S1I, S1T, S2I, S2T>
where
    I: VecIndex,
    T: VecValue,
    S1I: VecIndex,
    S1T: VecValue,
    S2I: VecIndex,
    S2T: VecValue,
{
    type I = I;
    type T = T;
}
