use crate::{TypedVec, VecIndex, VecValue};

use super::LazyVecFrom3;

impl<I, T, S1I, S1T, S2I, S2T, S3I, S3T> TypedVec
    for LazyVecFrom3<I, T, S1I, S1T, S2I, S2T, S3I, S3T>
where
    I: VecIndex,
    T: VecValue,
    S1I: VecIndex,
    S1T: VecValue,
    S2I: VecIndex,
    S2T: VecValue,
    S3I: VecIndex,
    S3T: VecValue,
{
    type I = I;
    type T = T;
}
