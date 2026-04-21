use crate::{TypedVec, VecIndex, VecValue};

use super::{DeltaOp, LazyDeltaVec};

impl<I, S, T, Op> TypedVec for LazyDeltaVec<I, S, T, Op>
where
    I: VecIndex,
    S: VecValue,
    T: VecValue,
    Op: DeltaOp<S, T>,
{
    type I = I;
    type T = T;
}
