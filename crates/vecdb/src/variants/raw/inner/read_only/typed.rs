use crate::{TypedVec, VecIndex, VecValue};

use super::{super::RawStrategy, ReadOnlyRawVec};

impl<I, T, S> TypedVec for ReadOnlyRawVec<I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: RawStrategy<T>,
{
    type I = I;
    type T = T;
}
