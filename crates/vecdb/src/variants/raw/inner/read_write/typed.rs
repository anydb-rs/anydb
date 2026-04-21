use crate::{TypedVec, VecIndex, VecValue};

use super::{super::RawStrategy, ReadWriteRawVec};

impl<I, T, S> TypedVec for ReadWriteRawVec<I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: RawStrategy<T>,
{
    type I = I;
    type T = T;
}
