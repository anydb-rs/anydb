use crate::{TypedVec, VecIndex, VecValue};

use super::{super::CompressionStrategy, ReadOnlyCompressedVec};

impl<I, T, S> TypedVec for ReadOnlyCompressedVec<I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: CompressionStrategy<T>,
{
    type I = I;
    type T = T;
}
