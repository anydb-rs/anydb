use crate::{TypedVec, VecIndex, VecValue};

use super::{super::CompressionStrategy, ReadWriteCompressedVec};

impl<I, T, S> TypedVec for ReadWriteCompressedVec<I, T, S>
where
    I: VecIndex,
    T: VecValue,
    S: CompressionStrategy<T>,
{
    type I = I;
    type T = T;
}
