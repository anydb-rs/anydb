use crate::{ReadableVec, WritableVec, ImportableVec, TypedVec, VecIndex, VecValue};

/// Super trait combining all common stored vec traits.
pub trait StoredVec:
    ImportableVec
    + TypedVec
    + WritableVec<Self::I, Self::T>
    + ReadableVec<Self::I, Self::T>
    + Clone
where
    Self::I: VecIndex,
    Self::T: VecValue,
{
}

impl<V> StoredVec for V
where
    V: ImportableVec + TypedVec + WritableVec<V::I, V::T> + ReadableVec<V::I, V::T> + Clone,
    V::I: VecIndex,
    V::T: VecValue,
{
}
