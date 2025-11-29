use crate::{CollectableVec, GenericStoredVec, ImportableVec, TypedVec, VecIndex, VecValue};

/// Super trait combining all common stored vec traits.
pub trait StoredVec:
    ImportableVec
    + TypedVec
    + GenericStoredVec<Self::I, Self::T>
    + CollectableVec<Self::I, Self::T>
    + Clone
where
    Self::I: VecIndex,
    Self::T: VecValue,
{
}

impl<V> StoredVec for V
where
    V: ImportableVec + TypedVec + GenericStoredVec<V::I, V::T> + CollectableVec<V::I, V::T> + Clone,
    V::I: VecIndex,
    V::T: VecValue,
{
}
