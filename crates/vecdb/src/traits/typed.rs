use crate::{AnyStoredVec, AnyVec, VecIndex, VecValue};

pub trait TypedVec: AnyVec {
    type I: VecIndex;
    type T: VecValue;
}

/// Convenience trait combining TypedVec and AnyStoredVec.
/// Automatically implemented for any type implementing both.
pub trait TypedStoredVec: TypedVec + AnyStoredVec {}

impl<V> TypedStoredVec for V where V: TypedVec + AnyStoredVec {}
