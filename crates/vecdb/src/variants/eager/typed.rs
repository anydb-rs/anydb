use crate::{StoredVec, TypedVec};

use super::EagerVec;

impl<V> TypedVec for EagerVec<V>
where
    V: StoredVec,
{
    type I = V::I;
    type T = V::T;
}
