use crate::TypedVec;

use super::CachedVec;

impl<V: TypedVec> TypedVec for CachedVec<V> {
    type I = V::I;
    type T = V::T;
}
