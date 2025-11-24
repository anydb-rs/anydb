use crate::{AnyVec, VecIndex, VecValue};

pub trait TypedVec: AnyVec {
    type I: VecIndex;
    type T: VecValue;
}
