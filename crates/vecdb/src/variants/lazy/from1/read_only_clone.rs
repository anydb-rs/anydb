use crate::{ReadOnlyClone, VecIndex, VecValue};

use super::LazyVecFrom1;

impl<I, T, S1I, S1T> ReadOnlyClone for LazyVecFrom1<I, T, S1I, S1T>
where
    I: VecIndex,
    T: VecValue,
    S1I: VecIndex,
    S1T: VecValue,
{
    type ReadOnly = Self;

    fn read_only_clone(&self) -> Self {
        self.clone()
    }
}
