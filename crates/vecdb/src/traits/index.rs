use std::{fmt::Debug, ops::Add};

use crate::PrintableIndex;

pub trait VecIndex
where
    Self: Debug
        + Default
        + Copy
        + Clone
        + PartialEq
        + Eq
        + PartialOrd
        + Ord
        + From<usize>
        + Into<usize>
        + Add<usize, Output = Self>
        + Send
        + Sync
        + PrintableIndex
        + 'static,
{
    #[inline]
    fn to_usize(self) -> usize {
        self.into()
    }

    #[inline]
    fn decremented(self) -> Option<Self> {
        self.to_usize().checked_sub(1).map(Self::from)
    }
}

impl<I> VecIndex for I where
    I: Debug
        + Default
        + Copy
        + Clone
        + PartialEq
        + Eq
        + PartialOrd
        + Ord
        + From<usize>
        + Into<usize>
        + Add<usize, Output = Self>
        + Send
        + Sync
        + PrintableIndex
        + 'static
{
}
