use crate::CheckedSub;

use super::DeltaOp;

/// Rolling sum from cumulative: `cum[h] - cum[start - 1]`
#[derive(Clone, Copy)]
pub struct DeltaSub;

impl<T> DeltaOp<T, T> for DeltaSub
where
    T: CheckedSub + Default,
{
    #[inline]
    fn ago_index(start: usize) -> Option<usize> {
        start.checked_sub(1)
    }

    #[inline]
    fn ago_default() -> T {
        T::default()
    }

    #[inline]
    fn count(h: usize, start: usize) -> usize {
        h - start + 1
    }

    #[inline]
    fn combine(current: T, ago: T, _count: usize) -> T {
        current.checked_sub(ago).unwrap_or_default()
    }
}
