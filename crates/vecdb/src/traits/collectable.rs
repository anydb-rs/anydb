use crate::ScannableVec;

use super::{VecIndex, VecValue};

/// Extension for [`ScannableVec`] adding signed-index range collection (Python-style).
///
/// Negative indices count from the end: `-1` is the last element, `-2` is second-to-last, etc.
/// Automatically implemented for all `ScannableVec + Clone` types.
pub trait CollectableVec<I, T>: ScannableVec<I, T>
where
    Self: Clone,
    I: VecIndex,
    T: VecValue,
{
    /// Collects values using signed indices. Negative indices count from the end.
    fn collect_signed_range(&self, from: Option<i64>, to: Option<i64>) -> Vec<T> {
        let from = from.map(|i| self.i64_to_usize(i)).unwrap_or(0);
        let to = to.map(|i| self.i64_to_usize(i)).unwrap_or_else(|| self.len());
        self.collect_range(from, to)
    }
}

impl<I, T, V> CollectableVec<I, T> for V
where
    V: ScannableVec<I, T> + Clone,
    I: VecIndex,
    T: VecValue,
{
}
