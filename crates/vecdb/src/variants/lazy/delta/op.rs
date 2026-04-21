/// Trait defining how to combine a current value with an earlier value.
///
/// `S` is the source type read from the vec, `T` is the output type produced.
/// When `S = T` (e.g., rolling sums), the operation is same-type.
/// When `S != T` (e.g., delta change/rate), the operation converts between types.
pub trait DeltaOp<S, T>: Send + Sync + 'static {
    /// Source index for the `ago` value given a window start.
    /// Returns `None` when there is no preceding element (cumulative ops at start = 0).
    #[inline]
    fn ago_index(start: usize) -> Option<usize> {
        Some(start)
    }

    /// Fallback `ago` value when `ago_index` returns `None`.
    #[inline]
    fn ago_default() -> S
    where
        S: Sized,
    {
        unreachable!()
    }

    /// Window element count from current index `h` and window start.
    #[inline]
    fn count(h: usize, start: usize) -> usize {
        h - start
    }

    fn combine(current: S, ago: S, count: usize) -> T;
}
