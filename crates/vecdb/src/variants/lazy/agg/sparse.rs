use crate::{AggFold, Cursor, ReadableVec, VecIndex, VecValue};

/// Sparse aggregation: emits `Option<T>` per output index.
///
/// `Some(last_value)` when the range contains source elements,
/// `None` when the range is empty.
pub struct Sparse;

impl<T: VecValue, SI: VecIndex> AggFold<Option<T>, SI, SI, T> for Sparse {
    #[inline]
    fn try_fold<S: ReadableVec<SI, T> + ?Sized, B, E, F: FnMut(B, Option<T>) -> Result<B, E>>(
        source: &S,
        mapping: &[SI],
        from: usize,
        to: usize,
        init: B,
        mut f: F,
    ) -> Result<B, E> {
        let source_len = source.len();
        let mut cursor = Cursor::new(source);
        let mut acc = init;
        for idx in from..to {
            let current_first = mapping[idx].to_usize();
            let next_first = mapping
                .get(idx + 1)
                .map(|h| h.to_usize())
                .unwrap_or(source_len);

            if next_first == 0 || current_first >= next_first {
                acc = f(acc, None)?;
                continue;
            }

            acc = f(acc, cursor.get(next_first - 1))?;
        }
        Ok(acc)
    }

    #[inline]
    fn collect_one<S: ReadableVec<SI, T> + ?Sized>(
        source: &S,
        mapping: &[SI],
        index: usize,
    ) -> Option<Option<T>> {
        let source_len = source.len();
        let current_first = mapping[index].to_usize();
        let next_first = mapping
            .get(index + 1)
            .map(|h| h.to_usize())
            .unwrap_or(source_len);

        if next_first == 0 || current_first >= next_first {
            return Some(None);
        }
        Some(source.collect_one_at(next_first - 1))
    }
}

