use crate::{AnyVec, ReadableVec, VecIndex, VecValue};

use super::LazyVecFrom1;

impl<I, T, S1I, S1T> ReadableVec<I, T> for LazyVecFrom1<I, T, S1I, S1T>
where
    I: VecIndex,
    T: VecValue,
    S1I: VecIndex,
    S1T: VecValue,
{
    #[inline]
    fn read_into_at(&self, from: usize, to: usize, buf: &mut Vec<T>) {
        let to = to.min(self.len());
        buf.reserve(to.saturating_sub(from));
        self.for_each_range_dyn_at(from, to, &mut |v| buf.push(v));
    }

    #[inline]
    fn for_each_range_dyn_at(&self, from: usize, to: usize, f: &mut dyn FnMut(T)) {
        let compute = self.compute;
        let to = to.min(self.len());
        let mut pos = from;
        self.source.for_each_range_dyn_at(from, to, &mut |v| {
            f(compute(I::from(pos), v));
            pos += 1;
        });
    }

    #[inline]
    fn fold_range_at<B, F: FnMut(B, T) -> B>(&self, from: usize, to: usize, init: B, mut f: F) -> B
    where
        Self: Sized,
    {
        self.try_fold_range_at(from, to, init, |acc, v| {
            Ok::<_, std::convert::Infallible>(f(acc, v))
        })
        .unwrap_or_else(|e: std::convert::Infallible| match e {})
    }

    #[inline]
    fn try_fold_range_at<B, E, F: FnMut(B, T) -> std::result::Result<B, E>>(
        &self,
        from: usize,
        to: usize,
        init: B,
        mut f: F,
    ) -> std::result::Result<B, E>
    where
        Self: Sized,
    {
        let to = to.min(self.len());
        if from >= to {
            return Ok(init);
        }
        let compute = self.compute;
        let buf = self.source.collect_range_dyn(from, to);
        buf.into_iter()
            .enumerate()
            .try_fold(init, |acc, (local, v)| {
                f(acc, compute(I::from(from + local), v))
            })
    }

    #[inline]
    fn collect_one_at(&self, index: usize) -> Option<T> {
        let v = self.source.collect_one_at(index)?;
        Some((self.compute)(I::from(index), v))
    }

    fn read_sorted_into_at(&self, indices: &[usize], out: &mut Vec<T>) {
        let compute = self.compute;
        let source_vals = self.source.read_sorted_at(indices);
        out.reserve(source_vals.len());
        indices
            .iter()
            .zip(source_vals)
            .for_each(|(&i, v)| out.push(compute(I::from(i), v)));
    }
}
