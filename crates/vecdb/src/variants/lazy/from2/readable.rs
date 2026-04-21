use crate::{AnyVec, ReadableVec, VecIndex, VecValue};

use super::LazyVecFrom2;

impl<I, T, S1I, S1T, S2I, S2T> ReadableVec<I, T> for LazyVecFrom2<I, T, S1I, S1T, S2I, S2T>
where
    I: VecIndex,
    T: VecValue,
    S1I: VecIndex,
    S1T: VecValue,
    S2I: VecIndex,
    S2T: VecValue,
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
        let buf1 = self.source1.collect_range_dyn(from, to);
        let buf2 = self.source2.collect_range_dyn(from, to);
        buf1.into_iter()
            .zip(buf2)
            .enumerate()
            .for_each(|(local, (v1, v2))| {
                f(compute(I::from(from + local), v1, v2));
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
        let buf1 = self.source1.collect_range_dyn(from, to);
        let buf2 = self.source2.collect_range_dyn(from, to);
        buf1.into_iter()
            .zip(buf2)
            .enumerate()
            .try_fold(init, |acc, (local, (v1, v2))| {
                f(acc, compute(I::from(from + local), v1, v2))
            })
    }

    #[inline]
    fn collect_one_at(&self, index: usize) -> Option<T> {
        if index >= self.len() {
            return None;
        }
        let v1 = self.source1.collect_one_at(index)?;
        let v2 = self.source2.collect_one_at(index)?;
        Some((self.compute)(I::from(index), v1, v2))
    }

    fn read_sorted_into_at(&self, indices: &[usize], out: &mut Vec<T>) {
        let compute = self.compute;
        let vals1 = self.source1.read_sorted_at(indices);
        let vals2 = self.source2.read_sorted_at(indices);
        out.reserve(vals1.len().min(vals2.len()));
        indices
            .iter()
            .zip(vals1.into_iter().zip(vals2))
            .for_each(|(&i, (v1, v2))| out.push(compute(I::from(i), v1, v2)));
    }
}
