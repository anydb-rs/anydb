use crate::{AnyVec, ReadableVec, VecIndex, VecValue};

use super::{DeltaOp, LazyDeltaVec};

impl<I, S, T, Op> ReadableVec<I, T> for LazyDeltaVec<I, S, T, Op>
where
    I: VecIndex,
    S: VecValue,
    T: VecValue,
    Op: DeltaOp<S, T>,
{
    #[inline]
    fn read_into_at(&self, from: usize, to: usize, buf: &mut Vec<T>) {
        let starts = (self.window_starts)();
        let to = to.min(self.len()).min(starts.len());
        if from >= to {
            return;
        }
        buf.reserve(to - from);
        self.bulk_for_each(from, to, &starts, |v| buf.push(v));
    }

    #[inline]
    fn for_each_range_dyn_at(&self, from: usize, to: usize, f: &mut dyn FnMut(T)) {
        let starts = (self.window_starts)();
        let to = to.min(self.len()).min(starts.len());
        if from >= to {
            return;
        }
        self.bulk_for_each(from, to, &starts, f);
    }

    #[inline]
    fn fold_range_at<B, F: FnMut(B, T) -> B>(&self, from: usize, to: usize, init: B, mut f: F) -> B
    where
        Self: Sized,
    {
        let starts = (self.window_starts)();
        let to = to.min(self.len()).min(starts.len());
        if from >= to {
            return init;
        }
        self.bulk_try_fold(from, to, &starts, init, |acc, v| {
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
        f: F,
    ) -> std::result::Result<B, E>
    where
        Self: Sized,
    {
        let starts = (self.window_starts)();
        let to = to.min(self.len()).min(starts.len());
        if from >= to {
            return Ok(init);
        }
        self.bulk_try_fold(from, to, &starts, init, f)
    }

    #[inline]
    fn collect_one_at(&self, index: usize) -> Option<T> {
        if index >= self.len() {
            return None;
        }
        let starts = (self.window_starts)();
        if index >= starts.len() {
            return None;
        }
        let start = starts[index].to_usize();
        let current = self.source.collect_one_at(index)?;
        let ago = match Op::ago_index(start) {
            Some(idx) => self.source.collect_one_at(idx)?,
            None => Op::ago_default(),
        };
        Some(Op::combine(current, ago, Op::count(index, start)))
    }

    fn read_sorted_into_at(&self, indices: &[usize], out: &mut Vec<T>) {
        if indices.is_empty() {
            return;
        }

        let starts = (self.window_starts)();
        let len = self.len().min(starts.len());
        let count = indices.len();

        let mut reads: Vec<(usize, u32, bool)> = Vec::with_capacity(count * 2);
        indices.iter().enumerate().for_each(|(slot, &h)| {
            if h < len {
                reads.push((h, slot as u32, true));
                if let Some(ago_idx) = Op::ago_index(starts[h].to_usize()) {
                    reads.push((ago_idx, slot as u32, false));
                }
            }
        });
        reads.sort_unstable_by_key(|r| r.0);

        let mut positions: Vec<usize> = Vec::with_capacity(reads.len());
        let mut val_indices: Vec<u32> = Vec::with_capacity(reads.len());
        reads.iter().for_each(|&(pos, _, _)| {
            if positions.last() != Some(&pos) {
                positions.push(pos);
            }
            val_indices.push((positions.len() - 1) as u32);
        });

        let vals = self.source.read_sorted_at(&positions);

        let mut current_vi = vec![0u32; count];
        let mut ago_vi = vec![0u32; count];
        reads
            .iter()
            .enumerate()
            .for_each(|(i, &(_, slot, is_current))| {
                let vi = val_indices[i];
                if is_current {
                    current_vi[slot as usize] = vi;
                } else {
                    ago_vi[slot as usize] = vi;
                }
            });

        out.reserve(count);
        indices.iter().enumerate().for_each(|(slot, &h)| {
            if h >= len {
                return;
            }
            let start = starts[h].to_usize();
            let current = vals[current_vi[slot] as usize].clone();
            let ago = match Op::ago_index(start) {
                Some(_) => vals[ago_vi[slot] as usize].clone(),
                None => Op::ago_default(),
            };
            out.push(Op::combine(current, ago, Op::count(h, start)));
        });
    }
}
