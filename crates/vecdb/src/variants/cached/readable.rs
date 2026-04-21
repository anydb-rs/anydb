use crate::{ReadableVec, TypedVec};

use super::CachedVec;

impl<V: TypedVec + ReadableVec<V::I, V::T>> ReadableVec<V::I, V::T> for CachedVec<V> {
    #[inline]
    fn read_into_at(&self, from: usize, to: usize, buf: &mut Vec<V::T>) {
        if let Some(data) = self.try_cached() {
            let to = to.min(data.len());
            if from < to {
                buf.extend_from_slice(&data[from..to]);
            }
        } else {
            self.inner.read_into_at(from, to, buf);
        }
    }

    #[inline]
    fn for_each_range_dyn_at(&self, from: usize, to: usize, f: &mut dyn FnMut(V::T)) {
        if let Some(data) = self.try_cached() {
            let to = to.min(data.len());
            let from = from.min(to);
            for v in &data[from..to] {
                f(v.clone());
            }
        } else {
            self.inner.for_each_range_dyn_at(from, to, f);
        }
    }

    #[inline]
    fn fold_range_at<B, F: FnMut(B, V::T) -> B>(
        &self,
        from: usize,
        to: usize,
        init: B,
        mut f: F,
    ) -> B
    where
        Self: Sized,
    {
        if let Some(data) = self.try_cached() {
            let to = to.min(data.len());
            let from = from.min(to);
            let mut acc = init;
            for v in &data[from..to] {
                acc = f(acc, v.clone());
            }
            acc
        } else {
            self.inner.fold_range_at(from, to, init, f)
        }
    }

    #[inline]
    fn try_fold_range_at<B, E, F: FnMut(B, V::T) -> Result<B, E>>(
        &self,
        from: usize,
        to: usize,
        init: B,
        mut f: F,
    ) -> Result<B, E>
    where
        Self: Sized,
    {
        if let Some(data) = self.try_cached() {
            let to = to.min(data.len());
            let from = from.min(to);
            let mut acc = init;
            for v in &data[from..to] {
                acc = f(acc, v.clone())?;
            }
            Ok(acc)
        } else {
            self.inner.try_fold_range_at(from, to, init, f)
        }
    }

    #[inline]
    fn collect_one_at(&self, index: usize) -> Option<V::T> {
        if let Some(data) = self.try_cached() {
            data.get(index).cloned()
        } else {
            self.inner.collect_one_at(index)
        }
    }

    #[inline]
    fn read_sorted_into_at(&self, indices: &[usize], out: &mut Vec<V::T>) {
        if let Some(data) = self.try_cached() {
            out.reserve(indices.len());
            for &i in indices {
                if let Some(v) = data.get(i) {
                    out.push(v.clone());
                }
            }
        } else {
            self.inner.read_sorted_into_at(indices, out);
        }
    }
}
