use crate::{
    AnyVec, Exit, WritableVec, ReadableVec, Result, StoredVec, VecIndex, VecValue, Version,
};

use super::EagerVec;

impl<V> EagerVec<V>
where
    V: StoredVec,
{
    pub fn compute_to<F>(
        &mut self,
        max_from: V::I,
        to: usize,
        version: Version,
        mut t: F,
        exit: &Exit,
    ) -> Result<()>
    where
        F: FnMut(V::I) -> (V::I, V::T),
    {
        self.compute_init(version, max_from, exit, |this| {
            let from = this.len();
            let end = this.batch_end(to);
            if from >= end {
                return Ok(());
            }

            for i in from..end {
                let (idx, val) = t(V::I::from(i));
                this.checked_push(idx, val)?;
            }

            Ok(())
        })
    }

    pub fn compute_range<A, F>(
        &mut self,
        max_from: V::I,
        other: &impl ReadableVec<V::I, A>,
        t: F,
        exit: &Exit,
    ) -> Result<()>
    where
        A: VecValue,
        F: FnMut(V::I) -> (V::I, V::T),
    {
        self.compute_to(max_from, other.len(), other.version(), t, exit)
    }

    pub fn compute_from_index<A>(
        &mut self,
        max_from: V::I,
        other: &impl ReadableVec<V::I, A>,
        exit: &Exit,
    ) -> Result<()>
    where
        V::T: From<V::I>,
        A: VecValue,
    {
        self.compute_to(
            max_from,
            other.len(),
            other.version(),
            |i| (i, V::T::from(i)),
            exit,
        )
    }

    pub fn compute_transform<A, F>(
        &mut self,
        max_from: V::I,
        other: &impl ReadableVec<V::I, A>,
        mut t: F,
        exit: &Exit,
    ) -> Result<()>
    where
        A: VecValue,
        F: FnMut((V::I, A, &Self)) -> (V::I, V::T),
    {
        self.compute_init(other.version(), max_from, exit, |this| {
            let skip = this.len();
            let end = this.batch_end(other.len());
            if skip >= end {
                return Ok(());
            }

            let mut i = skip;
            other.try_fold_range(skip, end, (), |(), b: A| {
                let (idx, v) = t((V::I::from(i), b, &*this));
                i += 1;
                this.checked_push(idx, v)
            })
        })
    }

    pub fn compute_transform2<A, B, F>(
        &mut self,
        max_from: V::I,
        other1: &impl ReadableVec<V::I, A>,
        other2: &impl ReadableVec<V::I, B>,
        mut t: F,
        exit: &Exit,
    ) -> Result<()>
    where
        A: VecValue,
        B: VecValue,
        F: FnMut((V::I, A, B, &Self)) -> (V::I, V::T),
    {
        self.compute_init(other1.version() + other2.version(), max_from, exit, |this| {
            let skip = this.len();
            let source_end = other1.len().min(other2.len());
            let end = this.batch_end(source_end);
            if skip >= end {
                return Ok(());
            }

            let batch2 = other2.collect_range(skip, end);
            let mut iter2 = batch2.into_iter();
            let mut i = skip;

            other1.try_fold_range(skip, end, (), |(), b: A| {
                let (idx, v) = t((V::I::from(i), b, iter2.next().unwrap(), &*this));
                i += 1;
                this.checked_push(idx, v)
            })
        })
    }

    pub fn compute_transform3<A, B, C, F>(
        &mut self,
        max_from: V::I,
        other1: &impl ReadableVec<V::I, A>,
        other2: &impl ReadableVec<V::I, B>,
        other3: &impl ReadableVec<V::I, C>,
        mut t: F,
        exit: &Exit,
    ) -> Result<()>
    where
        A: VecValue,
        B: VecValue,
        C: VecValue,
        F: FnMut((V::I, A, B, C, &Self)) -> (V::I, V::T),
    {
        self.compute_init(
            other1.version() + other2.version() + other3.version(),
            max_from,
            exit,
            |this| {
            let skip = this.len();
            let source_end = other1.len().min(other2.len()).min(other3.len());
            let end = this.batch_end(source_end);
            if skip >= end {
                return Ok(());
            }

            let batch2 = other2.collect_range(skip, end);
            let batch3 = other3.collect_range(skip, end);
            let mut iter2 = batch2.into_iter();
            let mut iter3 = batch3.into_iter();
            let mut i = skip;

            other1.try_fold_range(skip, end, (), |(), b: A| {
                let (idx, v) = t((
                    V::I::from(i),
                    b,
                    iter2.next().unwrap(),
                    iter3.next().unwrap(),
                    &*this,
                ));
                i += 1;
                this.checked_push(idx, v)
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn compute_transform4<A, B, C, D, F>(
        &mut self,
        max_from: V::I,
        other1: &impl ReadableVec<V::I, A>,
        other2: &impl ReadableVec<V::I, B>,
        other3: &impl ReadableVec<V::I, C>,
        other4: &impl ReadableVec<V::I, D>,
        mut t: F,
        exit: &Exit,
    ) -> Result<()>
    where
        A: VecValue,
        B: VecValue,
        C: VecValue,
        D: VecValue,
        F: FnMut((V::I, A, B, C, D, &Self)) -> (V::I, V::T),
    {
        self.compute_init(
            other1.version() + other2.version() + other3.version() + other4.version(),
            max_from,
            exit,
            |this| {
            let skip = this.len();
            let source_end = other1
                .len()
                .min(other2.len())
                .min(other3.len())
                .min(other4.len());
            let end = this.batch_end(source_end);
            if skip >= end {
                return Ok(());
            }

            let batch2 = other2.collect_range(skip, end);
            let batch3 = other3.collect_range(skip, end);
            let batch4 = other4.collect_range(skip, end);
            let mut iter2 = batch2.into_iter();
            let mut iter3 = batch3.into_iter();
            let mut iter4 = batch4.into_iter();
            let mut i = skip;

            other1.try_fold_range(skip, end, (), |(), b: A| {
                let (idx, v) = t((
                    V::I::from(i),
                    b,
                    iter2.next().unwrap(),
                    iter3.next().unwrap(),
                    iter4.next().unwrap(),
                    &*this,
                ));
                i += 1;
                this.checked_push(idx, v)
            })
        })
    }

    pub fn compute_coarser(
        &mut self,
        max_from: V::T,
        other: &impl ReadableVec<V::T, V::I>,
        exit: &Exit,
    ) -> Result<()>
    where
        V::I: VecValue + VecIndex,
        V::T: VecIndex,
    {
        self.validate_computed_version_or_reset(other.version())?;

        self.repeat_until_complete(exit, |this| {
            let skip = if this.len() > 0 {
                this.collect_last().unwrap()
                    .to_usize()
                    .min(max_from.to_usize())
            } else {
                0
            };

            let end = this.batch_end(other.len());
            if skip >= end {
                return Ok(());
            }

            let mut prev_i = None;
            let batch = other.collect_range(skip, end);
            for (j, i) in batch.into_iter().enumerate() {
                let v = V::T::from(skip + j);
                if prev_i.is_some_and(|prev_i| prev_i == i) {
                    continue;
                }
                if this.collect_one(i.to_usize()).is_none_or(|old_v| old_v > v) {
                    this.truncate_push(i, v)?;
                }
                prev_i.replace(i);
            }

            Ok(())
        })
    }
}
