//! Transform computation methods for EagerVec.

use log::info;

use crate::{
    AnyVec, Exit, GenericStoredVec, IterableVec, Result, StoredVec, VecIndex, VecValue, Version,
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
        self.validate_computed_version_or_reset(Version::ZERO + self.inner_version() + version)?;

        self.repeat_until_complete(exit, |this| {
            let from = this.len().min(max_from.to_usize());
            if from >= to {
                return Ok(());
            }

            for i in from..to {
                let (idx, val) = t(V::I::from(i));
                this.truncate_push(idx, val)?;

                if this.batch_limit_reached() {
                    break;
                }
            }

            Ok(())
        })
    }

    pub fn compute_range<A, F>(
        &mut self,
        max_from: V::I,
        other: &impl IterableVec<V::I, A>,
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
        other: &impl IterableVec<V::I, A>,
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
        other: &impl IterableVec<V::I, A>,
        mut t: F,
        exit: &Exit,
    ) -> Result<()>
    where
        A: VecValue,
        F: FnMut((V::I, A, &Self)) -> (V::I, V::T),
    {
        self.validate_computed_version_or_reset(
            Version::ZERO + self.inner_version() + other.version(),
        )?;

        self.repeat_until_complete(exit, |this| {
            let skip = this.len().min(max_from.to_usize());

            for (i, b) in other.iter().enumerate().skip(skip) {
                let (i, v) = t((V::I::from(i), b, this));
                this.truncate_push(i, v)?;

                if this.batch_limit_reached() {
                    break;
                }
            }

            Ok(())
        })?;

        Ok(())
    }

    pub fn compute_transform2<A, B, F>(
        &mut self,
        max_from: V::I,
        other1: &impl IterableVec<V::I, A>,
        other2: &impl IterableVec<V::I, B>,
        mut t: F,
        exit: &Exit,
    ) -> Result<()>
    where
        A: VecValue,
        B: VecValue,
        F: FnMut((V::I, A, B, &Self)) -> (V::I, V::T),
    {
        self.validate_computed_version_or_reset(
            Version::ZERO + self.inner_version() + other1.version() + other2.version(),
        )?;

        self.repeat_until_complete(exit, |this| {
            let skip = this.len().min(max_from.to_usize());
            let mut iter2 = other2.iter().skip(skip);

            for (i, b) in other1.iter().enumerate().skip(skip) {
                let (i, v) = t((V::I::from(i), b, iter2.next().unwrap(), this));
                this.truncate_push(i, v)?;

                if this.batch_limit_reached() {
                    break;
                }
            }

            Ok(())
        })
    }

    pub fn compute_transform3<A, B, C, F>(
        &mut self,
        max_from: V::I,
        other1: &impl IterableVec<V::I, A>,
        other2: &impl IterableVec<V::I, B>,
        other3: &impl IterableVec<V::I, C>,
        mut t: F,
        exit: &Exit,
    ) -> Result<()>
    where
        A: VecValue,
        B: VecValue,
        C: VecValue,
        F: FnMut((V::I, A, B, C, &Self)) -> (V::I, V::T),
    {
        self.validate_computed_version_or_reset(
            Version::ZERO
                + self.inner_version()
                + other1.version()
                + other2.version()
                + other3.version(),
        )?;

        self.repeat_until_complete(exit, |this| {
            let skip = this.len().min(max_from.to_usize());
            let mut iter2 = other2.iter().skip(skip);
            let mut iter3 = other3.iter().skip(skip);

            for (i, b) in other1.iter().enumerate().skip(skip) {
                let (i, v) = t((
                    V::I::from(i),
                    b,
                    iter2.next().unwrap(),
                    iter3.next().unwrap(),
                    this,
                ));
                this.truncate_push(i, v)?;

                if this.batch_limit_reached() {
                    break;
                }
            }

            Ok(())
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn compute_transform4<A, B, C, D, F>(
        &mut self,
        max_from: V::I,
        other1: &impl IterableVec<V::I, A>,
        other2: &impl IterableVec<V::I, B>,
        other3: &impl IterableVec<V::I, C>,
        other4: &impl IterableVec<V::I, D>,
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
        self.validate_computed_version_or_reset(
            Version::ZERO
                + self.inner_version()
                + other1.version()
                + other2.version()
                + other3.version()
                + other4.version(),
        )?;

        self.repeat_until_complete(exit, |this| {
            let skip = this.len().min(max_from.to_usize());
            let mut iter2 = other2.iter().skip(skip);
            let mut iter3 = other3.iter().skip(skip);
            let mut iter4 = other4.iter().skip(skip);

            for (i, b) in other1.iter().enumerate().skip(skip) {
                let (i, v) = t((
                    V::I::from(i),
                    b,
                    iter2.next().unwrap(),
                    iter3.next().unwrap(),
                    iter4.next().unwrap(),
                    this,
                ));
                this.truncate_push(i, v)?;

                if this.batch_limit_reached() {
                    break;
                }
            }

            Ok(())
        })
    }

    pub fn compute_coarser(
        &mut self,
        max_from: V::T,
        other: &impl IterableVec<V::T, V::I>,
        exit: &Exit,
    ) -> Result<()>
    where
        V::I: VecValue + VecIndex,
        V::T: VecIndex,
    {
        self.validate_computed_version_or_reset(
            Version::ZERO + self.inner_version() + other.version(),
        )?;

        self.repeat_until_complete(exit, |this| {
            let skip = max_from
                .to_usize()
                .min(this.iter().last().map_or(0_usize, |v| v.to_usize()));

            let mut prev_i = None;
            for (v, i) in other.iter().enumerate().skip(skip) {
                let v = V::T::from(v);
                if prev_i.is_some_and(|prev_i| prev_i == i) {
                    continue;
                }
                if this
                    .get_pushed_or_read_once(i)?
                    .is_none_or(|old_v| old_v > v)
                {
                    this.truncate_push(i, v)?;
                }
                prev_i.replace(i);

                if this.batch_limit_reached() {
                    break;
                }
            }

            Ok(())
        })
    }
}
