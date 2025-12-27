use crate::{
    AnyVec, Error, Exit, GenericStoredVec, IterableVec, Result, StoredVec, VecValue, Version,
};

use super::{CheckedSub, EagerVec};

impl<V> EagerVec<V>
where
    V: StoredVec,
    V::I: CheckedSub,
{
    fn compute_with_lookback<A, F>(
        &mut self,
        max_from: V::I,
        source: &impl IterableVec<V::I, A>,
        lookback_len: usize,
        exit: &Exit,
        transform: F,
    ) -> Result<()>
    where
        A: VecValue + Default,
        F: Fn(usize, A, A) -> V::T,
    {
        self.validate_computed_version_or_reset(
            Version::ZERO + self.inner_version() + source.version(),
        )?;

        self.truncate_if_needed(max_from)?;

        self.repeat_until_complete(exit, |this| {
            let skip = this.len();
            let mut lookback = source.create_lookback(skip, lookback_len, 0);

            for (i, current) in source.iter().enumerate().skip(skip) {
                let previous = lookback.get_and_push(i, current.clone(), A::default());
                let result = transform(i, current, previous);
                this.checked_push_at(i, result)?;

                if this.batch_limit_reached() {
                    break;
                }
            }

            Ok(())
        })
    }

    pub fn compute_previous_value<A>(
        &mut self,
        max_from: V::I,
        source: &impl IterableVec<V::I, A>,
        len: usize,
        exit: &Exit,
    ) -> Result<()>
    where
        A: VecValue + Default,
        f32: From<A>,
        V::T: From<f32>,
    {
        self.compute_with_lookback(max_from, source, len, exit, |i, _, previous| {
            // If there's no previous value (i < len), return NaN
            if i < len {
                V::T::from(f32::NAN)
            } else {
                V::T::from(f32::from(previous))
            }
        })
    }

    pub fn compute_change(
        &mut self,
        max_from: V::I,
        source: &impl IterableVec<V::I, V::T>,
        len: usize,
        exit: &Exit,
    ) -> Result<()>
    where
        V::T: CheckedSub + Default,
    {
        self.compute_with_lookback(max_from, source, len, exit, |i, current, previous| {
            // If there's no previous value (i < len), return 0 (no change)
            if i < len {
                V::T::default()
            } else {
                current.checked_sub(previous).unwrap()
            }
        })
    }

    pub fn compute_percentage_change<A>(
        &mut self,
        max_from: V::I,
        source: &impl IterableVec<V::I, A>,
        len: usize,
        exit: &Exit,
    ) -> Result<()>
    where
        A: VecValue + Default,
        f32: From<A>,
        V::T: From<f32>,
    {
        self.compute_with_lookback(max_from, source, len, exit, |i, current, previous| {
            // If there's no previous value (i < len), return NaN
            if i < len {
                V::T::from(f32::NAN)
            } else {
                let current_f32 = f32::from(current);
                let previous_f32 = f32::from(previous);
                V::T::from(((current_f32 / previous_f32) - 1.0) * 100.0)
            }
        })
    }

    pub fn compute_cagr<A>(
        &mut self,
        max_from: V::I,
        percentage_returns: &impl IterableVec<V::I, A>,
        days: usize,
        exit: &Exit,
    ) -> Result<()>
    where
        A: VecValue + Default,
        f32: From<A>,
        V::T: From<f32>,
    {
        if days == 0 || !days.is_multiple_of(365) {
            return Err(Error::InvalidArgument(
                "days must be non-zero and a multiple of 365",
            ));
        }

        let years = days / 365;

        self.compute_transform(
            max_from,
            percentage_returns,
            |(i, percentage, ..)| {
                let cagr = (((f32::from(percentage) / 100.0 + 1.0).powf(1.0 / years as f32)) - 1.0)
                    * 100.0;
                (i, V::T::from(cagr))
            },
            exit,
        )
    }
}
