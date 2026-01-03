use std::{
    collections::VecDeque,
    iter::Sum,
    ops::{Div, Sub},
};

use crate::{
    AnyVec, CollectableVec, Error, Exit, GenericStoredVec, IterableVec, Result, StoredVec,
    VecIndex, VecValue, Version,
};

use super::{CheckedSub, EagerVec};

impl<V> EagerVec<V>
where
    V: StoredVec,
{
    fn compute_monotonic_window<A, F>(
        &mut self,
        max_from: V::I,
        source: &impl IterableVec<V::I, A>,
        window: usize,
        exit: &Exit,
        should_pop: F,
    ) -> Result<()>
    where
        A: VecValue + Ord,
        V::T: From<A>,
        F: Fn(&A, &A) -> bool,
    {
        self.validate_computed_version_or_reset(source.version())?;

        self.truncate_if_needed(max_from)?;

        self.repeat_until_complete(exit, |this| {
            let skip = this.len();
            let mut deque: VecDeque<(usize, A)> = VecDeque::new();

            for (i, value) in source
                .iter()
                .enumerate()
                .skip(skip.checked_sub(window).unwrap_or_default())
            {
                // Remove elements outside the window from front
                while let Some(&(idx, _)) = deque.front() {
                    if i >= window && idx <= i - window {
                        deque.pop_front();
                    } else {
                        break;
                    }
                }

                // Remove elements that don't maintain monotonic property
                while let Some((_, v)) = deque.back() {
                    if should_pop(v, &value) {
                        deque.pop_back();
                    } else {
                        break;
                    }
                }

                deque.push_back((i, value));

                if i < skip {
                    continue;
                }

                let v = deque.front().unwrap().1.clone();
                this.checked_push_at(i, V::T::from(v))?;

                if this.batch_limit_reached() {
                    break;
                }
            }

            Ok(())
        })
    }

    pub fn compute_max<A>(
        &mut self,
        max_from: V::I,
        source: &impl IterableVec<V::I, A>,
        window: usize,
        exit: &Exit,
    ) -> Result<()>
    where
        A: VecValue + Ord,
        V::T: From<A>,
    {
        self.compute_monotonic_window(max_from, source, window, exit, |v, value| v < value)
    }

    pub fn compute_min<A>(
        &mut self,
        max_from: V::I,
        source: &impl IterableVec<V::I, A>,
        window: usize,
        exit: &Exit,
    ) -> Result<()>
    where
        A: VecValue + Ord,
        V::T: From<A>,
    {
        self.compute_monotonic_window(max_from, source, window, exit, |v, value| v > value)
    }

    pub fn compute_sum<A>(
        &mut self,
        max_from: V::I,
        source: &impl IterableVec<V::I, A>,
        window: usize,
        exit: &Exit,
    ) -> Result<()>
    where
        V::T: std::ops::Add<V::T, Output = V::T> + From<A> + Default + CheckedSub,
        A: VecValue,
    {
        self.validate_computed_version_or_reset(Version::ONE + source.version())?;

        self.truncate_if_needed(max_from)?;

        self.repeat_until_complete(exit, |this| {
            let skip = this.len();
            let mut prev = skip
                .checked_sub(1)
                .and_then(|prev_i| this.iter().get(V::I::from(prev_i)))
                .or(Some(V::T::default()));

            // Initialize buffer for sliding window sum
            let mut window_values = if window < usize::MAX {
                VecDeque::with_capacity(window + 1)
            } else {
                VecDeque::new()
            };

            if skip > 0 {
                let start = skip.saturating_sub(window);
                source.iter().skip(start).take(skip - start).for_each(|v| {
                    window_values.push_back(V::T::from(v));
                });
            }

            for (i, value) in source.iter().enumerate().skip(skip) {
                let value = V::T::from(value);

                let processed_values_count = i.to_usize() + 1;
                let len = (processed_values_count).min(window);

                let sum = if processed_values_count > len {
                    let prev_sum = prev.as_ref().unwrap().clone();
                    // Pop the oldest value from our window buffer (we own it, no clone needed)
                    let value_to_subtract = window_values.pop_front().unwrap();
                    prev_sum
                        .checked_sub(value_to_subtract)
                        .ok_or(Error::Underflow)?
                        + value.clone()
                } else {
                    prev.as_ref().unwrap().clone() + value.clone()
                };

                // Add current value to window buffer
                window_values.push_back(value);
                if window_values.len() > window {
                    window_values.pop_front();
                }

                prev.replace(sum.clone());
                this.checked_push_at(i, sum)?;

                if this.batch_limit_reached() {
                    break;
                }
            }

            Ok(())
        })
    }

    pub fn compute_sma<A>(
        &mut self,
        max_from: V::I,
        source: &impl IterableVec<V::I, A>,
        sma: usize,
        exit: &Exit,
    ) -> Result<()>
    where
        V::T: std::ops::Add<V::T, Output = V::T> + From<A> + From<f32>,
        A: VecValue,
        f32: From<V::T> + From<A>,
    {
        self.compute_sma_(max_from, source, sma, exit, None)
    }

    pub fn compute_sma_<A>(
        &mut self,
        max_from: V::I,
        source: &impl IterableVec<V::I, A>,
        sma: usize,
        exit: &Exit,
        min_i: Option<V::I>,
    ) -> Result<()>
    where
        V::T: std::ops::Add<V::T, Output = V::T> + From<A> + From<f32>,
        A: VecValue,
        f32: From<V::T> + From<A>,
    {
        self.validate_computed_version_or_reset(Version::ONE + source.version())?;

        self.truncate_if_needed(max_from)?;

        self.repeat_until_complete(exit, |this| {
            let skip = this.len();
            let min_i = min_i.map(|i| i.to_usize());
            let min_prev_i = min_i.unwrap_or_default();

            let mut prev = skip
                .checked_sub(1)
                .and_then(|prev_i| {
                    if prev_i > min_prev_i {
                        this.iter().get(V::I::from(prev_i))
                    } else {
                        Some(V::T::from(0.0))
                    }
                })
                .or(Some(V::T::from(0.0)));

            // Initialize buffer for sliding window SMA
            let mut window_values = if sma < usize::MAX {
                VecDeque::with_capacity(sma + 1)
            } else {
                VecDeque::new()
            };

            if skip > 0 {
                let start = skip.saturating_sub(sma).max(min_prev_i);
                source.iter().skip(start).take(skip - start).for_each(|v| {
                    window_values.push_back(f32::from(v));
                });
            }

            for (i, value) in source.iter().enumerate().skip(skip) {
                if min_i.is_none() || min_i.is_some_and(|min_i| min_i <= i) {
                    let processed_values_count = i.to_usize() - min_prev_i + 1;
                    let len = (processed_values_count).min(sma);

                    let value = f32::from(value);

                    let sma_result = V::T::from(if processed_values_count > sma {
                        let prev_sum = f32::from(prev.as_ref().unwrap().clone()) * len as f32;
                        // Pop the oldest value from our window buffer
                        let value_to_subtract = window_values.pop_front().unwrap();
                        (prev_sum - value_to_subtract + value) / len as f32
                    } else {
                        (f32::from(prev.as_ref().unwrap().clone()) * (len - 1) as f32 + value)
                            / len as f32
                    });

                    // Add current value to window buffer
                    window_values.push_back(value);
                    if window_values.len() > sma {
                        window_values.pop_front();
                    }

                    prev.replace(sma_result.clone());
                    this.checked_push_at(i, sma_result)?;
                } else {
                    this.checked_push_at(i, V::T::from(f32::NAN))?;
                }

                if this.batch_limit_reached() {
                    break;
                }
            }

            Ok(())
        })
    }

    pub fn compute_ema<A>(
        &mut self,
        max_from: V::I,
        source: &impl CollectableVec<V::I, A>,
        ema: usize,
        exit: &Exit,
    ) -> Result<()>
    where
        V::T: From<A> + From<f32>,
        A: VecValue + Sum,
        f32: From<A> + From<V::T>,
    {
        self.compute_ema_(max_from, source, ema, exit, None)
    }

    pub fn compute_ema_<A>(
        &mut self,
        max_from: V::I,
        source: &impl CollectableVec<V::I, A>,
        ema: usize,
        exit: &Exit,
        min_i: Option<V::I>,
    ) -> Result<()>
    where
        V::T: From<A> + From<f32>,
        A: VecValue + Sum,
        f32: From<A> + From<V::T>,
    {
        self.validate_computed_version_or_reset(Version::new(3) + source.version())?;

        self.truncate_if_needed(max_from)?;

        let smoothing: f32 = 2.0;
        let k = smoothing / (ema as f32 + 1.0);
        let _1_minus_k = 1.0 - k;

        self.repeat_until_complete(exit, |this| {
            let skip = this.len();
            let min_i = min_i.map(|i| i.to_usize());
            let min_prev_i = min_i.unwrap_or_default();

            let mut prev = skip
                .checked_sub(1)
                .and_then(|prev_i| {
                    if prev_i >= min_prev_i {
                        this.iter().get(V::I::from(prev_i))
                    } else {
                        Some(V::T::from(0.0))
                    }
                })
                .or(Some(V::T::from(0.0)));

            for (index, value) in source.iter().enumerate().skip(skip) {
                let value = value;

                if min_i.is_none() || min_i.is_some_and(|min_i| min_i <= index) {
                    let processed_values_count = index - min_prev_i + 1;

                    let value = f32::from(value);

                    let ema = if processed_values_count > ema {
                        let prev = f32::from(prev.as_ref().unwrap().clone());
                        let prev = if prev.is_nan() { 0.0 } else { prev };
                        V::T::from((value * k) + (prev * _1_minus_k))
                    } else {
                        let len = (processed_values_count).min(ema);
                        let prev = f32::from(prev.as_ref().unwrap().clone());
                        V::T::from((prev * (len - 1) as f32 + value) / len as f32)
                    };

                    prev.replace(ema.clone());
                    this.checked_push_at(index, ema)?;
                } else {
                    this.checked_push_at(index, V::T::from(f32::NAN))?;
                }

                if this.batch_limit_reached() {
                    break;
                }
            }

            Ok(())
        })
    }

    /// Compute Wilder's Running Moving Average (RMA).
    /// Uses alpha = 1/period instead of EMA's 2/(period+1).
    /// This is the standard smoothing method for RSI.
    pub fn compute_rma<A>(
        &mut self,
        max_from: V::I,
        source: &impl CollectableVec<V::I, A>,
        period: usize,
        exit: &Exit,
    ) -> Result<()>
    where
        V::T: From<A> + From<f32>,
        A: VecValue + Sum,
        f32: From<A> + From<V::T>,
    {
        self.validate_computed_version_or_reset(Version::new(4) + source.version())?;

        self.truncate_if_needed(max_from)?;

        // Wilder's smoothing: alpha = 1/period
        let k = 1.0 / period as f32;
        let _1_minus_k = 1.0 - k;

        self.repeat_until_complete(exit, |this| {
            let skip = this.len();

            let mut prev = skip
                .checked_sub(1)
                .and_then(|prev_i| this.iter().get(V::I::from(prev_i)))
                .or(Some(V::T::from(0.0)));

            for (index, value) in source.iter().enumerate().skip(skip) {
                let processed_values_count = index + 1;
                let value = f32::from(value);

                let rma = if processed_values_count > period {
                    // After initial period: RMA = prev * (1 - 1/period) + current * (1/period)
                    let prev = f32::from(prev.as_ref().unwrap().clone());
                    let prev = if prev.is_nan() { 0.0 } else { prev };
                    V::T::from((value * k) + (prev * _1_minus_k))
                } else {
                    // Initial period: use SMA (cumulative average)
                    let len = processed_values_count.min(period);
                    let prev = f32::from(prev.as_ref().unwrap().clone());
                    V::T::from((prev * (len - 1) as f32 + value) / len as f32)
                };

                prev.replace(rma.clone());
                this.checked_push_at(index, rma)?;

                if this.batch_limit_reached() {
                    break;
                }
            }

            Ok(())
        })
    }

    fn compute_all_time_extreme<A, F>(
        &mut self,
        max_from: V::I,
        source: &impl IterableVec<V::I, A>,
        exit: &Exit,
        compare: F,
        exclude_default: bool,
    ) -> Result<()>
    where
        V::T: From<A> + Ord + Default,
        A: VecValue,
        F: Fn(V::T, V::T) -> V::T + Copy,
    {
        let mut prev = None;
        self.compute_transform(
            max_from,
            source,
            |(i, v, this)| {
                if prev.is_none() {
                    let i = i.to_usize();
                    prev.replace(if i > 0 {
                        this.iter().nth(i - 1).unwrap()
                    } else {
                        V::T::from(source.iter().next().unwrap())
                    });
                }
                let v = V::T::from(v);
                let extreme = compare(prev.as_ref().unwrap().clone(), v.clone());

                prev.replace(if !exclude_default || extreme != V::T::default() {
                    extreme.clone()
                } else {
                    // Reverse the comparison if excluding default
                    if &extreme == prev.as_ref().unwrap() {
                        v
                    } else {
                        prev.as_ref().unwrap().clone()
                    }
                });
                (i, extreme)
            },
            exit,
        )
    }

    /// Computes the all time high of a source.
    /// This version is more optimized than `compute_max` with a window set to `usize::MAX`.
    pub fn compute_all_time_high<A>(
        &mut self,
        max_from: V::I,
        source: &impl IterableVec<V::I, A>,
        exit: &Exit,
    ) -> Result<()>
    where
        V::T: From<A> + Ord + Default,
        A: VecValue,
    {
        self.compute_all_time_extreme(max_from, source, exit, |prev, v| prev.max(v), false)
    }

    /// Computes the all time low of a source.
    /// This version is more optimized than `compute_min` with a window set to `usize::MAX`.
    pub fn compute_all_time_low<A>(
        &mut self,
        max_from: V::I,
        source: &impl IterableVec<V::I, A>,
        exit: &Exit,
    ) -> Result<()>
    where
        V::T: From<A> + Ord + Default,
        A: VecValue,
    {
        self.compute_all_time_low_(max_from, source, exit, false)
    }

    /// Computes the all time low of a source.
    /// This version is more optimized than `compute_min` with a window set to `usize::MAX`.
    pub fn compute_all_time_low_<A>(
        &mut self,
        max_from: V::I,
        source: &impl IterableVec<V::I, A>,
        exit: &Exit,
        exclude_default: bool,
    ) -> Result<()>
    where
        V::T: From<A> + Ord + Default,
        A: VecValue,
    {
        self.compute_all_time_extreme(
            max_from,
            source,
            exit,
            |prev, v| prev.min(v),
            exclude_default,
        )
    }

    pub fn compute_zscore<A, B, C>(
        &mut self,
        max_from: V::I,
        source: &impl IterableVec<V::I, A>,
        sma: &impl IterableVec<V::I, B>,
        sd: &impl IterableVec<V::I, C>,
        exit: &Exit,
    ) -> Result<()>
    where
        V::T: From<f32>,
        A: VecValue + Sub<B, Output = A> + Div<C, Output = V::T>,
        B: VecValue,
        C: VecValue,
        f32: From<A> + From<B> + From<C>,
    {
        self.compute_transform3(
            max_from,
            source,
            sma,
            sd,
            |(i, ratio, sma, sd, ..)| (i, (ratio - sma) / sd),
            exit,
        )
    }
}
