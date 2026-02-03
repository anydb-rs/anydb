use std::{
    collections::VecDeque,
    iter::Sum,
    ops::{AddAssign, Div, Sub, SubAssign},
};

use crate::{
    AnyVec, CollectableVec, Error, Exit, GenericStoredVec, IterableVec, Result, StoredVec,
    VecIndex, VecValue, Version,
};

use super::{CheckedSub, EagerVec};

/// Helper for rolling window computations.
struct RollingWindow {
    values: VecDeque<f32>,
    window: usize,
}

impl RollingWindow {
    fn new(window: usize) -> Self {
        Self {
            values: VecDeque::with_capacity(window + 1),
            window,
        }
    }

    fn init_from_source<I, A>(
        &mut self,
        source: &impl IterableVec<I, A>,
        skip: usize,
        min_i: usize,
    ) where
        I: VecIndex,
        A: VecValue,
        f32: From<A>,
    {
        if skip > 0 {
            let start = skip.saturating_sub(self.window).max(min_i);
            source.iter().skip(start).take(skip - start).for_each(|v| {
                self.values.push_back(f32::from(v));
            });
        }
    }

    fn push(&mut self, value: f32) {
        self.values.push_back(value);
        if self.values.len() > self.window {
            self.values.pop_front();
        }
    }

    fn push_and_pop(&mut self, value: f32) -> Option<f32> {
        self.values.push_back(value);
        if self.values.len() > self.window {
            self.values.pop_front()
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn median(&self) -> f32 {
        let mut sorted: Vec<f32> = self.values.iter().copied().collect();
        sorted.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        if sorted.len() % 2 == 0 {
            let mid = sorted.len() / 2;
            (sorted[mid - 1] + sorted[mid]) / 2.0
        } else {
            sorted[sorted.len() / 2]
        }
    }

    fn sma(&mut self, value: f32, prev_sma: f32) -> f32 {
        let len = self.len();
        let popped = self.push_and_pop(value);
        match popped {
            Some(old) => {
                let prev_sum = prev_sma * len as f32;
                (prev_sum - old + value) / len as f32
            }
            None => (prev_sma * len as f32 + value) / (len + 1) as f32,
        }
    }
}

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

    /// Compute rolling sum with variable window starts.
    /// For each index i, computes sum of values from window_starts[i] to i (inclusive).
    pub fn compute_rolling_sum<A>(
        &mut self,
        max_from: V::I,
        window_starts: &impl IterableVec<V::I, V::I>,
        values: &impl IterableVec<V::I, A>,
        exit: &Exit,
    ) -> Result<()>
    where
        A: VecValue,
        V::T: From<A> + Default + AddAssign + SubAssign,
    {
        self.validate_computed_version_or_reset(window_starts.version() + values.version())?;

        self.truncate_if_needed(max_from)?;

        self.repeat_until_complete(exit, |this| {
            let skip = this.len();

            // Initialize running sum and prev_start from previous state
            let mut values_iter = values.iter();

            let (mut running_sum, mut prev_start) = if skip > 0 {
                let prev_idx = V::I::from(skip - 1);
                let prev_start = window_starts.iter().get_unwrap(prev_idx);
                let sum = this.iter().get_unwrap(prev_idx);
                (sum, prev_start)
            } else {
                (V::T::default(), V::I::from(0))
            };

            for (i, (start, value)) in window_starts
                .iter()
                .zip(values.iter())
                .enumerate()
                .skip(skip)
            {
                // Add current value to sum
                running_sum += V::T::from(value);

                // Subtract values that fell out of the window
                while prev_start < start {
                    running_sum -= V::T::from(values_iter.get_unwrap(prev_start));
                    prev_start = V::I::from(prev_start.to_usize() + 1);
                }

                this.checked_push_at(i, running_sum.clone())?;

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
        window: usize,
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

            let mut prev_sma = skip
                .checked_sub(1)
                .and_then(|prev_i| {
                    if prev_i > min_prev_i {
                        this.iter().get(V::I::from(prev_i)).map(|v| f32::from(v))
                    } else {
                        Some(0.0)
                    }
                })
                .unwrap_or(0.0);

            let mut rolling = RollingWindow::new(window);
            rolling.init_from_source(source, skip, min_prev_i);

            for (i, value) in source.iter().enumerate().skip(skip) {
                if min_i.is_none() || min_i.is_some_and(|min_i| min_i <= i) {
                    let sma_result = rolling.sma(f32::from(value), prev_sma);
                    prev_sma = sma_result;
                    this.checked_push_at(i, V::T::from(sma_result))?;
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

    pub fn compute_rolling_median<A>(
        &mut self,
        max_from: V::I,
        source: &impl IterableVec<V::I, A>,
        window: usize,
        exit: &Exit,
    ) -> Result<()>
    where
        V::T: From<f32>,
        A: VecValue,
        f32: From<A>,
    {
        self.validate_computed_version_or_reset(Version::ONE + source.version())?;

        self.truncate_if_needed(max_from)?;

        self.repeat_until_complete(exit, |this| {
            let skip = this.len();

            let mut rolling = RollingWindow::new(window);
            rolling.init_from_source(source, skip, 0);

            for (i, value) in source.iter().enumerate().skip(skip) {
                rolling.push(f32::from(value));

                this.checked_push_at(i, V::T::from(rolling.median()))?;

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

    /// Computes the all time high starting from a specific index.
    /// Values before `from` will be the default value (typically 0).
    pub fn compute_all_time_high_from<A>(
        &mut self,
        max_from: V::I,
        source: &impl IterableVec<V::I, A>,
        from: V::I,
        exit: &Exit,
    ) -> Result<()>
    where
        V::T: From<A> + Ord + Default + Copy,
        A: VecValue,
    {
        let from_usize = from.to_usize();
        let mut prev: Option<V::T> = None;
        self.compute_transform(
            max_from,
            source,
            |(i, v, this)| {
                let idx = i.to_usize();
                if prev.is_none() {
                    prev = Some(if idx > 0 {
                        this.read_at_unwrap_once(idx - 1)
                    } else {
                        V::T::default()
                    });
                }
                if idx >= from_usize {
                    *prev.as_mut().unwrap() = prev.unwrap().max(V::T::from(v));
                }
                (i, prev.unwrap())
            },
            exit,
        )
    }

    /// Computes the all time low starting from a specific index.
    /// Values before `from` will be the default value (typically 0).
    pub fn compute_all_time_low_from<A>(
        &mut self,
        max_from: V::I,
        source: &impl IterableVec<V::I, A>,
        from: V::I,
        exit: &Exit,
    ) -> Result<()>
    where
        V::T: From<A> + Ord + Default + Copy,
        A: VecValue,
    {
        let from_usize = from.to_usize();
        let mut prev: Option<V::T> = None;
        self.compute_transform(
            max_from,
            source,
            |(i, v, this)| {
                let idx = i.to_usize();
                if prev.is_none() {
                    prev = Some(if idx > 0 {
                        this.read_at_unwrap_once(idx - 1)
                    } else {
                        V::T::default()
                    });
                }
                if idx >= from_usize {
                    *prev.as_mut().unwrap() = prev.unwrap().min(V::T::from(v));
                }
                (i, prev.unwrap())
            },
            exit,
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
