use std::{
    collections::VecDeque,
    f32,
    fmt::Debug,
    iter::Sum,
    ops::{Add, Div, Mul, Sub},
    panic,
    path::PathBuf,
};

use rawdb::{Database, Reader, Region};

mod checked_sub;
mod saturating_add;

pub use checked_sub::*;
pub use saturating_add::*;

use crate::{
    AnyStoredVec, AnyVec, BoxedVecIterator, CollectableVec, Exit, GenericStoredVec, Header,
    ImportOptions, Importable, IterableVec, PrintableIndex, Result, TypedStoredVec, TypedVec,
    VecIndex, VecValue, Version,
};

/// Stored vector with eager computation methods for deriving values from other vectors.
///
/// Wraps any stored vec type and provides various computation methods (transform, arithmetic operations,
/// moving averages, etc.) to eagerly compute and persist derived data. Results are stored on disk
/// and incrementally updated when source data changes.
#[derive(Debug, Clone)]
#[must_use = "Vector should be stored to keep data accessible"]
pub struct EagerVec<V>(pub V);

impl<V: Importable> Importable for EagerVec<V> {
    fn import(db: &Database, name: &str, version: Version) -> Result<Self> {
        Ok(Self(V::import(db, name, version)?))
    }

    fn import_with(options: ImportOptions) -> Result<Self> {
        Ok(Self(V::import_with(options)?))
    }

    fn forced_import(db: &Database, name: &str, version: Version) -> Result<Self> {
        Ok(Self(V::forced_import(db, name, version)?))
    }

    fn forced_import_with(options: ImportOptions) -> Result<Self> {
        Ok(Self(V::forced_import_with(options)?))
    }
}

impl<V> EagerVec<V>
where
    V: TypedStoredVec + GenericStoredVec<V::I, V::T> + IterableVec<V::I, V::T> + Clone,
    V::I: VecIndex,
    V::T: VecValue,
{
    #[inline]
    pub fn inner_version(&self) -> Version {
        self.0.header().vec_version()
    }

    /// Helper that repeatedly calls a compute function until it completes.
    /// Flushes between iterations when batch limit is hit.
    fn repeat_until_complete<F>(&mut self, exit: &Exit, mut f: F) -> Result<()>
    where
        F: FnMut(&mut Self) -> Result<()>,
    {
        loop {
            f(self)?;
            let batch_limit_reached = self.batch_limit_reached();
            self.safe_flush(exit)?;
            if !batch_limit_reached {
                break;
            }
        }

        Ok(())
    }

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
        })
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

    pub fn compute_add(
        &mut self,
        max_from: V::I,
        added: &impl IterableVec<V::I, V::T>,
        adder: &impl IterableVec<V::I, V::T>,
        exit: &Exit,
    ) -> Result<()>
    where
        V::T: Add<Output = V::T>,
    {
        self.compute_transform2(
            max_from,
            added,
            adder,
            |(i, v1, v2, ..)| (i, (v1 + v2)),
            exit,
        )
    }

    pub fn compute_subtract(
        &mut self,
        max_from: V::I,
        subtracted: &impl IterableVec<V::I, V::T>,
        subtracter: &impl IterableVec<V::I, V::T>,
        exit: &Exit,
    ) -> Result<()>
    where
        V::T: CheckedSub,
    {
        self.compute_transform2(
            max_from,
            subtracted,
            subtracter,
            |(i, v1, v2, ..)| (i, (v1.checked_sub(v2).unwrap())),
            exit,
        )
    }

    pub fn compute_multiply<A, B>(
        &mut self,
        max_from: V::I,
        multiplied: &impl IterableVec<V::I, A>,
        multiplier: &impl IterableVec<V::I, B>,
        exit: &Exit,
    ) -> Result<()>
    where
        A: VecValue,
        B: VecValue,
        V::T: From<A> + Mul<B, Output = V::T>,
    {
        self.compute_transform2(
            max_from,
            multiplied,
            multiplier,
            |(i, v1, v2, ..)| (i, V::T::from(v1) * v2),
            exit,
        )
    }

    pub fn compute_divide<A, B>(
        &mut self,
        max_from: V::I,
        divided: &impl IterableVec<V::I, A>,
        divider: &impl IterableVec<V::I, B>,
        exit: &Exit,
    ) -> Result<()>
    where
        A: VecValue,
        B: VecValue,
        V::T: From<A> + Div<B, Output = V::T>,
    {
        self.compute_transform2(
            max_from,
            divided,
            divider,
            |(i, v1, v2, ..)| (i, V::T::from(v1) / v2),
            exit,
        )
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

    pub fn compute_percentage<A, B>(
        &mut self,
        max_from: V::I,
        divided: &impl IterableVec<V::I, A>,
        divider: &impl IterableVec<V::I, B>,
        exit: &Exit,
    ) -> Result<()>
    where
        A: VecValue,
        B: VecValue,
        V::T: From<A>
            + From<B>
            + From<u8>
            + Mul<V::T, Output = V::T>
            + Div<V::T, Output = V::T>
            + Sub<V::T, Output = V::T>
            + Copy,
    {
        self.compute_percentage_(max_from, divided, divider, exit, false)
    }

    pub fn compute_percentage_difference<A, B>(
        &mut self,
        max_from: V::I,
        divided: &impl IterableVec<V::I, A>,
        divider: &impl IterableVec<V::I, B>,
        exit: &Exit,
    ) -> Result<()>
    where
        A: VecValue,
        B: VecValue,
        V::T: From<A>
            + From<B>
            + From<u8>
            + Mul<V::T, Output = V::T>
            + Div<V::T, Output = V::T>
            + Sub<V::T, Output = V::T>
            + Copy,
    {
        self.compute_percentage_(max_from, divided, divider, exit, true)
    }

    pub fn compute_percentage_<A, B>(
        &mut self,
        max_from: V::I,
        divided: &impl IterableVec<V::I, A>,
        divider: &impl IterableVec<V::I, B>,
        exit: &Exit,
        as_difference: bool,
    ) -> Result<()>
    where
        A: VecValue,
        B: VecValue,
        V::T: From<A>
            + From<B>
            + From<u8>
            + Mul<V::T, Output = V::T>
            + Div<V::T, Output = V::T>
            + Sub<V::T, Output = V::T>
            + Copy,
    {
        let multiplier = V::T::from(100u8);
        self.compute_transform2(
            max_from,
            divided,
            divider,
            move |(i, v1, v2, ..)| {
                let divided = V::T::from(v1);
                let divider = V::T::from(v2);
                let v = divided * multiplier;
                let mut v = v / divider;
                if as_difference {
                    v = v - multiplier;
                }
                (i, v)
            },
            exit,
        )
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

    pub fn compute_count_from_indexes<A, B>(
        &mut self,
        max_from: V::I,
        first_indexes: &impl IterableVec<V::I, A>,
        other_to_else: &impl IterableVec<A, B>,
        exit: &Exit,
    ) -> Result<()>
    where
        V::T: From<A>,
        A: VecValue
            + VecIndex
            + Copy
            + Add<usize, Output = A>
            + CheckedSub<A>
            + TryInto<V::T>
            + Default,
        <A as TryInto<V::T>>::Error: core::error::Error + 'static,
        B: VecValue,
    {
        self.compute_filtered_count_from_indexes(
            max_from,
            first_indexes,
            other_to_else,
            |_| true,
            exit,
        )
    }

    pub fn compute_filtered_count_from_indexes<A, B>(
        &mut self,
        max_from: V::I,
        first_indexes: &impl IterableVec<V::I, A>,
        other_to_else: &impl IterableVec<A, B>,
        mut filter: impl FnMut(A) -> bool,
        exit: &Exit,
    ) -> Result<()>
    where
        V::T: From<A>,
        A: VecValue
            + VecIndex
            + Copy
            + Add<usize, Output = A>
            + CheckedSub<A>
            + TryInto<V::T>
            + Default,
        B: VecValue,
        <A as TryInto<V::T>>::Error: core::error::Error + 'static,
    {
        self.validate_computed_version_or_reset(
            Version::ZERO
                + self.inner_version()
                + first_indexes.version()
                + other_to_else.version(),
        )?;

        self.repeat_until_complete(exit, |this| {
            let skip = max_from.to_usize().min(this.len());
            let mut other_iter = first_indexes.iter();

            for (i, first_index) in first_indexes.iter().enumerate().skip(skip) {
                let end = other_iter
                    .get_at(i + 1)
                    .map(|v| v.to_usize())
                    .unwrap_or_else(|| other_to_else.len());

                let range = first_index.to_usize()..end;
                let count = range.into_iter().filter(|i| filter(A::from(*i))).count();
                this.truncate_push_at(i, V::T::from(A::from(count)))?;

                if this.batch_limit_reached() {
                    break;
                }
            }

            Ok(())
        })
    }

    pub fn compute_is_first_ordered<A>(
        &mut self,
        max_from: V::I,
        self_to_other: &impl IterableVec<V::I, A>,
        other_to_self: &impl IterableVec<A, V::I>,
        exit: &Exit,
    ) -> Result<()>
    where
        V::I: VecValue,
        V::T: From<bool>,
        A: VecIndex + VecValue,
    {
        self.validate_computed_version_or_reset(
            Version::ZERO
                + self.inner_version()
                + self_to_other.version()
                + other_to_self.version(),
        )?;

        self.repeat_until_complete(exit, |this| {
            let skip = max_from.to_usize().min(this.len());
            let mut other_to_self_iter = other_to_self.iter();

            for (i, other) in self_to_other.iter().enumerate().skip(skip) {
                this.truncate_push_at(
                    i,
                    V::T::from(other_to_self_iter.get_unwrap(other).to_usize() == i),
                )?;

                if this.batch_limit_reached() {
                    break;
                }
            }

            Ok(())
        })
    }

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
        self.validate_computed_version_or_reset(
            Version::ZERO + self.inner_version() + source.version(),
        )?;

        self.repeat_until_complete(exit, |this| {
            let skip = max_from.to_usize().min(this.len());
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
                this.truncate_push_at(i, V::T::from(v))?;

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
        V::T: Add<V::T, Output = V::T> + From<A> + Default + CheckedSub,
        A: VecValue,
    {
        self.validate_computed_version_or_reset(
            Version::ONE + self.inner_version() + source.version(),
        )?;

        self.repeat_until_complete(exit, |this| {
            let skip = max_from.to_usize().min(this.len());
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
                    // Pop the oldest value from our window buffer
                    let value_to_subtract = window_values.pop_front().unwrap();
                    prev_sum
                        .clone()
                        .checked_sub(value_to_subtract.clone())
                        .unwrap_or_else(|| {
                            panic!("Underflow: prev_sum={prev_sum:?}, sub={value_to_subtract:?}")
                        })
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
                this.truncate_push_at(i, sum)?;

                if this.batch_limit_reached() {
                    break;
                }
            }

            Ok(())
        })
    }

    pub fn compute_sum_from_indexes<A, B>(
        &mut self,
        max_from: V::I,
        first_indexes: &impl IterableVec<V::I, A>,
        indexes_count: &impl IterableVec<V::I, B>,
        source: &impl IterableVec<A, V::T>,
        exit: &Exit,
    ) -> Result<()>
    where
        V::T: Default + SaturatingAdd,
        A: VecIndex + VecValue,
        B: VecValue,
        usize: From<B>,
    {
        self.compute_filtered_sum_from_indexes(
            max_from,
            first_indexes,
            indexes_count,
            source,
            |_| true,
            exit,
        )
    }

    pub fn compute_filtered_sum_from_indexes<A, B>(
        &mut self,
        max_from: V::I,
        first_indexes: &impl IterableVec<V::I, A>,
        indexes_count: &impl IterableVec<V::I, B>,
        source: &impl IterableVec<A, V::T>,
        mut filter: impl FnMut(&V::T) -> bool,
        exit: &Exit,
    ) -> Result<()>
    where
        V::T: Default + SaturatingAdd,
        A: VecIndex + VecValue,
        B: VecValue,
        usize: From<B>,
    {
        self.validate_computed_version_or_reset(
            Version::ZERO
                + self.inner_version()
                + first_indexes.version()
                + indexes_count.version()
                + source.version(),
        )?;

        self.repeat_until_complete(exit, |this| {
            let skip = max_from.to_usize().min(this.len());
            let mut source_iter = source.iter();

            // Set position once - source indices are sequential
            if let Some(starting_first_index) = first_indexes.iter().get(skip.into()) {
                source_iter.set_position(starting_first_index);
            }

            for (i, count) in indexes_count.iter().enumerate().skip(skip) {
                let count = usize::from(count);
                // Sequential read - iterator advances automatically
                let sum = (&mut source_iter)
                    .take(count)
                    .filter(|v| filter(v))
                    .fold(V::T::default(), |acc, val| acc.saturating_add(val));
                this.truncate_push_at(i, sum)?;

                if this.batch_limit_reached() {
                    break;
                }
            }

            Ok(())
        })
    }

    fn compute_aggregate_of_others<O, F>(
        &mut self,
        max_from: V::I,
        others: &[&O],
        exit: &Exit,
        aggregate: F,
    ) -> Result<()>
    where
        O: IterableVec<V::I, V::T>,
        F: Fn(Box<dyn Iterator<Item = V::T> + '_>) -> V::T,
    {
        self.validate_computed_version_or_reset(
            Version::ZERO + self.inner_version() + others.iter().map(|v| v.version()).sum(),
        )?;

        if others.is_empty() {
            unreachable!("others should've length of 1 at least");
        }

        self.repeat_until_complete(exit, |this| {
            let skip = max_from.to_usize().min(this.len());
            let mut others_iter = others
                .iter()
                .map(|v| v.iter().skip(skip))
                .collect::<Vec<_>>();

            for i in skip..others.first().unwrap().len() {
                let values = Box::new(others_iter.iter_mut().map(|iter| iter.next().unwrap()));
                let result = aggregate(values);
                this.truncate_push_at(i, result)?;

                if this.batch_limit_reached() {
                    break;
                }
            }

            Ok(())
        })
    }

    pub fn compute_sum_of_others<O>(
        &mut self,
        max_from: V::I,
        others: &[&O],
        exit: &Exit,
    ) -> Result<()>
    where
        O: IterableVec<V::I, V::T>,
        V::T: Add<V::T, Output = V::T>,
    {
        self.compute_aggregate_of_others(max_from, others, exit, |values| {
            values.reduce(|sum, v| sum + v).unwrap()
        })
    }

    pub fn compute_min_of_others<O>(
        &mut self,
        max_from: V::I,
        others: &[&O],
        exit: &Exit,
    ) -> Result<()>
    where
        O: IterableVec<V::I, V::T>,
        V::T: Add<V::T, Output = V::T> + Ord,
    {
        self.compute_aggregate_of_others(max_from, others, exit, |values| values.min().unwrap())
    }

    pub fn compute_max_of_others<O>(
        &mut self,
        max_from: V::I,
        others: &[&O],
        exit: &Exit,
    ) -> Result<()>
    where
        O: IterableVec<V::I, V::T>,
        V::T: Add<V::T, Output = V::T> + Ord,
    {
        self.compute_aggregate_of_others(max_from, others, exit, |values| values.max().unwrap())
    }

    pub fn compute_sma<A>(
        &mut self,
        max_from: V::I,
        source: &impl IterableVec<V::I, A>,
        sma: usize,
        exit: &Exit,
    ) -> Result<()>
    where
        V::T: Add<V::T, Output = V::T> + From<A> + From<f32>,
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
        V::T: Add<V::T, Output = V::T> + From<A> + From<f32>,
        A: VecValue,
        f32: From<V::T> + From<A>,
    {
        self.validate_computed_version_or_reset(
            Version::ONE + self.inner_version() + source.version(),
        )?;

        self.repeat_until_complete(exit, |this| {
            let skip = max_from.to_usize().min(this.len());
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
                    this.truncate_push_at(i, sma_result)?;
                } else {
                    this.truncate_push_at(i, V::T::from(f32::NAN))?;
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
        self.validate_computed_version_or_reset(
            Version::new(3) + self.inner_version() + source.version(),
        )?;

        let smoothing: f32 = 2.0;
        let k = smoothing / (ema as f32 + 1.0);
        let _1_minus_k = 1.0 - k;

        self.repeat_until_complete(exit, |this| {
            let skip = max_from.to_usize().min(this.len());
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
                    this.truncate_push_at(index, ema)?;
                } else {
                    this.truncate_push_at(index, V::T::from(f32::NAN))?;
                }

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
        V::I: CheckedSub,
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
        V::I: CheckedSub,
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
        V::I: CheckedSub,
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
        V::I: CheckedSub,
        A: VecValue + Default,
        f32: From<A>,
        V::T: From<f32>,
    {
        if days == 0 || !days.is_multiple_of(365) {
            panic!("bad days");
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

    /// Removes this vector and all its associated regions from the database
    pub fn remove(self) -> Result<()> {
        self.0.remove()
    }
}

// Methods that need lookback functionality
impl<V> EagerVec<V>
where
    V: TypedStoredVec
        + GenericStoredVec<V::I, V::T>
        + IterableVec<V::I, V::T>
        + CollectableVec<V::I, V::T>
        + Clone,
    V::I: VecIndex + CheckedSub,
    V::T: VecValue,
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

        self.repeat_until_complete(exit, |this| {
            let skip = max_from.to_usize().min(this.len());
            let mut lookback = source.create_lookback(skip, lookback_len, 0);

            for (i, current) in source.iter().enumerate().skip(skip) {
                let previous = lookback.get_and_push(i, current.clone(), A::default());
                let result = transform(i, current, previous);
                this.truncate_push_at(i, result)?;

                if this.batch_limit_reached() {
                    break;
                }
            }

            Ok(())
        })
    }
}

impl<V> AnyVec for EagerVec<V>
where
    V: TypedStoredVec,
    V::I: VecIndex,
    V::T: VecValue,
{
    #[inline]
    fn version(&self) -> Version {
        self.0.header().computed_version()
    }

    #[inline]
    fn name(&self) -> &str {
        self.0.name()
    }

    #[inline]
    fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    fn index_type_to_string(&self) -> &'static str {
        <V::I as PrintableIndex>::to_string()
    }

    #[inline]
    fn value_type_to_size_of(&self) -> usize {
        size_of::<V::T>()
    }

    #[inline]
    fn region_names(&self) -> Vec<String> {
        self.0.region_names()
    }
}

impl<V> AnyStoredVec for EagerVec<V>
where
    V: TypedStoredVec,
    V::I: VecIndex,
    V::T: VecValue,
{
    #[inline]
    fn db_path(&self) -> PathBuf {
        self.0.db_path()
    }

    #[inline]
    fn region(&self) -> &Region {
        self.0.region()
    }

    #[inline]
    fn header(&self) -> &Header {
        self.0.header()
    }

    #[inline]
    fn mut_header(&mut self) -> &mut Header {
        self.0.mut_header()
    }

    #[inline]
    fn saved_stamped_changes(&self) -> u16 {
        self.0.saved_stamped_changes()
    }

    #[inline]
    fn write(&mut self) -> Result<()> {
        self.0.write()
    }

    #[inline]
    fn stored_len(&self) -> usize {
        self.0.stored_len()
    }

    #[inline]
    fn real_stored_len(&self) -> usize {
        self.0.real_stored_len()
    }

    #[inline]
    fn serialize_changes(&self) -> Result<Vec<u8>> {
        self.0.serialize_changes()
    }

    #[inline]
    fn db(&self) -> Database {
        self.0.db()
    }

    fn remove(self) -> Result<()> {
        self.0.remove()
    }
}

impl<V> GenericStoredVec<V::I, V::T> for EagerVec<V>
where
    V: TypedVec + GenericStoredVec<V::I, V::T>,
    V::I: VecIndex,
    V::T: VecValue,
{
    #[inline]
    fn unchecked_read_at(&self, index: usize, reader: &Reader) -> Result<V::T> {
        self.0.unchecked_read_at(index, reader)
    }

    #[inline]
    fn read_value_from_bytes(&self, bytes: &[u8]) -> Result<V::T> {
        self.0.read_value_from_bytes(bytes)
    }

    #[inline]
    fn value_to_bytes(&self, value: &V::T) -> Vec<u8> {
        self.0.value_to_bytes(value)
    }

    #[inline]
    fn pushed(&self) -> &[V::T] {
        self.0.pushed()
    }
    #[inline]
    fn mut_pushed(&mut self) -> &mut Vec<V::T> {
        self.0.mut_pushed()
    }
    #[inline]
    fn prev_pushed(&self) -> &[V::T] {
        self.0.prev_pushed()
    }
    #[inline]
    fn mut_prev_pushed(&mut self) -> &mut Vec<V::T> {
        self.0.mut_prev_pushed()
    }

    #[inline]
    #[doc(hidden)]
    fn update_stored_len(&self, val: usize) {
        self.0.update_stored_len(val);
    }
    #[inline]
    fn prev_stored_len(&self) -> usize {
        self.0.prev_stored_len()
    }
    #[inline]
    fn mut_prev_stored_len(&mut self) -> &mut usize {
        self.0.mut_prev_stored_len()
    }

    #[inline]
    fn truncate_if_needed(&mut self, index: V::I) -> Result<()> {
        self.0.truncate_if_needed(index)
    }

    #[inline]
    fn reset(&mut self) -> Result<()> {
        self.0.reset()
    }
}

impl<'a, V> IntoIterator for &'a EagerVec<V>
where
    V: TypedVec,
    &'a V: IntoIterator<Item = V::T>,
    V::I: VecIndex,
    V::T: VecValue,
{
    type Item = V::T;
    type IntoIter = <&'a V as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        (&self.0).into_iter()
    }
}

impl<V> IterableVec<V::I, V::T> for EagerVec<V>
where
    V: TypedStoredVec + IterableVec<V::I, V::T>,
    V::I: VecIndex,
    V::T: VecValue,
{
    fn iter(&self) -> BoxedVecIterator<'_, V::I, V::T> {
        self.0.iter()
    }
}

impl<V> TypedVec for EagerVec<V>
where
    V: TypedStoredVec,
{
    type I = V::I;
    type T = V::T;
}
