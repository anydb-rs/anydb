use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    f32,
    fmt::Debug,
    iter::Sum,
    ops::{Add, Div, Mul, Sub},
    path::PathBuf,
};

use rawdb::{Database, Reader, Region};

mod checked_sub;
mod saturating_add;

pub use checked_sub::*;
pub use saturating_add::*;

use crate::{
    AnyStoredVec, AnyVec, BoxedVecIterator, CollectableVec, Compressable, Exit, Format,
    GenericStoredVec, IterableVec, Result, StoredVec, StoredVecIterator, TypedVec,
    TypedVecIterator, VecIndex, VecValue, Version,
    variants::{Header, ImportOptions},
};

/// Stored vector with eager computation methods for deriving values from other vectors.
///
/// Wraps a StoredVec and provides various computation methods (transform, arithmetic operations,
/// moving averages, etc.) to eagerly compute and persist derived data. Results are stored on disk
/// and incrementally updated when source data changes.
#[derive(Debug, Clone)]
pub struct EagerVec<I, T>(StoredVec<I, T>);

impl<I, T> EagerVec<I, T>
where
    I: VecIndex,
    T: Compressable,
{
    pub fn forced_import_compressed(db: &Database, name: &str, version: Version) -> Result<Self> {
        Self::forced_import_compressed_with((db, name, version).into())
    }

    pub fn forced_import_compressed_with(options: ImportOptions) -> Result<Self> {
        Ok(Self(StoredVec::forced_import_with(
            options,
            Format::Compressed,
        )?))
    }

    pub fn forced_import_raw(db: &Database, name: &str, version: Version) -> Result<Self> {
        Self::forced_import_raw_with((db, name, version).into())
    }

    pub fn forced_import_raw_with(options: ImportOptions) -> Result<Self> {
        Ok(Self(StoredVec::forced_import_with(options, Format::Raw)?))
    }

    pub fn forced_import(
        db: &Database,
        name: &str,
        version: Version,
        format: Format,
    ) -> Result<Self> {
        Self::forced_import_with((db, name, version).into(), format)
    }

    pub fn forced_import_with(options: ImportOptions, format: Format) -> Result<Self> {
        Ok(Self(StoredVec::forced_import_with(options, format)?))
    }

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
            let len_before = self.len();
            f(self)?;

            // Check if we made progress
            if self.len() == len_before {
                break; // No progress, computation is complete
            }

            // Flush between batches
            self.safe_flush(exit)?;
        }

        Ok(())
    }

    pub fn compute_to<F>(
        &mut self,
        max_from: I,
        to: usize,
        version: Version,
        mut t: F,
        exit: &Exit,
    ) -> Result<()>
    where
        F: FnMut(I) -> (I, T),
    {
        self.validate_computed_version_or_reset(Version::ZERO + self.inner_version() + version)?;

        self.repeat_until_complete(exit, |this| {
            let from = this.len().max(max_from.to_usize());
            if from >= to {
                return Ok(());
            }

            for i in from..to {
                let (idx, val) = t(I::from(i));
                this.forced_push(idx, val)?;

                if this.batch_limit_reached() {
                    break;
                }
            }

            Ok(())
        })
    }

    pub fn compute_range<A, F>(
        &mut self,
        max_from: I,
        other: &impl IterableVec<I, A>,
        t: F,
        exit: &Exit,
    ) -> Result<()>
    where
        A: VecValue,
        F: FnMut(I) -> (I, T),
    {
        self.compute_to(max_from, other.len(), other.version(), t, exit)
    }

    pub fn compute_from_index<T2>(
        &mut self,
        max_from: I,
        other: &impl IterableVec<I, T2>,
        exit: &Exit,
    ) -> Result<()>
    where
        T: From<I>,
        T2: VecValue,
    {
        self.compute_to(
            max_from,
            other.len(),
            other.version(),
            |i| (i, T::from(i)),
            exit,
        )
    }

    pub fn compute_transform<A, B, F>(
        &mut self,
        max_from: A,
        other: &impl IterableVec<A, B>,
        mut t: F,
        exit: &Exit,
    ) -> Result<()>
    where
        A: VecIndex,
        B: VecValue,
        F: FnMut((A, B, &Self)) -> (I, T),
    {
        self.validate_computed_version_or_reset(
            Version::ZERO + self.inner_version() + other.version(),
        )?;

        self.repeat_until_complete(exit, |this| {
            let skip = this.len().max(max_from.to_usize());

            for (a, b) in other.iter().enumerate().skip(skip) {
                let (i, v) = t((A::from(a), b, this));
                this.forced_push(i, v)?;

                if this.batch_limit_reached() {
                    break;
                }
            }

            Ok(())
        })
    }

    pub fn compute_transform2<A, B, C, F>(
        &mut self,
        max_from: A,
        other1: &impl IterableVec<A, B>,
        other2: &impl IterableVec<A, C>,
        mut t: F,
        exit: &Exit,
    ) -> Result<()>
    where
        A: VecIndex,
        B: VecValue,
        C: VecValue,
        F: FnMut((A, B, C, &Self)) -> (I, T),
    {
        self.validate_computed_version_or_reset(
            Version::ZERO + self.inner_version() + other1.version() + other2.version(),
        )?;

        self.repeat_until_complete(exit, |this| {
            let skip = this.len().max(max_from.to_usize());
            let mut iter2 = other2.iter().skip(skip);

            for (a, b) in other1.iter().enumerate().skip(skip) {
                let (i, v) = t((A::from(a), b, iter2.next().unwrap(), this));
                this.forced_push(i, v)?;

                if this.batch_limit_reached() {
                    break;
                }
            }

            Ok(())
        })
    }

    pub fn compute_transform3<A, B, C, D, F>(
        &mut self,
        max_from: A,
        other1: &impl IterableVec<A, B>,
        other2: &impl IterableVec<A, C>,
        other3: &impl IterableVec<A, D>,
        mut t: F,
        exit: &Exit,
    ) -> Result<()>
    where
        A: VecIndex,
        B: VecValue,
        C: VecValue,
        D: VecValue,
        F: FnMut((A, B, C, D, &Self)) -> (I, T),
    {
        self.validate_computed_version_or_reset(
            Version::ZERO
                + self.inner_version()
                + other1.version()
                + other2.version()
                + other3.version(),
        )?;

        self.repeat_until_complete(exit, |this| {
            let skip = this.len().max(max_from.to_usize());
            let mut iter2 = other2.iter().skip(skip);
            let mut iter3 = other3.iter().skip(skip);

            for (a, b) in other1.iter().enumerate().skip(skip) {
                let (i, v) = t((
                    A::from(a),
                    b,
                    iter2.next().unwrap(),
                    iter3.next().unwrap(),
                    this,
                ));
                this.forced_push(i, v)?;

                if this.batch_limit_reached() {
                    break;
                }
            }

            Ok(())
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn compute_transform4<A, B, C, D, E, F>(
        &mut self,
        max_from: A,
        other1: &impl IterableVec<A, B>,
        other2: &impl IterableVec<A, C>,
        other3: &impl IterableVec<A, D>,
        other4: &impl IterableVec<A, E>,
        mut t: F,
        exit: &Exit,
    ) -> Result<()>
    where
        A: VecIndex,
        B: VecValue,
        C: VecValue,
        D: VecValue,
        E: VecValue,
        F: FnMut((A, B, C, D, E, &Self)) -> (I, T),
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
            let skip = this.len().max(max_from.to_usize());
            let mut iter2 = other2.iter().skip(skip);
            let mut iter3 = other3.iter().skip(skip);
            let mut iter4 = other4.iter().skip(skip);

            for (a, b) in other1.iter().enumerate().skip(skip) {
                let (i, v) = t((
                    A::from(a),
                    b,
                    iter2.next().unwrap(),
                    iter3.next().unwrap(),
                    iter4.next().unwrap(),
                    this,
                ));
                this.forced_push(i, v)?;

                if this.batch_limit_reached() {
                    break;
                }
            }

            Ok(())
        })
    }

    pub fn compute_add(
        &mut self,
        max_from: I,
        added: &impl IterableVec<I, T>,
        adder: &impl IterableVec<I, T>,
        exit: &Exit,
    ) -> Result<()>
    where
        T: Add<Output = T>,
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
        max_from: I,
        subtracted: &impl IterableVec<I, T>,
        subtracter: &impl IterableVec<I, T>,
        exit: &Exit,
    ) -> Result<()>
    where
        T: CheckedSub,
    {
        self.compute_transform2(
            max_from,
            subtracted,
            subtracter,
            |(i, v1, v2, ..)| (i, (v1.checked_sub(v2).unwrap())),
            exit,
        )
    }

    pub fn compute_multiply<T2, T3>(
        &mut self,
        max_from: I,
        multiplied: &impl IterableVec<I, T2>,
        multiplier: &impl IterableVec<I, T3>,
        exit: &Exit,
    ) -> Result<()>
    where
        T2: VecValue,
        T3: VecValue,
        T: From<T2> + Mul<T3, Output = T>,
    {
        self.compute_transform2(
            max_from,
            multiplied,
            multiplier,
            |(i, v1, v2, ..)| (i, T::from(v1) * v2),
            exit,
        )
    }

    pub fn compute_divide<T2, T3>(
        &mut self,
        max_from: I,
        divided: &impl IterableVec<I, T2>,
        divider: &impl IterableVec<I, T3>,
        exit: &Exit,
    ) -> Result<()>
    where
        T2: VecValue,
        T3: VecValue,
        T: From<T2> + Mul<usize, Output = T> + Div<T3, Output = T> + CheckedSub<usize>,
    {
        self.compute_transform2(
            max_from,
            divided,
            divider,
            |(i, v1, v2, ..)| (i, T::from(v1) / v2),
            exit,
        )
    }

    fn compute_all_time_extreme<T2, F>(
        &mut self,
        max_from: I,
        source: &impl IterableVec<I, T2>,
        exit: &Exit,
        compare: F,
        exclude_default: bool,
    ) -> Result<()>
    where
        T: From<T2> + Ord + Default,
        T2: VecValue,
        F: Fn(T, T) -> T + Copy,
    {
        let mut prev = None;
        self.compute_transform(
            max_from,
            source,
            |(i, v, this)| {
                if prev.is_none() {
                    let i = i.to_usize();
                    prev.replace(if i > 0 {
                        this.into_iter().nth(i - 1).unwrap()
                    } else {
                        T::from(source.iter().next().unwrap())
                    });
                }
                let v = T::from(v);
                let extreme = compare(prev.unwrap(), v);

                prev.replace(if !exclude_default || extreme != T::default() {
                    extreme
                } else {
                    // Reverse the comparison if excluding default
                    if extreme == prev.unwrap() {
                        v
                    } else {
                        prev.unwrap()
                    }
                });
                (i, extreme)
            },
            exit,
        )
    }

    pub fn compute_all_time_high<T2>(
        &mut self,
        max_from: I,
        source: &impl IterableVec<I, T2>,
        exit: &Exit,
    ) -> Result<()>
    where
        T: From<T2> + Ord + Default,
        T2: VecValue,
    {
        self.compute_all_time_extreme(max_from, source, exit, |prev, v| prev.max(v), false)
    }

    pub fn compute_all_time_low<T2>(
        &mut self,
        max_from: I,
        source: &impl IterableVec<I, T2>,
        exit: &Exit,
    ) -> Result<()>
    where
        T: From<T2> + Ord + Default,
        T2: VecValue,
    {
        self.compute_all_time_low_(max_from, source, exit, false)
    }

    pub fn compute_all_time_low_<T2>(
        &mut self,
        max_from: I,
        source: &impl IterableVec<I, T2>,
        exit: &Exit,
        exclude_default: bool,
    ) -> Result<()>
    where
        T: From<T2> + Ord + Default,
        T2: VecValue,
    {
        self.compute_all_time_extreme(
            max_from,
            source,
            exit,
            |prev, v| prev.min(v),
            exclude_default,
        )
    }

    pub fn compute_percentage<T2, T3>(
        &mut self,
        max_from: I,
        divided: &impl IterableVec<I, T2>,
        divider: &impl IterableVec<I, T3>,
        exit: &Exit,
    ) -> Result<()>
    where
        T2: VecValue,
        T3: VecValue,
        T: From<T2> + From<T3> + Mul<usize, Output = T> + Div<T, Output = T> + CheckedSub<usize>,
    {
        self.compute_percentage_(max_from, divided, divider, exit, false)
    }

    pub fn compute_percentage_difference<T2, T3>(
        &mut self,
        max_from: I,
        divided: &impl IterableVec<I, T2>,
        divider: &impl IterableVec<I, T3>,
        exit: &Exit,
    ) -> Result<()>
    where
        T2: VecValue,
        T3: VecValue,
        T: From<T2> + From<T3> + Mul<usize, Output = T> + Div<T, Output = T> + CheckedSub<usize>,
    {
        self.compute_percentage_(max_from, divided, divider, exit, true)
    }

    pub fn compute_percentage_<T2, T3>(
        &mut self,
        max_from: I,
        divided: &impl IterableVec<I, T2>,
        divider: &impl IterableVec<I, T3>,
        exit: &Exit,
        as_difference: bool,
    ) -> Result<()>
    where
        T2: VecValue,
        T3: VecValue,
        T: From<T2> + From<T3> + Mul<usize, Output = T> + Div<T, Output = T> + CheckedSub<usize>,
    {
        let multiplier = 100;
        self.compute_transform2(
            max_from,
            divided,
            divider,
            |(i, v1, v2, ..)| {
                let divided = T::from(v1);
                let divider = T::from(v2);
                let v = divided * multiplier;
                let mut v = v / divider;
                if as_difference {
                    v = v.checked_sub(multiplier).unwrap();
                }
                (i, v)
            },
            exit,
        )
    }

    pub fn compute_coarser(
        &mut self,
        max_from: T,
        other: &impl IterableVec<T, I>,
        exit: &Exit,
    ) -> Result<()>
    where
        I: VecValue + VecIndex,
        T: VecIndex,
    {
        self.validate_computed_version_or_reset(
            Version::ZERO + self.inner_version() + other.version(),
        )?;

        self.repeat_until_complete(exit, |this| {
            let skip = max_from
                .to_usize()
                .min(this.into_iter().last().map_or(0_usize, |v| v.to_usize()));

            let mut prev_i = None;
            for (v, i) in other.iter().enumerate().skip(skip) {
                let v = T::from(v);
                if prev_i.is_some_and(|prev_i| prev_i == i) {
                    continue;
                }
                if this
                    .get_pushed_or_read_once(i)?
                    .is_none_or(|old_v| old_v > v)
                {
                    this.forced_push(i, v)?;
                }
                prev_i.replace(i);

                if this.batch_limit_reached() {
                    break;
                }
            }

            Ok(())
        })
    }

    pub fn compute_count_from_indexes<T2, T3>(
        &mut self,
        max_from: I,
        first_indexes: &impl IterableVec<I, T2>,
        other_to_else: &impl IterableVec<T2, T3>,
        exit: &Exit,
    ) -> Result<()>
    where
        T: From<T2>,
        T2: VecValue
            + VecIndex
            + Copy
            + Add<usize, Output = T2>
            + CheckedSub<T2>
            + TryInto<T>
            + Default,
        <T2 as TryInto<T>>::Error: core::error::Error + 'static,
        T3: VecValue,
    {
        self.compute_filtered_count_from_indexes(
            max_from,
            first_indexes,
            other_to_else,
            |_| true,
            exit,
        )
    }

    pub fn compute_filtered_count_from_indexes<T2, T3>(
        &mut self,
        max_from: I,
        first_indexes: &impl IterableVec<I, T2>,
        other_to_else: &impl IterableVec<T2, T3>,
        mut filter: impl FnMut(T2) -> bool,
        exit: &Exit,
    ) -> Result<()>
    where
        T: From<T2>,
        T2: VecValue
            + VecIndex
            + Copy
            + Add<usize, Output = T2>
            + CheckedSub<T2>
            + TryInto<T>
            + Default,
        T3: VecValue,
        <T2 as TryInto<T>>::Error: core::error::Error + 'static,
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
                let count = range.into_iter().filter(|i| filter(T2::from(*i))).count();
                this.forced_push_at(i, T::from(T2::from(count)))?;

                if this.batch_limit_reached() {
                    break;
                }
            }

            Ok(())
        })
    }

    pub fn compute_is_first_ordered<A>(
        &mut self,
        max_from: I,
        self_to_other: &impl IterableVec<I, A>,
        other_to_self: &impl IterableVec<A, I>,
        exit: &Exit,
    ) -> Result<()>
    where
        I: VecValue,
        T: From<bool>,
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
                this.forced_push_at(
                    i,
                    T::from(other_to_self_iter.get_unwrap(other).to_usize() == i),
                )?;

                if this.batch_limit_reached() {
                    break;
                }
            }

            Ok(())
        })
    }

    fn compute_monotonic_window<T2, F>(
        &mut self,
        max_from: I,
        source: &impl IterableVec<I, T2>,
        window: usize,
        exit: &Exit,
        should_pop: F,
    ) -> Result<()>
    where
        T2: VecValue + Ord,
        T: From<T2>,
        F: Fn(&T2, &T2) -> bool,
    {
        self.validate_computed_version_or_reset(
            Version::ZERO + self.inner_version() + source.version(),
        )?;

        self.repeat_until_complete(exit, |this| {
            let skip = max_from.to_usize().min(this.len());
            let mut deque: VecDeque<(usize, T2)> = VecDeque::new();

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
                this.forced_push_at(i, T::from(v))?;

                if this.batch_limit_reached() {
                    break;
                }
            }

            Ok(())
        })
    }

    pub fn compute_max<T2>(
        &mut self,
        max_from: I,
        source: &impl IterableVec<I, T2>,
        window: usize,
        exit: &Exit,
    ) -> Result<()>
    where
        T2: VecValue + Ord,
        T: From<T2>,
    {
        self.compute_monotonic_window(max_from, source, window, exit, |v, value| v < value)
    }

    pub fn compute_min<T2>(
        &mut self,
        max_from: I,
        source: &impl IterableVec<I, T2>,
        window: usize,
        exit: &Exit,
    ) -> Result<()>
    where
        T2: VecValue + Ord,
        T: From<T2>,
    {
        self.compute_monotonic_window(max_from, source, window, exit, |v, value| v > value)
    }

    pub fn compute_sum<T2>(
        &mut self,
        max_from: I,
        source: &impl IterableVec<I, T2>,
        window: usize,
        exit: &Exit,
    ) -> Result<()>
    where
        T: Add<T, Output = T> + From<T2> + Default + CheckedSub,
        T2: VecValue,
    {
        self.validate_computed_version_or_reset(
            Version::ONE + self.inner_version() + source.version(),
        )?;

        self.repeat_until_complete(exit, |this| {
            let skip = max_from.to_usize().min(this.len());
            let mut prev = skip
                .checked_sub(1)
                .and_then(|prev_i| this.into_iter().get(I::from(prev_i)))
                .or(Some(T::default()));

            // Initialize buffer for sliding window sum
            let mut window_values = if window < usize::MAX {
                VecDeque::with_capacity(window + 1)
            } else {
                VecDeque::new()
            };

            if skip > 0 {
                let start = skip.saturating_sub(window);
                source.iter().skip(start).take(skip - start).for_each(|v| {
                    window_values.push_back(T::from(v));
                });
            }

            for (i, value) in source.iter().enumerate().skip(skip) {
                let value = T::from(value);

                let processed_values_count = i.to_usize() + 1;
                let len = (processed_values_count).min(window);

                let sum = if processed_values_count > len {
                    let prev_sum = prev.unwrap();
                    // Pop the oldest value from our window buffer
                    let value_to_subtract = window_values.pop_front().unwrap();
                    prev_sum.checked_sub(value_to_subtract).unwrap() + value
                } else {
                    prev.unwrap() + value
                };

                // Add current value to window buffer
                window_values.push_back(value);
                if window_values.len() > window {
                    window_values.pop_front();
                }

                prev.replace(sum);
                this.forced_push_at(i, sum)?;

                if this.batch_limit_reached() {
                    break;
                }
            }

            Ok(())
        })
    }

    pub fn compute_sum_from_indexes<T2, T3>(
        &mut self,
        max_from: I,
        first_indexes: &impl IterableVec<I, T2>,
        indexes_count: &impl IterableVec<I, T3>,
        source: &impl IterableVec<T2, T>,
        exit: &Exit,
    ) -> Result<()>
    where
        T: From<usize> + SaturatingAdd,
        T2: VecIndex + VecValue,
        T3: VecValue,
        usize: From<T3>,
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

    pub fn compute_filtered_sum_from_indexes<T2, T3>(
        &mut self,
        max_from: I,
        first_indexes: &impl IterableVec<I, T2>,
        indexes_count: &impl IterableVec<I, T3>,
        source: &impl IterableVec<T2, T>,
        mut filter: impl FnMut(&T) -> bool,
        exit: &Exit,
    ) -> Result<()>
    where
        T: From<usize> + SaturatingAdd,
        T2: VecIndex + VecValue,
        T3: VecValue,
        usize: From<T3>,
    {
        self.validate_computed_version_or_reset(
            Version::ZERO
                + self.inner_version()
                + first_indexes.version()
                + indexes_count.version(),
        )?;

        self.repeat_until_complete(exit, |this| {
            let skip = max_from.to_usize().min(this.len());
            let mut source_iter = source.iter();

            // Set position once - source indices are sequential
            if let Some(starting_first_index) = first_indexes.iter().get(max_from) {
                source_iter.set_position(starting_first_index);
            }

            for (i, count) in indexes_count.iter().enumerate().skip(skip) {
                let count = usize::from(count);
                // Sequential read - iterator advances automatically
                let sum = (&mut source_iter)
                    .take(count)
                    .filter(|v| filter(v))
                    .fold(T::from(0_usize), |acc, val| acc.saturating_add(val));
                this.forced_push_at(i, sum)?;

                if this.batch_limit_reached() {
                    break;
                }
            }

            Ok(())
        })
    }

    fn compute_aggregate_of_others<V, F>(
        &mut self,
        max_from: I,
        others: &[&V],
        exit: &Exit,
        aggregate: F,
    ) -> Result<()>
    where
        V: IterableVec<I, T>,
        F: Fn(Box<dyn Iterator<Item = T> + '_>) -> T,
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
                this.forced_push_at(i, result)?;

                if this.batch_limit_reached() {
                    break;
                }
            }

            Ok(())
        })
    }

    pub fn compute_sum_of_others(
        &mut self,
        max_from: I,
        others: &[&impl IterableVec<I, T>],
        exit: &Exit,
    ) -> Result<()>
    where
        T: Add<T, Output = T>,
    {
        self.compute_aggregate_of_others(max_from, others, exit, |values| {
            values.reduce(|sum, v| sum + v).unwrap()
        })
    }

    pub fn compute_min_of_others(
        &mut self,
        max_from: I,
        others: &[&impl IterableVec<I, T>],
        exit: &Exit,
    ) -> Result<()>
    where
        T: Add<T, Output = T> + Ord,
    {
        self.compute_aggregate_of_others(max_from, others, exit, |values| values.min().unwrap())
    }

    pub fn compute_max_of_others(
        &mut self,
        max_from: I,
        others: &[&impl IterableVec<I, T>],
        exit: &Exit,
    ) -> Result<()>
    where
        T: Add<T, Output = T> + Ord,
    {
        self.compute_aggregate_of_others(max_from, others, exit, |values| values.max().unwrap())
    }

    pub fn compute_sma<T2>(
        &mut self,
        max_from: I,
        source: &impl IterableVec<I, T2>,
        sma: usize,
        exit: &Exit,
    ) -> Result<()>
    where
        T: Add<T, Output = T> + From<T2> + Div<usize, Output = T> + From<f32>,
        T2: VecValue,
        f32: From<T> + From<T2>,
    {
        self.compute_sma_(max_from, source, sma, exit, None)
    }

    pub fn compute_sma_<T2>(
        &mut self,
        max_from: I,
        source: &impl IterableVec<I, T2>,
        sma: usize,
        exit: &Exit,
        min_i: Option<I>,
    ) -> Result<()>
    where
        T: Add<T, Output = T> + From<T2> + Div<usize, Output = T> + From<f32>,
        T2: VecValue,
        f32: From<T> + From<T2>,
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
                        this.into_iter().get(I::from(prev_i))
                    } else {
                        Some(T::from(0.0))
                    }
                })
                .or(Some(T::from(0.0)));

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

                    let sma_result = T::from(if processed_values_count > sma {
                        let prev_sum = f32::from(prev.unwrap()) * len as f32;
                        // Pop the oldest value from our window buffer
                        let value_to_subtract = window_values.pop_front().unwrap();
                        (prev_sum - value_to_subtract + value) / len as f32
                    } else {
                        (f32::from(prev.unwrap()) * (len - 1) as f32 + value) / len as f32
                    });

                    // Add current value to window buffer
                    window_values.push_back(value);
                    if window_values.len() > sma {
                        window_values.pop_front();
                    }

                    prev.replace(sma_result);
                    this.forced_push_at(i, sma_result)?;
                } else {
                    this.forced_push_at(i, T::from(f32::NAN))?;
                }

                if this.batch_limit_reached() {
                    break;
                }
            }

            Ok(())
        })
    }

    pub fn compute_ema<T2>(
        &mut self,
        max_from: I,
        source: &impl CollectableVec<I, T2>,
        ema: usize,
        exit: &Exit,
    ) -> Result<()>
    where
        T: From<T2> + From<f32>,
        T2: VecValue + Div<usize, Output = T2> + Sum,
        f32: From<T2> + From<T>,
    {
        self.compute_ema_(max_from, source, ema, exit, None)
    }

    pub fn compute_ema_<T2>(
        &mut self,
        max_from: I,
        source: &impl CollectableVec<I, T2>,
        ema: usize,
        exit: &Exit,
        min_i: Option<I>,
    ) -> Result<()>
    where
        T: From<T2> + From<f32>,
        T2: VecValue + Div<usize, Output = T2> + Sum,
        f32: From<T2> + From<T>,
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
                        this.into_iter().get(I::from(prev_i))
                    } else {
                        Some(T::from(0.0))
                    }
                })
                .or(Some(T::from(0.0)));

            for (index, value) in source.iter().enumerate().skip(skip) {
                let value = value;

                if min_i.is_none() || min_i.is_some_and(|min_i| min_i <= index) {
                    let processed_values_count = index - min_prev_i + 1;

                    let value = f32::from(value);

                    let ema = if processed_values_count > ema {
                        let prev = f32::from(prev.unwrap());
                        let prev = if prev.is_nan() { 0.0 } else { prev };
                        T::from((value * k) + (prev * _1_minus_k))
                    } else {
                        let len = (processed_values_count).min(ema);
                        let prev = f32::from(prev.unwrap());
                        T::from((prev * (len - 1) as f32 + value) / len as f32)
                    };

                    prev.replace(ema);
                    this.forced_push_at(index, ema)?;
                } else {
                    this.forced_push_at(index, T::from(f32::NAN))?;
                }

                if this.batch_limit_reached() {
                    break;
                }
            }

            Ok(())
        })
    }

    fn compute_with_lookback<T2, F>(
        &mut self,
        max_from: I,
        source: &impl IterableVec<I, T2>,
        lookback_len: usize,
        exit: &Exit,
        transform: F,
    ) -> Result<()>
    where
        I: CheckedSub,
        T2: Compressable + Default,
        F: Fn(usize, T2, T2) -> T,
    {
        self.validate_computed_version_or_reset(
            Version::ZERO + self.inner_version() + source.version(),
        )?;

        self.repeat_until_complete(exit, |this| {
            let skip = max_from.to_usize().min(this.len());
            let mut lookback = source.create_lookback(skip, lookback_len, 0);

            for (i, current) in source.iter().enumerate().skip(skip) {
                let previous = lookback.get_and_push(i, current, T2::default());
                let result = transform(i, current, previous);
                this.forced_push_at(i, result)?;

                if this.batch_limit_reached() {
                    break;
                }
            }

            Ok(())
        })
    }

    pub fn compute_previous_value<T2>(
        &mut self,
        max_from: I,
        source: &impl IterableVec<I, T2>,
        len: usize,
        exit: &Exit,
    ) -> Result<()>
    where
        I: CheckedSub,
        T2: Compressable + Default,
        f32: From<T2>,
        T: From<f32>,
    {
        self.compute_with_lookback(max_from, source, len, exit, |i, _, previous| {
            // If there's no previous value (i < len), return NaN
            if i < len {
                T::from(f32::NAN)
            } else {
                T::from(f32::from(previous))
            }
        })
    }

    pub fn compute_change(
        &mut self,
        max_from: I,
        source: &impl IterableVec<I, T>,
        len: usize,
        exit: &Exit,
    ) -> Result<()>
    where
        I: CheckedSub,
        T: CheckedSub + Default + Compressable,
    {
        self.compute_with_lookback(max_from, source, len, exit, |i, current, previous| {
            // If there's no previous value (i < len), return 0 (no change)
            if i < len {
                T::default()
            } else {
                current.checked_sub(previous).unwrap()
            }
        })
    }

    pub fn compute_percentage_change<T2>(
        &mut self,
        max_from: I,
        source: &impl IterableVec<I, T2>,
        len: usize,
        exit: &Exit,
    ) -> Result<()>
    where
        I: CheckedSub,
        T2: Compressable + Default,
        f32: From<T2>,
        T: From<f32>,
    {
        self.compute_with_lookback(max_from, source, len, exit, |i, current, previous| {
            // If there's no previous value (i < len), return NaN
            if i < len {
                T::from(f32::NAN)
            } else {
                let current_f32 = f32::from(current);
                let previous_f32 = f32::from(previous);
                T::from(((current_f32 / previous_f32) - 1.0) * 100.0)
            }
        })
    }

    pub fn compute_cagr<T2>(
        &mut self,
        max_from: I,
        percentage_returns: &impl IterableVec<I, T2>,
        days: usize,
        exit: &Exit,
    ) -> Result<()>
    where
        I: CheckedSub,
        T2: VecValue + Default,
        f32: From<T2>,
        T: From<f32>,
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
                (i, T::from(cagr))
            },
            exit,
        )
    }

    pub fn compute_zscore<T2, T3, T4>(
        &mut self,
        max_from: I,
        source: &impl IterableVec<I, T2>,
        sma: &impl IterableVec<I, T3>,
        sd: &impl IterableVec<I, T4>,
        exit: &Exit,
    ) -> Result<()>
    where
        T: From<f32>,
        T2: VecValue + Sub<T3, Output = T2> + Div<T4, Output = T>,
        T3: VecValue,
        T4: VecValue,
        T2: VecValue,
        f32: From<T2> + From<T3> + From<T4>,
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

impl<I, T> AnyVec for EagerVec<I, T>
where
    I: VecIndex,
    T: Compressable,
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
        I::to_string()
    }

    #[inline]
    fn value_type_to_size_of(&self) -> usize {
        size_of::<T>()
    }

    #[inline]
    fn region_names(&self) -> Vec<String> {
        self.0.region_names()
    }
}

impl<I, T> AnyStoredVec for EagerVec<I, T>
where
    I: VecIndex,
    T: Compressable,
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
}

impl<I, T> GenericStoredVec<I, T> for EagerVec<I, T>
where
    I: VecIndex,
    T: Compressable,
{
    #[inline]
    fn unchecked_read_at(&self, index: usize, reader: &Reader) -> Result<T> {
        self.0.unchecked_read_at(index, reader)
    }

    #[inline]
    fn pushed(&self) -> &[T] {
        self.0.pushed()
    }
    #[inline]
    fn mut_pushed(&mut self) -> &mut Vec<T> {
        self.0.mut_pushed()
    }
    #[inline]
    fn prev_pushed(&self) -> &[T] {
        self.0.prev_pushed()
    }
    #[inline]
    fn mut_prev_pushed(&mut self) -> &mut Vec<T> {
        self.0.mut_prev_pushed()
    }

    #[inline]
    fn holes(&self) -> &BTreeSet<usize> {
        self.0.holes()
    }
    #[inline]
    fn mut_holes(&mut self) -> &mut BTreeSet<usize> {
        self.0.mut_holes()
    }
    #[inline]
    fn prev_holes(&self) -> &BTreeSet<usize> {
        self.0.prev_holes()
    }
    #[inline]
    fn mut_prev_holes(&mut self) -> &mut BTreeSet<usize> {
        self.0.mut_prev_holes()
    }

    #[inline]
    fn updated(&self) -> &BTreeMap<usize, T> {
        self.0.updated()
    }
    #[inline]
    fn mut_updated(&mut self) -> &mut BTreeMap<usize, T> {
        self.0.mut_updated()
    }
    #[inline]
    fn prev_updated(&self) -> &BTreeMap<usize, T> {
        self.0.prev_updated()
    }
    #[inline]
    fn mut_prev_updated(&mut self) -> &mut BTreeMap<usize, T> {
        self.0.mut_prev_updated()
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
    fn truncate_if_needed(&mut self, index: I) -> Result<()> {
        self.0.truncate_if_needed(index)
    }

    #[inline]
    fn reset(&mut self) -> Result<()> {
        self.0.reset()
    }
}

impl<'a, I, T> IntoIterator for &'a EagerVec<I, T>
where
    I: VecIndex,
    T: Compressable,
{
    type Item = T;
    type IntoIter = StoredVecIterator<'a, I, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<I, T> IterableVec<I, T> for EagerVec<I, T>
where
    I: VecIndex,
    T: Compressable,
{
    fn iter(&self) -> BoxedVecIterator<'_, I, T>
    where
        I: VecIndex,
        T: VecValue,
    {
        Box::new(self.0.into_iter())
    }
}

impl<I, T> TypedVec for EagerVec<I, T>
where
    I: VecIndex,
    T: Compressable,
{
    type I = I;
    type T = T;
}
