use std::ops::{Add, AddAssign};

use crate::{AnyVec, Exit, GenericStoredVec, IterableVec, Result, StoredVec, VecIndex, VecValue};

use super::EagerVec;

impl<V> EagerVec<V>
where
    V: StoredVec,
{
    /// Compute cumulative sum from a source vec.
    ///
    /// Each value in the result is the sum of all values from the source up to
    /// and including that index.
    pub fn compute_cumulative<S>(
        &mut self,
        max_from: V::I,
        source: &impl IterableVec<V::I, S>,
        exit: &Exit,
    ) -> Result<()>
    where
        S: VecValue + Into<V::T>,
        V::T: From<usize> + AddAssign + Copy,
    {
        self.validate_computed_version_or_reset(source.version())?;

        self.truncate_if_needed(max_from)?;

        self.repeat_until_complete(exit, |this| {
            let skip = this.len();

            let mut cumulative_val = if skip > 0 {
                this.read_unwrap_once(V::I::from(skip - 1))
            } else {
                V::T::from(0_usize)
            };

            for (i, v) in source.iter().enumerate().skip(skip) {
                cumulative_val += v.into();
                this.checked_push_at(i, cumulative_val)?;

                if this.batch_limit_reached() {
                    break;
                }
            }

            Ok(())
        })
    }

    /// Compute cumulative sum from adding two source vecs element-wise.
    ///
    /// Each value in the result is the cumulative sum of `source1[i] + source2[i]`
    /// for all indices up to and including i.
    pub fn compute_cumulative_binary<S1, S2>(
        &mut self,
        max_from: V::I,
        source1: &impl IterableVec<V::I, S1>,
        source2: &impl IterableVec<V::I, S2>,
        exit: &Exit,
    ) -> Result<()>
    where
        S1: VecValue + Into<V::T>,
        S2: VecValue + Into<V::T>,
        V::T: From<usize> + AddAssign + Add<Output = V::T> + Copy,
    {
        self.compute_cumulative_transformed_binary(
            max_from,
            source1,
            source2,
            |v1: S1, v2: S2| v1.into() + v2.into(),
            exit,
        )
    }

    /// Compute cumulative sum from a custom binary transform of two source vecs.
    ///
    /// Each value in the result is the cumulative sum of `transform(source1[i], source2[i])`
    /// for all indices up to and including i.
    pub fn compute_cumulative_transformed_binary<S1, S2, F>(
        &mut self,
        max_from: V::I,
        source1: &impl IterableVec<V::I, S1>,
        source2: &impl IterableVec<V::I, S2>,
        mut transform: F,
        exit: &Exit,
    ) -> Result<()>
    where
        S1: VecValue,
        S2: VecValue,
        V::T: From<usize> + AddAssign + Copy,
        F: FnMut(S1, S2) -> V::T,
    {
        let combined_version = source1.version() + source2.version();
        self.validate_computed_version_or_reset(combined_version)?;

        self.truncate_if_needed(max_from)?;

        let target_len = source1.len().min(source2.len());

        self.repeat_until_complete(exit, |this| {
            let skip = this.len();

            if skip >= target_len {
                return Ok(());
            }

            let mut cumulative_val = if skip > 0 {
                this.read_unwrap_once(V::I::from(skip - 1))
            } else {
                V::T::from(0_usize)
            };

            let mut iter1 = source1.iter().skip(skip);
            let mut iter2 = source2.iter().skip(skip);

            for i in skip..target_len {
                let v1 = iter1.next().unwrap();
                let v2 = iter2.next().unwrap();
                cumulative_val += transform(v1, v2);
                this.checked_push_at(i, cumulative_val)?;

                if this.batch_limit_reached() {
                    break;
                }
            }

            Ok(())
        })
    }

    /// Compute cumulative count of values matching a predicate.
    ///
    /// Each value in the result is the count of values from the source up to
    /// and including that index where the predicate returns true.
    pub fn compute_cumulative_count<S, P>(
        &mut self,
        max_from: V::I,
        source: &impl IterableVec<V::I, S>,
        predicate: P,
        exit: &Exit,
    ) -> Result<()>
    where
        S: VecValue,
        V::T: From<usize> + AddAssign + Copy,
        P: Fn(&S) -> bool,
    {
        let mut count: Option<V::T> = None;
        self.compute_transform(
            max_from,
            source,
            |(i, v, this)| {
                if count.is_none() {
                    let idx = i.to_usize();
                    count = Some(if idx > 0 {
                        this.read_at_unwrap_once(idx - 1)
                    } else {
                        V::T::from(0_usize)
                    });
                }
                if predicate(&v) {
                    *count.as_mut().unwrap() += V::T::from(1_usize);
                }
                (i, count.unwrap())
            },
            exit,
        )
    }

    /// Compute rolling count of values matching a predicate within a window.
    pub fn compute_rolling_count<S, P>(
        &mut self,
        max_from: V::I,
        source: &impl IterableVec<V::I, S>,
        window_size: usize,
        predicate: P,
        exit: &Exit,
    ) -> Result<()>
    where
        S: VecValue,
        V::T: From<usize> + Copy,
        P: Fn(&S) -> bool,
    {
        self.validate_computed_version_or_reset(source.version())?;
        self.truncate_if_needed(max_from)?;

        self.repeat_until_complete(exit, |this| {
            let skip = this.len();
            let mut count = 0usize;
            let mut ring = vec![false; window_size];

            // Rebuild state from source
            if skip > 0 {
                let start = skip.saturating_sub(window_size);
                for (i, v) in source.iter().enumerate().skip(start).take(skip - start) {
                    let matches = predicate(&v);
                    let slot = i % window_size;
                    if ring[slot] { count -= 1; }
                    ring[slot] = matches;
                    if matches { count += 1; }
                }
            }

            for (i, v) in source.iter().enumerate().skip(skip) {
                let matches = predicate(&v);
                let slot = i % window_size;
                if ring[slot] { count -= 1; }
                ring[slot] = matches;
                if matches { count += 1; }

                this.checked_push_at(i, V::T::from(count))?;
                if this.batch_limit_reached() { break; }
            }
            Ok(())
        })
    }

    /// Compute cumulative count of values matching a predicate, starting from a specific index.
    ///
    /// Values before `from` will be 0. Starting at `from`, counts values where predicate is true.
    pub fn compute_cumulative_count_from<S, P>(
        &mut self,
        max_from: V::I,
        source: &impl IterableVec<V::I, S>,
        from: V::I,
        predicate: P,
        exit: &Exit,
    ) -> Result<()>
    where
        S: VecValue,
        V::T: From<usize> + AddAssign + Copy,
        P: Fn(&S) -> bool,
    {
        let from_usize = from.to_usize();
        let mut count: Option<V::T> = None;
        self.compute_transform(
            max_from,
            source,
            |(i, v, this)| {
                let idx = i.to_usize();
                if count.is_none() {
                    count = Some(if idx > 0 {
                        this.read_at_unwrap_once(idx - 1)
                    } else {
                        V::T::from(0_usize)
                    });
                }
                if idx >= from_usize && predicate(&v) {
                    *count.as_mut().unwrap() += V::T::from(1_usize);
                }
                (i, count.unwrap())
            },
            exit,
        )
    }
}
