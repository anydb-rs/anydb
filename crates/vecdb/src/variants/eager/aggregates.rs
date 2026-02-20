use std::ops::Add;

use crate::{
    AnyVec, Error, Exit, WritableVec, ReadableVec, Result, StoredVec, VecIndex, VecValue,
    Version,
};

use super::{CheckedSub, EagerVec, SaturatingAdd};

impl<V> EagerVec<V>
where
    V: StoredVec,
{
    fn compute_aggregate_of_others<O, F>(
        &mut self,
        max_from: V::I,
        others: &[&O],
        exit: &Exit,
        aggregate: F,
    ) -> Result<()>
    where
        O: ReadableVec<V::I, V::T>,
        F: Fn(&[Vec<V::T>], usize) -> V::T,
    {
        if others.is_empty() {
            return Err(Error::InvalidArgument(
                "others must have at least one element",
            ));
        }

        self.compute_init(others.iter().map(|v| v.version()).sum(), max_from, exit, |this| {
            let skip = this.len();
            let source_end = others.iter().map(|v| v.len()).min().unwrap();
            let end = this.batch_end(source_end);
            if skip >= end {
                return Ok(());
            }

            let batches: Vec<Vec<V::T>> = others
                .iter()
                .map(|v| v.collect_range_at(skip, end))
                .collect();

            for j in 0..(end - skip) {
                let i = skip + j;
                this.checked_push_at(i, aggregate(&batches, j))?;
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
        O: ReadableVec<V::I, V::T>,
        V::T: Add<V::T, Output = V::T>,
    {
        self.compute_aggregate_of_others(max_from, others, exit, |batches, j| {
            batches.iter().map(|b| b[j].clone()).reduce(|sum, v| sum + v).unwrap()
        })
    }

    pub fn compute_min_of_others<O>(
        &mut self,
        max_from: V::I,
        others: &[&O],
        exit: &Exit,
    ) -> Result<()>
    where
        O: ReadableVec<V::I, V::T>,
        V::T: Add<V::T, Output = V::T> + Ord,
    {
        self.compute_aggregate_of_others(max_from, others, exit, |batches, j| {
            batches.iter().map(|b| &b[j]).min().unwrap().clone()
        })
    }

    pub fn compute_max_of_others<O>(
        &mut self,
        max_from: V::I,
        others: &[&O],
        exit: &Exit,
    ) -> Result<()>
    where
        O: ReadableVec<V::I, V::T>,
        V::T: Add<V::T, Output = V::T> + Ord,
    {
        self.compute_aggregate_of_others(max_from, others, exit, |batches, j| {
            batches.iter().map(|b| &b[j]).max().unwrap().clone()
        })
    }

    /// Computes weighted average: sum(weight_i * value_i) / sum(weight_i)
    ///
    /// Takes parallel slices of weight and value vecs from multiple sources.
    /// For each index, computes the weighted average across all sources.
    /// Returns zero if total weight is zero.
    pub fn compute_weighted_average_of_others<W, OW, OV>(
        &mut self,
        max_from: V::I,
        weights: &[&OW],
        values: &[&OV],
        exit: &Exit,
    ) -> Result<()>
    where
        W: VecValue + Into<f64>,
        OW: ReadableVec<V::I, W>,
        OV: ReadableVec<V::I, V::T>,
        V::T: Into<f64> + From<f64>,
    {
        if weights.len() != values.len() {
            return Err(Error::InvalidArgument(
                "weights and values must have same length",
            ));
        }

        if weights.is_empty() {
            return Err(Error::InvalidArgument(
                "weights and values must have at least one element",
            ));
        }

        self.compute_init(
            weights.iter().map(|v| v.version()).sum::<Version>()
                + values.iter().map(|v| v.version()).sum(),
            max_from,
            exit,
            |this| {
            let skip = this.len();

            let source_end = weights
                .iter()
                .map(|w| w.len())
                .chain(values.iter().map(|v| v.len()))
                .min()
                .unwrap_or(0);
            let end = this.batch_end(source_end);

            if skip >= end {
                return Ok(());
            }

            let weight_batches: Vec<Vec<W>> = weights
                .iter()
                .map(|w| w.collect_range_at(skip, end))
                .collect();
            let value_batches: Vec<Vec<V::T>> = values
                .iter()
                .map(|v| v.collect_range_at(skip, end))
                .collect();

            for j in 0..(end - skip) {
                let i = skip + j;
                let mut total_weight = 0.0_f64;
                let mut weighted_sum = 0.0_f64;

                for (w_batch, v_batch) in weight_batches.iter().zip(value_batches.iter()) {
                    let weight: f64 = w_batch[j].clone().into();
                    let value: f64 = v_batch[j].clone().into();

                    if weight > 0.0 {
                        total_weight += weight;
                        weighted_sum += weight * value;
                    }
                }

                let result = if total_weight > 0.0 {
                    V::T::from(weighted_sum / total_weight)
                } else {
                    V::T::from(0.0)
                };

                this.checked_push_at(i, result)?;
            }

            Ok(())
        })
    }

    pub fn compute_sum_from_indexes<A, B>(
        &mut self,
        max_from: V::I,
        first_indexes: &impl ReadableVec<V::I, A>,
        indexes_count: &impl ReadableVec<V::I, B>,
        source: &impl ReadableVec<A, V::T>,
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
        first_indexes: &impl ReadableVec<V::I, A>,
        indexes_count: &impl ReadableVec<V::I, B>,
        source: &impl ReadableVec<A, V::T>,
        mut filter: impl FnMut(&V::T) -> bool,
        exit: &Exit,
    ) -> Result<()>
    where
        V::T: Default + SaturatingAdd,
        A: VecIndex + VecValue,
        B: VecValue,
        usize: From<B>,
    {
        self.compute_init(
            first_indexes.version() + indexes_count.version() + source.version(),
            max_from,
            exit,
            |this| {
            let skip = this.len();
            let source_end = indexes_count.len();
            let end = this.batch_end(source_end);
            if skip >= end {
                return Ok(());
            }

            // Get the starting position in source
            let pos = if skip < first_indexes.len() {
                first_indexes.collect_one_at(skip).unwrap().to_usize()
            } else {
                return Ok(());
            };

            let counts_batch: Vec<usize> = indexes_count
                .collect_range_at(skip, end)
                .into_iter()
                .map(usize::from)
                .collect();
            let total_count: usize = counts_batch.iter().sum();

            // Single batch read instead of per-element allocations
            let all_values = source.collect_range_at(pos, pos + total_count);
            let mut offset = 0;
            for (j, &count) in counts_batch.iter().enumerate() {
                let i = skip + j;
                let sum = all_values[offset..offset + count]
                    .iter()
                    .filter(|v| filter(v))
                    .fold(V::T::default(), |acc, val| acc.saturating_add(val.clone()));
                offset += count;
                this.checked_push_at(i, sum)?;
            }

            Ok(())
        })
    }

    pub fn compute_count_from_indexes<A, B>(
        &mut self,
        max_from: V::I,
        first_indexes: &impl ReadableVec<V::I, A>,
        other_to_else: &impl ReadableVec<A, B>,
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
        first_indexes: &impl ReadableVec<V::I, A>,
        other_to_else: &impl ReadableVec<A, B>,
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
        self.compute_init(first_indexes.version() + other_to_else.version(), max_from, exit, |this| {
            let skip = this.len();
            let source_end = first_indexes.len();
            let end = this.batch_end(source_end);
            if skip >= end {
                return Ok(());
            }

            let fi_batch = first_indexes.collect_range_at(skip, end);
            for (j, first_index) in fi_batch.iter().enumerate() {
                let i = skip + j;
                let next_first = if i + 1 < first_indexes.len() {
                    fi_batch.get(j + 1).map(|fi| fi.to_usize()).unwrap_or_else(|| {
                        first_indexes.collect_one_at(i + 1).unwrap().to_usize()
                    })
                } else {
                    other_to_else.len()
                };

                let range = first_index.to_usize()..next_first;
                let count = range.into_iter().filter(|i| filter(A::from(*i))).count();
                this.checked_push_at(i, V::T::from(A::from(count)))?;
            }

            Ok(())
        })
    }
}
