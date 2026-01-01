use std::ops::Add;

use crate::{
    AnyVec, Error, Exit, GenericStoredVec, IterableVec, Result, StoredVec, VecIndex, VecValue,
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
        O: IterableVec<V::I, V::T>,
        F: Fn(Box<dyn Iterator<Item = V::T> + '_>) -> V::T,
    {
        self.validate_computed_version_or_reset(others.iter().map(|v| v.version()).sum())?;

        if others.is_empty() {
            return Err(Error::InvalidArgument(
                "others must have at least one element",
            ));
        }

        self.truncate_if_needed(max_from)?;

        self.repeat_until_complete(exit, |this| {
            let skip = this.len();
            let mut others_iter = others
                .iter()
                .map(|v| v.iter().skip(skip))
                .collect::<Vec<_>>();

            for i in skip..others.first().unwrap().len() {
                let values = Box::new(others_iter.iter_mut().map(|iter| iter.next().unwrap()));
                let result = aggregate(values);
                this.checked_push_at(i, result)?;

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
            first_indexes.version() + indexes_count.version() + source.version(),
        )?;

        self.truncate_if_needed(max_from)?;

        self.repeat_until_complete(exit, |this| {
            let skip = this.len();
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
                this.checked_push_at(i, sum)?;

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
        self.validate_computed_version_or_reset(first_indexes.version() + other_to_else.version())?;

        self.truncate_if_needed(max_from)?;

        self.repeat_until_complete(exit, |this| {
            let skip = this.len();
            let mut other_iter = first_indexes.iter();

            for (i, first_index) in first_indexes.iter().enumerate().skip(skip) {
                let end = other_iter
                    .get_at(i + 1)
                    .map(|v| v.to_usize())
                    .unwrap_or_else(|| other_to_else.len());

                let range = first_index.to_usize()..end;
                let count = range.into_iter().filter(|i| filter(A::from(*i))).count();
                this.checked_push_at(i, V::T::from(A::from(count)))?;

                if this.batch_limit_reached() {
                    break;
                }
            }

            Ok(())
        })
    }
}
