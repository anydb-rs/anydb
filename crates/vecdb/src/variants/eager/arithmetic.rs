use std::ops::{Add, Div, Mul, Sub};

use crate::{Exit, IterableVec, Result, StoredVec, VecValue};

use super::{CheckedSub, EagerVec};

impl<V> EagerVec<V>
where
    V: StoredVec,
{
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
            |(i, v1, v2, ..)| {
                (
                    i,
                    v1.checked_sub(v2)
                        .expect("subtraction underflow in compute_subtract"),
                )
            },
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

    fn compute_percentage_<A, B>(
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
}
