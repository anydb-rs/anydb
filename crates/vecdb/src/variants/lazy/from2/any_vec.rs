use crate::{AnyVec, VecIndex, VecValue, Version, short_type_name};

use super::LazyVecFrom2;

impl<I, T, S1I, S1T, S2I, S2T> AnyVec for LazyVecFrom2<I, T, S1I, S1T, S2I, S2T>
where
    I: VecIndex,
    T: VecValue,
    S1I: VecIndex,
    S1T: VecValue,
    S2I: VecIndex,
    S2T: VecValue,
{
    fn version(&self) -> Version {
        self.base_version + self.source1.version() + self.source2.version()
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn index_type_to_string(&self) -> &'static str {
        I::to_string()
    }

    fn len(&self) -> usize {
        let len1 = if self.s1_counts {
            self.source1.len()
        } else {
            usize::MAX
        };
        let len2 = if self.s2_counts {
            self.source2.len()
        } else {
            usize::MAX
        };
        len1.min(len2)
    }

    #[inline]
    fn value_type_to_size_of(&self) -> usize {
        size_of::<T>()
    }

    #[inline]
    fn value_type_to_string(&self) -> &'static str {
        short_type_name::<T>()
    }

    #[inline]
    fn region_names(&self) -> Vec<String> {
        Vec::new()
    }
}
