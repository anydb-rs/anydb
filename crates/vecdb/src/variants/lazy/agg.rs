use std::sync::Arc;

use crate::{
    AnyVec, Cursor, ReadableBoxedVec, ReadableVec, TypedVec, VecIndex, VecValue, Version,
    short_type_name,
};

/// Lazy aggregation vector that maps coarser output indices to ranges in a finer source.
///
/// `LazyAggVec` computes values on-the-fly by looking up the last value in each
/// coarse-grained period from the underlying fine-grained source. No data is stored
/// on disk; values are computed during iteration using cursor-based sequential access.
///
/// Five type parameters:
/// - `I`: Output index type (coarser, e.g., `Day1`)
/// - `O`: Output value type
/// - `S1I`: Source index type (finer, e.g., `Height`)
/// - `S2T`: Mapping value type (stored in the mapping vec)
/// - `S1T`: Source value type (defaults to `O`)
pub struct LazyAggVec<I, O, S1I, S2T, S1T = O>
where
    I: VecIndex,
    O: VecValue,
    S1I: VecIndex,
    S2T: VecValue,
    S1T: VecValue,
{
    name: Arc<str>,
    version: Version,
    source: ReadableBoxedVec<S1I, S1T>,
    mapping: ReadableBoxedVec<I, S2T>,
    #[allow(clippy::type_complexity)]
    for_each_range:
        fn(usize, usize, &ReadableBoxedVec<S1I, S1T>, &ReadableBoxedVec<I, S2T>, &mut dyn FnMut(O)),
}

// --- Constructors ---

impl<I, O, S1I, S2T, S1T> LazyAggVec<I, O, S1I, S2T, S1T>
where
    I: VecIndex,
    O: VecValue,
    S1I: VecIndex,
    S2T: VecValue,
    S1T: VecValue,
{
    /// Create a lazy aggregation vec with a custom `for_each_range` function.
    #[allow(clippy::type_complexity)]
    pub fn new(
        name: &str,
        version: Version,
        source: ReadableBoxedVec<S1I, S1T>,
        mapping: ReadableBoxedVec<I, S2T>,
        for_each_range: fn(
            usize,
            usize,
            &ReadableBoxedVec<S1I, S1T>,
            &ReadableBoxedVec<I, S2T>,
            &mut dyn FnMut(O),
        ),
    ) -> Self {
        Self {
            name: Arc::from(name),
            version,
            source,
            mapping,
            for_each_range,
        }
    }
}

impl<I, O, SI> LazyAggVec<I, O, SI, SI>
where
    I: VecIndex,
    O: VecValue,
    SI: VecIndex,
{
    /// Create from a source using an explicit first-index mapping.
    ///
    /// For output index `i`, looks up `mapping[i+1].to_usize() - 1` (last position in the period).
    /// Uses cursor-based sequential access for efficient forward traversal.
    pub fn from_first_index(
        name: &str,
        version: Version,
        source: ReadableBoxedVec<SI, O>,
        first_index: ReadableBoxedVec<I, SI>,
    ) -> Self {
        fn for_each_range<I: VecIndex, O: VecValue, SI: VecIndex>(
            from: usize,
            to: usize,
            source: &ReadableBoxedVec<SI, O>,
            mapping: &ReadableBoxedVec<I, SI>,
            f: &mut dyn FnMut(O),
        ) {
            let map_end = (to + 1).min(mapping.len());
            let heights = mapping.collect_range_dyn(from, map_end);
            let source_len = source.len();
            let mut cursor = Cursor::from_dyn(&**source);
            for idx in 0..(to - from) {
                let next_first = heights
                    .get(idx + 1)
                    .map(|h| h.to_usize())
                    .unwrap_or(source_len);
                if next_first == 0 {
                    continue;
                }
                let target = next_first - 1;
                if cursor.position() <= target {
                    cursor.advance(target - cursor.position());
                    if let Some(v) = cursor.next() {
                        f(v);
                    }
                } else if let Some(v) = source.collect_one_at(target) {
                    f(v);
                }
            }
        }
        Self {
            name: Arc::from(name),
            version,
            source,
            mapping: first_index,
            for_each_range: for_each_range::<I, O, SI>,
        }
    }
}

impl<I, T, SI> LazyAggVec<I, Option<T>, SI, SI, T>
where
    I: VecIndex,
    T: VecValue,
    SI: VecIndex,
{
    /// Create a sparse aggregation that emits `Option<T>` for every period.
    ///
    /// `Some(last_value)` when the period contains source elements,
    /// `None` when the period is empty (no source elements mapped to it).
    pub fn sparse_from_first_index(
        name: &str,
        version: Version,
        source: ReadableBoxedVec<SI, T>,
        first_index: ReadableBoxedVec<I, SI>,
    ) -> Self {
        fn for_each_range<I: VecIndex, T: VecValue, SI: VecIndex>(
            from: usize,
            to: usize,
            source: &ReadableBoxedVec<SI, T>,
            mapping: &ReadableBoxedVec<I, SI>,
            f: &mut dyn FnMut(Option<T>),
        ) {
            let map_end = (to + 1).min(mapping.len());
            let indices = mapping.collect_range_dyn(from, map_end);
            let source_len = source.len();
            let mut cursor = Cursor::from_dyn(&**source);

            for idx in 0..(to - from) {
                let current_first = indices[idx].to_usize();
                let next_first = indices
                    .get(idx + 1)
                    .map(|h| h.to_usize())
                    .unwrap_or(source_len);

                // Empty period: no elements belong to this slot
                if next_first == 0 || current_first >= next_first {
                    f(None);
                    continue;
                }

                // Last position in this period
                let target = next_first - 1;

                if cursor.position() <= target {
                    cursor.advance(target - cursor.position());
                    match cursor.next() {
                        Some(v) => f(Some(v)),
                        None => f(None),
                    }
                } else {
                    match source.collect_one_at(target) {
                        Some(v) => f(Some(v)),
                        None => f(None),
                    }
                }
            }
        }
        Self {
            name: Arc::from(name),
            version,
            source,
            mapping: first_index,
            for_each_range: for_each_range::<I, T, SI>,
        }
    }

    /// Returns the underlying source vec.
    pub fn source(&self) -> &ReadableBoxedVec<SI, T> {
        &self.source
    }

    /// Returns the first-index mapping vec.
    pub fn first_index(&self) -> &ReadableBoxedVec<I, SI> {
        &self.mapping
    }
}

// --- Clone ---

impl<I, O, S1I, S2T, S1T> Clone for LazyAggVec<I, O, S1I, S2T, S1T>
where
    I: VecIndex,
    O: VecValue,
    S1I: VecIndex,
    S2T: VecValue,
    S1T: VecValue,
{
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            version: self.version,
            source: self.source.clone(),
            mapping: self.mapping.clone(),
            for_each_range: self.for_each_range,
        }
    }
}

// --- AnyVec ---

impl<I, O, S1I, S2T, S1T> AnyVec for LazyAggVec<I, O, S1I, S2T, S1T>
where
    I: VecIndex,
    O: VecValue,
    S1I: VecIndex,
    S2T: VecValue,
    S1T: VecValue,
{
    fn version(&self) -> Version {
        self.version + self.source.version() + self.mapping.version()
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn index_type_to_string(&self) -> &'static str {
        I::to_string()
    }

    fn len(&self) -> usize {
        self.mapping.len()
    }

    #[inline]
    fn value_type_to_size_of(&self) -> usize {
        size_of::<O>()
    }

    #[inline]
    fn value_type_to_string(&self) -> &'static str {
        short_type_name::<O>()
    }

    #[inline]
    fn region_names(&self) -> Vec<String> {
        vec![]
    }
}

// --- TypedVec ---

impl<I, O, S1I, S2T, S1T> TypedVec for LazyAggVec<I, O, S1I, S2T, S1T>
where
    I: VecIndex,
    O: VecValue,
    S1I: VecIndex,
    S2T: VecValue,
    S1T: VecValue,
{
    type I = I;
    type T = O;
}

// --- ReadableVec ---

impl<I, O, S1I, S2T, S1T> ReadableVec<I, O> for LazyAggVec<I, O, S1I, S2T, S1T>
where
    I: VecIndex,
    O: VecValue,
    S1I: VecIndex,
    S2T: VecValue,
    S1T: VecValue,
{
    fn read_into_at(&self, from: usize, to: usize, buf: &mut Vec<O>) {
        let to = to.min(self.mapping.len());
        if from >= to {
            return;
        }
        buf.reserve(to - from);
        (self.for_each_range)(from, to, &self.source, &self.mapping, &mut |v| buf.push(v));
    }

    fn for_each_range_dyn_at(&self, from: usize, to: usize, f: &mut dyn FnMut(O)) {
        let to = to.min(self.mapping.len());
        if from >= to {
            return;
        }
        (self.for_each_range)(from, to, &self.source, &self.mapping, f);
    }

    #[inline]
    fn fold_range_at<B, F: FnMut(B, O) -> B>(&self, from: usize, to: usize, init: B, mut f: F) -> B
    where
        Self: Sized,
    {
        let to = to.min(self.mapping.len());
        if from >= to {
            return init;
        }
        let mut acc = Some(init);
        (self.for_each_range)(from, to, &self.source, &self.mapping, &mut |v| {
            acc = Some(f(acc.take().unwrap(), v));
        });
        acc.unwrap()
    }

    #[inline]
    fn try_fold_range_at<B, E, F: FnMut(B, O) -> std::result::Result<B, E>>(
        &self,
        from: usize,
        to: usize,
        init: B,
        mut f: F,
    ) -> std::result::Result<B, E>
    where
        Self: Sized,
    {
        let to = to.min(self.mapping.len());
        if from >= to {
            return Ok(init);
        }
        let mut acc: Option<std::result::Result<B, E>> = Some(Ok(init));
        (self.for_each_range)(from, to, &self.source, &self.mapping, &mut |v| {
            if let Some(Ok(a)) = acc.take() {
                acc = Some(f(a, v));
            }
        });
        acc.unwrap()
    }

    #[inline]
    fn collect_one_at(&self, index: usize) -> Option<O> {
        if index >= self.mapping.len() {
            return None;
        }
        let mut result = None;
        (self.for_each_range)(index, index + 1, &self.source, &self.mapping, &mut |v| {
            result = Some(v)
        });
        result
    }
}
