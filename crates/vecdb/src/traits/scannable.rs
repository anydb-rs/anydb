use crate::{AnyVec, VecIndex, VecValue};

/// High-performance scanning over vector values.
///
/// This is the primary trait for reading data from any vec type — stored, compressed,
/// lazy, or computed. All methods see the full state including uncommitted (pushed) values.
///
/// # Method overview
///
/// | Method | Use when |
/// |--------|----------|
/// | `for_each` / `for_each_range` | Processing every element, static dispatch |
/// | `fold` / `fold_range` | Accumulating a result (SIMD-optimized on stored vecs) |
/// | `collect` / `collect_range` | Materializing values into a `Vec<T>` |
/// | `collect_one` / `collect_first` / `collect_last` | Materializing a single value |
/// | `for_each_range_dyn` | Trait-object contexts (`&dyn ScannableVec`) |
/// | `try_fold_range` | Fold with early exit on error |
///
/// # Point reads
///
/// For raw vecs, use `VecReader::get()` for O(1) random access.
/// For any vec through the trait, use `collect_one(i)` — this materializes
/// a single value (decodes a page for compressed vecs).
///
/// # Performance
///
/// Stored vecs override `fold_range` and `try_fold_range` to delegate to their
/// internal source's optimized `fold()`, enabling SIMD auto-vectorization.
/// The default implementations route through `for_each_range_dyn` which uses
/// `&mut dyn FnMut` — fast, but not SIMD-friendly.
///
/// For maximum throughput on stored vecs, prefer `fold_range` / `for_each_range`
/// with static dispatch (`&impl ScannableVec` or concrete type).
pub trait ScannableVec<I: VecIndex, T: VecValue>: AnyVec {
    // ── Required ─────────────────────────────────────────────────────

    /// Iterates over `[from, to)`, calling `f` for each value.
    ///
    /// Object-safe: callable on `&dyn ScannableVec`. Stored vecs implement this
    /// by folding their internal source with a `&mut dyn FnMut` callback.
    fn for_each_range_dyn(&self, from: usize, to: usize, f: &mut dyn FnMut(T));

    // ── Overridable (stored vecs override for SIMD) ──────────────────

    /// Folds over `[from, to)` with an accumulator.
    ///
    /// Stored vecs override this to delegate to their source's `fold()`,
    /// which the compiler can auto-vectorize. The default routes through
    /// `for_each_range_dyn`.
    #[inline]
    fn fold_range<B, F: FnMut(B, T) -> B>(&self, from: usize, to: usize, init: B, mut f: F) -> B
    where
        Self: Sized,
    {
        let mut acc = Some(init);
        self.for_each_range_dyn(from, to, &mut |v| {
            acc = Some(f(acc.take().unwrap(), v));
        });
        acc.unwrap()
    }

    /// Fallible fold over `[from, to)` with early exit on error.
    ///
    /// Stored vecs override this to delegate to their source's `try_fold()`,
    /// which truly short-circuits. The default runs `for_each_range_dyn` to
    /// completion even after an error (but discards subsequent results).
    #[inline]
    fn try_fold_range<B, E, F: FnMut(B, T) -> std::result::Result<B, E>>(
        &self,
        from: usize,
        to: usize,
        init: B,
        mut f: F,
    ) -> std::result::Result<B, E>
    where
        Self: Sized,
    {
        let mut acc = Some(init);
        let mut err: Option<E> = None;
        self.for_each_range_dyn(from, to, &mut |v| {
            if err.is_some() {
                return;
            }
            match f(acc.take().unwrap(), v) {
                Ok(b) => acc = Some(b),
                Err(e) => err = Some(e),
            }
        });
        match err {
            Some(e) => Err(e),
            None => Ok(acc.unwrap()),
        }
    }

    // ── Convenience (all have defaults) ──────────────────────────────

    /// Calls `f` for each value in `[from, to)`. Requires `Sized` (static dispatch).
    #[inline]
    fn for_each_range<F: FnMut(T)>(&self, from: usize, to: usize, mut f: F)
    where
        Self: Sized,
    {
        self.fold_range(from, to, (), |(), v| f(v));
    }

    /// Calls `f` for every value in the vector.
    #[inline]
    fn for_each<F: FnMut(T)>(&self, f: F)
    where
        Self: Sized,
    {
        self.for_each_range(0, self.len(), f);
    }

    /// Folds over all values with an accumulator.
    #[inline]
    fn fold<B, F: FnMut(B, T) -> B>(&self, init: B, f: F) -> B
    where
        Self: Sized,
    {
        self.fold_range(0, self.len(), init, f)
    }

    /// Collects values in `[from, to)` into a `Vec<T>`.
    #[inline]
    fn collect_range(&self, from: usize, to: usize) -> Vec<T> {
        let mut v = Vec::with_capacity(to.saturating_sub(from));
        self.for_each_range_dyn(from, to, &mut |val| v.push(val));
        v
    }

    /// Collects all values into a `Vec<T>`.
    #[inline]
    fn collect(&self) -> Vec<T> {
        self.collect_range(0, self.len())
    }

    /// Collects a single value at `index`, or `None` if out of bounds.
    #[inline]
    fn collect_one(&self, index: usize) -> Option<T> {
        let mut result = None;
        self.for_each_range_dyn(index, index + 1, &mut |v| result = Some(v));
        result
    }

    /// Collects the first value, or `None` if empty.
    #[inline]
    fn collect_first(&self) -> Option<T> {
        self.collect_one(0)
    }

    /// Collects the last value, or `None` if empty.
    #[inline]
    fn collect_last(&self) -> Option<T> {
        let len = self.len();
        if len > 0 { self.collect_one(len - 1) } else { None }
    }
}

/// Trait for scannable vectors that can be cloned as trait objects.
pub trait ScannableCloneableVec<I: VecIndex, T: VecValue>: ScannableVec<I, T> {
    fn boxed_clone(&self) -> Box<dyn ScannableCloneableVec<I, T>>;
}

impl<I: VecIndex, T: VecValue, U> ScannableCloneableVec<I, T> for U
where
    U: 'static + ScannableVec<I, T> + Clone,
{
    fn boxed_clone(&self) -> Box<dyn ScannableCloneableVec<I, T>> {
        Box::new(self.clone())
    }
}

impl<I: VecIndex, T: VecValue> Clone for Box<dyn ScannableCloneableVec<I, T>> {
    fn clone(&self) -> Self {
        self.boxed_clone()
    }
}

/// Type alias for boxed cloneable scannable vectors.
pub type ScannableBoxedVec<I, T> = Box<dyn ScannableCloneableVec<I, T>>;
