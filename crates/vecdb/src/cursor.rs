use crate::{READ_CHUNK_SIZE, ReadableVec, VecIndex, VecValue};

/// Forward-only reader that reuses an internal buffer across chunked `read_into_at` calls.
///
/// One allocation for the lifetime of the cursor. Ideal for sequential access patterns
/// (iterating tx-indexed vecs, computing rolling windows) where repeated `collect_one`
/// calls would decompress the same page thousands of times.
///
/// # Example
/// ```ignore
/// let mut c = vec.cursor();
/// while let Some(val) = c.next() {
///     // process val
/// }
/// ```
pub struct Cursor<'a, I: VecIndex, T: VecValue> {
    source: &'a dyn ReadableVec<I, T>,
    buf: Vec<T>,
    /// Absolute position of buf[0] in the source vec.
    buf_start: usize,
    /// Current absolute position in the source vec.
    pos: usize,
    chunk_size: usize,
    len: usize,
}

impl<'a, I: VecIndex, T: VecValue> Cursor<'a, I, T> {
    /// Creates a new cursor with default chunk size ([`READ_CHUNK_SIZE`]).
    #[inline]
    pub fn new<V: ReadableVec<I, T>>(source: &'a V) -> Self {
        Self::init(source, READ_CHUNK_SIZE)
    }

    /// Creates a new cursor from a trait object with default chunk size.
    #[inline]
    pub fn from_dyn(source: &'a dyn ReadableVec<I, T>) -> Self {
        Self::init(source, READ_CHUNK_SIZE)
    }

    #[inline]
    fn init(source: &'a dyn ReadableVec<I, T>, chunk_size: usize) -> Self {
        let len = source.len();
        Self {
            source,
            buf: Vec::with_capacity(chunk_size.min(len)),
            buf_start: 0,
            pos: 0,
            chunk_size,
            len,
        }
    }

    /// Returns the current absolute position.
    #[inline]
    pub fn position(&self) -> usize {
        self.pos
    }

    /// Returns the number of elements remaining.
    #[inline]
    pub fn remaining(&self) -> usize {
        self.len.saturating_sub(self.pos)
    }

    /// Advances the position by `n` without reading. Cheap — no decompression.
    #[inline]
    pub fn advance(&mut self, n: usize) {
        self.pos = self.pos.saturating_add(n).min(self.len);
    }

    /// Returns the next value and advances position, or `None` if exhausted.
    #[inline]
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<T> {
        let local = self.ensure_buffered()?;
        let val = self.buf[local].clone();
        self.pos += 1;
        Some(val)
    }

    /// Folds over the next `n` elements with a monomorphized closure.
    /// Advances position by the number of elements consumed.
    ///
    /// Consumes any already-buffered data first, then reads fresh chunks.
    /// The last chunk may read past `n` — leftover data stays in the buffer
    /// for subsequent `next()` calls.
    #[inline]
    pub fn fold<B>(&mut self, n: usize, init: B, mut f: impl FnMut(B, T) -> B) -> B {
        let target = self.pos.saturating_add(n).min(self.len);
        let mut acc = init;

        while self.pos < target {
            if self.ensure_buffered().is_none() {
                break;
            }
            let local = self.pos - self.buf_start;
            let local_end = (target - self.buf_start).min(self.buf.len());
            for val in self.buf[local..local_end].iter().cloned() {
                acc = f(acc, val);
            }
            self.pos = self.buf_start + local_end;
        }

        acc
    }

    /// Calls `f` for each of the next `n` elements.
    /// Advances position by the number of elements consumed.
    #[inline]
    pub fn for_each(&mut self, n: usize, mut f: impl FnMut(T)) {
        self.fold(n, (), |(), v| f(v));
    }

    /// Ensures the buffer contains data at `self.pos`.
    /// Returns the local index within `buf`, or `None` if exhausted.
    #[inline]
    fn ensure_buffered(&mut self) -> Option<usize> {
        if self.pos >= self.len {
            return None;
        }

        let buf_end = self.buf_start + self.buf.len();
        if self.pos < buf_end {
            return Some(self.pos - self.buf_start);
        }

        // Refill: read a full chunk from current position.
        self.buf.clear();
        let end = self.pos.saturating_add(self.chunk_size).min(self.len);
        self.buf_start = self.pos;
        self.source.read_into_at(self.pos, end, &mut self.buf);

        if self.buf.is_empty() { None } else { Some(0) }
    }
}
