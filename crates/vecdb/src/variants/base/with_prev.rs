/// Wrapper that tracks both current and previous values for rollback support.
///
/// Used for types where both current and previous are the same type and cloneable.
#[derive(Debug, Clone)]
pub struct WithPrev<T> {
    current: T,
    previous: T,
}

impl<T> WithPrev<T> {
    /// Creates a new WithPrev with the same value for current and previous.
    pub fn new(value: T) -> Self
    where
        T: Clone,
    {
        Self {
            current: value.clone(),
            previous: value,
        }
    }

    /// Returns a reference to the current value.
    #[inline(always)]
    pub fn current(&self) -> &T {
        &self.current
    }

    /// Returns a mutable reference to the current value.
    #[inline]
    pub fn current_mut(&mut self) -> &mut T {
        &mut self.current
    }

    /// Returns a reference to the previous value.
    #[inline(always)]
    pub fn previous(&self) -> &T {
        &self.previous
    }

    /// Returns a mutable reference to the previous value.
    #[inline]
    pub fn previous_mut(&mut self) -> &mut T {
        &mut self.previous
    }

    /// Saves the current value to previous.
    #[inline]
    pub fn save(&mut self)
    where
        T: Clone,
    {
        self.previous.clone_from(&self.current);
    }

    /// Restores the previous value to current.
    #[inline]
    pub fn restore(&mut self)
    where
        T: Clone,
    {
        self.current.clone_from(&self.previous);
    }

    /// Swaps current and previous values.
    #[inline]
    pub fn swap(&mut self) {
        std::mem::swap(&mut self.current, &mut self.previous);
    }

    /// Takes the current value, replacing it with the default.
    #[inline]
    pub fn take_current(&mut self) -> T
    where
        T: Default,
    {
        std::mem::take(&mut self.current)
    }

    /// Takes the previous value, replacing it with the default.
    #[inline]
    pub fn take_previous(&mut self) -> T
    where
        T: Default,
    {
        std::mem::take(&mut self.previous)
    }

    /// Clears both current and previous values.
    #[inline]
    pub fn clear(&mut self)
    where
        T: Default,
    {
        self.current = T::default();
        self.previous = T::default();
    }

    /// Clears only the previous value.
    #[inline]
    pub fn clear_previous(&mut self)
    where
        T: Default,
    {
        self.previous = T::default();
    }
}

impl<T: Default> Default for WithPrev<T> {
    fn default() -> Self {
        Self {
            current: T::default(),
            previous: T::default(),
        }
    }
}
