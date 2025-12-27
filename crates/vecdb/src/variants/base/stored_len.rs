use super::SharedLen;

/// Stored length with rollback support.
///
/// The current length is shared (via `SharedLen`) so clones see updates,
/// but previous length is local to each instance for independent rollback.
#[derive(Debug, Clone)]
pub struct StoredLen {
    current: SharedLen,
    previous: usize,
}

impl StoredLen {
    /// Creates a new stored length.
    pub fn new(val: usize) -> Self {
        Self {
            current: SharedLen::new(val),
            previous: val,
        }
    }

    /// Gets the current length.
    #[inline(always)]
    pub fn get(&self) -> usize {
        self.current.get()
    }

    /// Sets the current length.
    #[inline]
    pub fn set(&self, val: usize) {
        self.current.set(val);
    }

    /// Gets the previous length.
    #[inline(always)]
    pub fn previous(&self) -> usize {
        self.previous
    }

    /// Returns a mutable reference to the previous length.
    #[inline]
    pub fn previous_mut(&mut self) -> &mut usize {
        &mut self.previous
    }

    /// Saves current to previous.
    #[inline]
    pub fn save(&mut self) {
        self.previous = self.get();
    }

    /// Restores previous to current.
    #[inline]
    pub fn restore(&self) {
        self.set(self.previous);
    }
}

impl Default for StoredLen {
    fn default() -> Self {
        Self::new(0)
    }
}
