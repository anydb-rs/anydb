use std::collections::VecDeque;

use crate::{BoxedVecIterator, VecIndex, VecValue};

/// Strategy for accessing historical values within a sliding window.
///
/// Automatically selects between buffered (ring buffer) or direct access
/// based on the number of items to process relative to the window size.
pub enum Lookback<'a, I: VecIndex, T: VecValue> {
    /// Ring buffer strategy for many items (when items_to_process > window).
    /// Maintains a fixed-size buffer for efficient sequential access.
    Buffer {
        window: usize,
        buf: VecDeque<T>,
    },
    /// Direct iterator access for few items (when items_to_process <= window).
    /// More efficient when random access to recent history is sufficient.
    DirectAccess {
        window: usize,
        iter: BoxedVecIterator<'a, I, T>,
    },
}

impl<'a, I: VecIndex, T: VecValue> Lookback<'a, I, T> {
    /// Retrieves the value at the lookback position (index - window).
    ///
    /// Returns `default` if insufficient history exists (index < window).
    pub fn get_at_lookback(&mut self, index: usize, default: T) -> T
    where
        T: Default + Clone,
    {
        match self {
            Self::Buffer { window, buf } => {
                if buf.len() > *window {
                    buf.front().cloned().unwrap()
                } else {
                    default
                }
            }
            Self::DirectAccess { window, iter } => index
                .checked_sub(*window)
                .map(|prev_i| iter.get_at_unwrap(prev_i))
                .unwrap_or(default),
        }
    }

    /// Retrieves the lookback value and advances the window with the current value.
    ///
    /// For Buffer strategy: maintains a sliding window by popping old values when full.
    /// For DirectAccess: directly accesses the historical position.
    pub fn get_and_push(&mut self, index: usize, current: T, default: T) -> T
    where
        T: Clone,
    {
        match self {
            Self::Buffer { window, buf } => {
                let val = if buf.len() == *window {
                    buf.pop_front().unwrap()
                } else {
                    default
                };
                buf.push_back(current);
                val
            }
            Self::DirectAccess { window, iter } => index
                .checked_sub(*window)
                .map(|prev_i| iter.get_at_unwrap(prev_i))
                .unwrap_or(default),
        }
    }

    /// Advances the window with the current value, maintaining the configured window size.
    ///
    /// Only affects Buffer strategy; DirectAccess is unmodified.
    pub fn push_and_maintain(&mut self, current: T)
    where
        T: Clone,
    {
        if let Self::Buffer { window, buf } = self {
            buf.push_back(current);
            if buf.len() > *window + 1 {
                buf.pop_front();
            }
        }
    }
}
