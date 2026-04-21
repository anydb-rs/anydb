//! Numeric traits (overflow-safe arithmetic) shared across the crate.

mod checked_sub;
mod saturating_add;

pub use checked_sub::*;
pub use saturating_add::*;
