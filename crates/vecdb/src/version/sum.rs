use std::{iter::Sum, ops::Add};

use super::Version;

impl Sum for Version {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Self::ZERO, Add::add)
    }
}
