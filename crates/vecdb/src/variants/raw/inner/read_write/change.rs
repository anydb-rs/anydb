use std::collections::BTreeSet;

use crate::ChangeData;

pub(super) struct RawChangeData<T> {
    pub base: ChangeData<T>,
    pub modifications: Vec<(usize, T)>,
    pub prev_holes: BTreeSet<usize>,
}
