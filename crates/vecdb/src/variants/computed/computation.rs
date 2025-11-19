/// Computation strategy for derived vectors.
#[derive(Default, Debug, PartialEq, PartialOrd, Ord, Eq, Clone, Copy)]
pub enum Computation {
    /// Values computed once, stored on disk, and incrementally updated.
    Eager,
    /// Values recomputed on-the-fly during each access.
    #[default]
    Lazy,
}

impl Computation {
    pub fn eager(&self) -> bool {
        *self == Self::Eager
    }

    pub fn lazy(&self) -> bool {
        *self == Self::Lazy
    }
}
