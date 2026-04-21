use super::UnaryTransform;

/// v -> v
pub struct Ident;

impl<T> UnaryTransform<T> for Ident {
    #[inline(always)]
    fn apply(value: T) -> T {
        value
    }
}
