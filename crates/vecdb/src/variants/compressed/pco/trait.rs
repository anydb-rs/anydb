use pco::data_types::Number;

/// Witnesses that `Self` has the same memory layout as `T`, enabling the
/// zero-copy cast between `[Self]` and `[T::NumberType]` used by pco's encoder.
pub trait TransparentPco<T> {}

/// Binds a vec value type to its underlying pco `Number` representation.
pub trait Pco
where
    Self: TransparentPco<Self::NumberType>,
{
    type NumberType: Number;
}
