use super::{AnySerializableVec, AnyWritableVec};

/// Type-erased trait for vectors that are both writable and serializable.
/// This trait is automatically implemented for any type that implements both
/// `AnyWritableVec` and `AnySerializableVec`.
pub trait AnyExportableVec: AnyWritableVec + AnySerializableVec {}

/// Blanket implementation for all types that implement both traits.
impl<V> AnyExportableVec for V where V: AnyWritableVec + AnySerializableVec {}
