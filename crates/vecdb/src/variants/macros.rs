/// Generates trait implementations for vec wrappers (LZ4Vec, PcoVec, ZstdVec, BytesVec, ZeroCopyVec).
///
/// # Usage
/// ```ignore
/// impl_vec_wrapper!(
///     LZ4Vec,
///     CompressedVecInner<I, T, LZ4Strategy<T>>,
///     LZ4VecValue,
///     Format::LZ4,
/// );
/// ```
///
/// This generates implementations for:
/// - `Deref` / `DerefMut`
/// - `ImportableVec`
/// - `AnyVec`
/// - `TypedVec`
/// - `AnyStoredVec`
/// - `GenericStoredVec`
/// - `ScannableVec` (delegates `for_each_range_dyn` / `fold_range` to inner)
macro_rules! impl_vec_wrapper {
    ($wrapper:ident, $inner:ty, $value_trait:ident, $format:expr) => {
        impl<I, T> ::std::ops::Deref for $wrapper<I, T> {
            type Target = $inner;

            #[inline]
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl<I, T> ::std::ops::DerefMut for $wrapper<I, T> {
            #[inline]
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }

        impl<I, T> $crate::ImportableVec for $wrapper<I, T>
        where
            I: $crate::VecIndex,
            T: $value_trait,
        {
            fn import(
                db: &::rawdb::Database,
                name: &str,
                version: $crate::Version,
            ) -> $crate::Result<Self> {
                Self::import_with((db, name, version).into())
            }

            fn import_with(options: $crate::ImportOptions) -> $crate::Result<Self> {
                Ok(Self(<$inner>::import_with(options, $format)?))
            }

            fn forced_import(
                db: &::rawdb::Database,
                name: &str,
                version: $crate::Version,
            ) -> $crate::Result<Self> {
                Self::forced_import_with((db, name, version).into())
            }

            fn forced_import_with(options: $crate::ImportOptions) -> $crate::Result<Self> {
                Ok(Self(<$inner>::forced_import_with(options, $format)?))
            }
        }

        impl<I, T> $crate::AnyVec for $wrapper<I, T>
        where
            I: $crate::VecIndex,
            T: $value_trait,
        {
            #[inline]
            fn version(&self) -> $crate::Version {
                self.0.version()
            }

            #[inline]
            fn name(&self) -> &str {
                self.0.name()
            }

            #[inline]
            fn len(&self) -> usize {
                self.0.len()
            }

            #[inline]
            fn index_type_to_string(&self) -> &'static str {
                self.0.index_type_to_string()
            }

            #[inline]
            fn value_type_to_size_of(&self) -> usize {
                self.0.value_type_to_size_of()
            }

            #[inline]
            fn value_type_to_string(&self) -> &'static str {
                self.0.value_type_to_string()
            }

            #[inline]
            fn region_names(&self) -> Vec<String> {
                self.0.region_names()
            }
        }

        impl<I, T> $crate::TypedVec for $wrapper<I, T>
        where
            I: $crate::VecIndex,
            T: $value_trait,
        {
            type I = I;
            type T = T;
        }

        impl<I, T> $crate::AnyStoredVec for $wrapper<I, T>
        where
            I: $crate::VecIndex,
            T: $value_trait,
        {
            #[inline]
            fn db_path(&self) -> ::std::path::PathBuf {
                self.0.db_path()
            }

            #[inline]
            fn region(&self) -> &::rawdb::Region {
                self.0.region()
            }

            #[inline]
            fn header(&self) -> &$crate::Header {
                self.0.header()
            }

            #[inline]
            fn mut_header(&mut self) -> &mut $crate::Header {
                self.0.mut_header()
            }

            #[inline]
            fn saved_stamped_changes(&self) -> u16 {
                self.0.saved_stamped_changes()
            }

            #[inline]
            fn db(&self) -> ::rawdb::Database {
                self.0.db()
            }

            #[inline]
            fn real_stored_len(&self) -> usize {
                self.0.real_stored_len()
            }

            #[inline]
            fn stored_len(&self) -> usize {
                self.0.stored_len()
            }

            #[inline]
            fn write(&mut self) -> $crate::Result<bool> {
                self.0.write()
            }

            #[inline]
            fn serialize_changes(&self) -> $crate::Result<Vec<u8>> {
                self.0.serialize_changes()
            }

            #[inline]
            fn any_stamped_write_with_changes(
                &mut self,
                stamp: $crate::Stamp,
            ) -> $crate::Result<()> {
                $crate::GenericStoredVec::stamped_write_with_changes(&mut self.0, stamp)
            }

            fn remove(self) -> $crate::Result<()> {
                self.0.remove()
            }

            fn any_reset(&mut self) -> $crate::Result<()> {
                $crate::GenericStoredVec::reset(self)
            }
        }

        impl<I, T> $crate::GenericStoredVec<I, T> for $wrapper<I, T>
        where
            I: $crate::VecIndex,
            T: $value_trait,
        {
            #[inline]
            fn collect_stored_range(
                &self,
                from: usize,
                to: usize,
            ) -> $crate::Result<Vec<T>> {
                self.0.collect_stored_range(from, to)
            }

            #[inline(always)]
            fn read_value_from_bytes(&self, bytes: &[u8]) -> $crate::Result<T> {
                self.0.read_value_from_bytes(bytes)
            }

            #[inline]
            fn write_value_to(&self, value: &T, buf: &mut Vec<u8>) {
                self.0.write_value_to(value, buf)
            }

            #[inline]
            fn pushed(&self) -> &[T] {
                self.0.pushed()
            }

            #[inline]
            fn mut_pushed(&mut self) -> &mut Vec<T> {
                self.0.mut_pushed()
            }

            #[inline]
            fn prev_pushed(&self) -> &[T] {
                self.0.prev_pushed()
            }

            #[inline]
            fn mut_prev_pushed(&mut self) -> &mut Vec<T> {
                self.0.mut_prev_pushed()
            }

            #[inline]
            fn update_stored_len(&self, val: usize) {
                self.0.update_stored_len(val)
            }

            #[inline]
            fn prev_stored_len(&self) -> usize {
                self.0.prev_stored_len()
            }

            #[inline]
            fn mut_prev_stored_len(&mut self) -> &mut usize {
                self.0.mut_prev_stored_len()
            }

            #[inline]
            fn reset(&mut self) -> $crate::Result<()> {
                self.0.reset()
            }

            #[inline]
            fn restore_truncated_value(&mut self, index: usize, value: T) {
                self.0.restore_truncated_value(index, value)
            }

            #[inline]
            fn truncate_if_needed_at(&mut self, index: usize) -> $crate::Result<()> {
                self.0.truncate_if_needed_at(index)
            }

            #[inline]
            fn reset_unsaved(&mut self) {
                self.0.reset_unsaved()
            }

            #[inline]
            fn is_dirty(&self) -> bool {
                self.0.is_dirty()
            }

            #[inline]
            fn rollback_before(&mut self, stamp: $crate::Stamp) -> $crate::Result<$crate::Stamp> {
                self.0.rollback_before(stamp)
            }

            #[inline]
            fn rollback(&mut self) -> $crate::Result<()> {
                self.0.rollback()
            }

            #[inline]
            fn deserialize_then_undo_changes(&mut self, bytes: &[u8]) -> $crate::Result<()> {
                self.0.deserialize_then_undo_changes(bytes)
            }
        }

        impl<I, T> $crate::ScannableVec<I, T> for $wrapper<I, T>
        where
            I: $crate::VecIndex,
            T: $value_trait,
        {
            #[inline]
            fn for_each_range_dyn(&self, from: usize, to: usize, f: &mut dyn FnMut(T)) {
                $crate::ScannableVec::<I, T>::for_each_range_dyn(&self.0, from, to, f)
            }

            #[inline]
            fn fold_range<B, F: FnMut(B, T) -> B>(&self, from: usize, to: usize, init: B, f: F) -> B
            where
                Self: Sized,
            {
                $crate::ScannableVec::<I, T>::fold_range(&self.0, from, to, init, f)
            }

            #[inline]
            fn try_fold_range<B, E, F: FnMut(B, T) -> ::std::result::Result<B, E>>(
                &self,
                from: usize,
                to: usize,
                init: B,
                f: F,
            ) -> ::std::result::Result<B, E>
            where
                Self: Sized,
            {
                $crate::ScannableVec::<I, T>::try_fold_range(&self.0, from, to, init, f)
            }
        }

    };
}

pub(crate) use impl_vec_wrapper;
