//! Internal proc-macros for vecdb vec wrapper trait implementations.
//!
//! This crate provides proc-macros that generate boilerplate trait implementations
//! for vec wrapper types. Using proc-macros instead of declarative macros provides
//! better IDE support (rust-analyzer can expand and analyze them).
//!
//! This crate is internal to vecdb and not intended for external use.

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input, Ident, Token, Type,
};

/// Arguments for the vec_wrapper! macro.
/// Format: vec_wrapper!(WrapperName, InnerType, ValueTrait, IteratorType)
struct VecWrapperArgs {
    wrapper_name: Ident,
    _comma1: Token![,],
    inner_type: Type,
    _comma2: Token![,],
    value_trait: Type,
    _comma3: Token![,],
    iterator_type: Ident,
}

impl Parse for VecWrapperArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        Ok(Self {
            wrapper_name: input.parse()?,
            _comma1: input.parse()?,
            inner_type: input.parse()?,
            _comma2: input.parse()?,
            value_trait: input.parse()?,
            _comma3: input.parse()?,
            iterator_type: input.parse()?,
        })
    }
}

/// Generates trait implementations for vec wrappers (LZ4Vec, PcoVec, ZstdVec, BytesVec, ZeroCopyVec).
///
/// # Usage
/// ```ignore
/// vec_wrapper!(LZ4Vec, CompressedVecInner<I, T, LZ4Strategy<T>>, LZ4VecValue, LZ4VecIterator);
/// vec_wrapper!(BytesVec, RawVecInner<I, T, BytesStrategy<T>>, BytesVecValue, BytesVecIterator);
/// ```
///
/// This generates implementations for:
/// - `Deref` / `DerefMut`
/// - `AnyVec`
/// - `TypedVec`
/// - `AnyStoredVec`
/// - `GenericStoredVec`
/// - `IntoIterator`
/// - `IterableVec`
#[proc_macro]
pub fn vec_wrapper(input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(input as VecWrapperArgs);
    let wrapper = &args.wrapper_name;
    let inner = &args.inner_type;
    let value_trait = &args.value_trait;
    let iterator = &args.iterator_type;

    let expanded = quote! {
        impl<I, T> ::std::ops::Deref for #wrapper<I, T> {
            type Target = #inner;

            #[inline]
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl<I, T> ::std::ops::DerefMut for #wrapper<I, T> {
            #[inline]
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }

        impl<I, T> crate::AnyVec for #wrapper<I, T>
        where
            I: crate::VecIndex,
            T: #value_trait,
        {
            #[inline]
            fn version(&self) -> crate::Version {
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
            fn region_names(&self) -> Vec<String> {
                self.0.region_names()
            }
        }

        impl<I, T> crate::TypedVec for #wrapper<I, T>
        where
            I: crate::VecIndex,
            T: #value_trait,
        {
            type I = I;
            type T = T;
        }

        impl<I, T> crate::AnyStoredVec for #wrapper<I, T>
        where
            I: crate::VecIndex,
            T: #value_trait,
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
            fn header(&self) -> &crate::Header {
                self.0.header()
            }

            #[inline]
            fn mut_header(&mut self) -> &mut crate::Header {
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
            fn write(&mut self) -> crate::Result<()> {
                self.0.write()
            }

            #[inline]
            fn serialize_changes(&self) -> crate::Result<Vec<u8>> {
                self.0.serialize_changes()
            }

            fn remove(self) -> crate::Result<()> {
                self.0.remove()
            }
        }

        impl<I, T> crate::GenericStoredVec<I, T> for #wrapper<I, T>
        where
            I: crate::VecIndex,
            T: #value_trait,
        {
            #[inline]
            fn unchecked_read_at(
                &self,
                index: usize,
                reader: &::rawdb::Reader,
            ) -> crate::Result<T> {
                self.0.unchecked_read_at(index, reader)
            }

            #[inline(always)]
            fn read_value_from_bytes(&self, bytes: &[u8]) -> crate::Result<T> {
                self.0.read_value_from_bytes(bytes)
            }

            #[inline]
            fn value_to_bytes(&self, value: &T) -> Vec<u8> {
                self.0.value_to_bytes(value)
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
            fn reset(&mut self) -> crate::Result<()> {
                self.0.reset()
            }

            #[inline]
            fn get_stored_value_for_serialization(
                &self,
                index: usize,
                reader: &::rawdb::Reader,
            ) -> crate::Result<T> {
                self.0.get_stored_value_for_serialization(index, reader)
            }

            #[inline]
            fn restore_truncated_value(&mut self, index: usize, value: T) {
                self.0.restore_truncated_value(index, value)
            }

            #[inline]
            fn truncate_if_needed_at(&mut self, index: usize) -> crate::Result<()> {
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
            fn stamped_flush_with_changes(
                &mut self,
                stamp: crate::Stamp,
            ) -> crate::Result<()> {
                self.0.stamped_flush_with_changes(stamp)
            }

            #[inline]
            fn rollback_before(&mut self, stamp: crate::Stamp) -> crate::Result<crate::Stamp> {
                self.0.rollback_before(stamp)
            }

            #[inline]
            fn rollback(&mut self) -> crate::Result<()> {
                self.0.rollback()
            }

            #[inline]
            fn deserialize_then_undo_changes(&mut self, bytes: &[u8]) -> crate::Result<()> {
                self.0.deserialize_then_undo_changes(bytes)
            }
        }

        impl<'a, I, T> IntoIterator for &'a #wrapper<I, T>
        where
            I: crate::VecIndex,
            T: #value_trait,
        {
            type Item = T;
            type IntoIter = #iterator<'a, I, T>;

            fn into_iter(self) -> Self::IntoIter {
                self.iter()
                    .expect(concat!(stringify!(#iterator), "::new(self) to work"))
            }
        }

        impl<I, T> crate::IterableVec<I, T> for #wrapper<I, T>
        where
            I: crate::VecIndex,
            T: #value_trait,
        {
            fn iter(&self) -> crate::BoxedVecIterator<'_, I, T> {
                Box::new(self.into_iter())
            }
        }
    };

    TokenStream::from(expanded)
}
