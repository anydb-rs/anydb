use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DataStruct, DeriveInput, Fields, parse_macro_input};

#[proc_macro_derive(Bytes)]
pub fn derive_bytes(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = &input.ident;
    let generics = &input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let inner_type = match &input.data {
        Data::Struct(DataStruct {
            fields: Fields::Unnamed(fields),
            ..
        }) if fields.unnamed.len() == 1 => &fields.unnamed[0].ty,
        _ => {
            return syn::Error::new_spanned(
                &input.ident,
                "Bytes can only be derived for single-field tuple structs",
            )
            .to_compile_error()
            .into();
        }
    };

    let expanded = quote! {
        impl #impl_generics ::vecdb::Bytes for #struct_name #ty_generics #where_clause {
            fn to_bytes(&self) -> Vec<u8> {
                self.0.to_bytes()
            }

            fn from_bytes(bytes: &[u8]) -> ::vecdb::Result<Self> {
                Ok(Self(#inner_type::from_bytes(bytes)?))
            }
        }
    };

    TokenStream::from(expanded)
}

#[proc_macro_derive(Pco)]
pub fn derive_pco(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = &input.ident;
    let generics = &input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let inner_type = match &input.data {
        Data::Struct(DataStruct {
            fields: Fields::Unnamed(fields),
            ..
        }) if fields.unnamed.len() == 1 => &fields.unnamed[0].ty,
        _ => {
            return syn::Error::new_spanned(
                &input.ident,
                "PcoVecValue can only be derived for single-field tuple structs",
            )
            .to_compile_error()
            .into();
        }
    };

    // Check if we have generic parameters
    let has_generics = !generics.params.is_empty();

    let expanded = if has_generics {
        let where_clause = if where_clause.is_some() {
            quote! { #where_clause #inner_type: ::vecdb::Pco, }
        } else {
            quote! { where #inner_type: ::vecdb::Pco, }
        };

        quote! {
            impl #impl_generics ::vecdb::Bytes for #struct_name #ty_generics #where_clause {
                fn to_bytes(&self) -> Vec<u8> {
                    self.0.to_bytes()
                }

                fn from_bytes(bytes: &[u8]) -> ::vecdb::Result<Self> {
                    Ok(Self(#inner_type::from_bytes(bytes)?))
                }
            }

            impl #impl_generics ::vecdb::TransparentPco<<#inner_type as ::vecdb::Pco>::NumberType> for #struct_name #ty_generics #where_clause {}

            impl #impl_generics ::vecdb::Pco for #struct_name #ty_generics #where_clause {
                type NumberType = <#inner_type as ::vecdb::Pco>::NumberType;
            }
        }
    } else {
        quote! {
            impl ::vecdb::Bytes for #struct_name {
                fn to_bytes(&self) -> Vec<u8> {
                    self.0.to_bytes()
                }

                fn from_bytes(bytes: &[u8]) -> ::vecdb::Result<Self> {
                    Ok(Self(#inner_type::from_bytes(bytes)?))
                }
            }

            impl ::vecdb::TransparentPco<<#inner_type as ::vecdb::Pco>::NumberType> for #struct_name {}

            impl ::vecdb::Pco for #struct_name {
                type NumberType = <#inner_type as ::vecdb::Pco>::NumberType;
            }
        }
    };

    TokenStream::from(expanded)
}
