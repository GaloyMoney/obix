use darling::{FromDeriveInput, FromVariant, ToTokens};
use proc_macro2::TokenStream;
use quote::{TokenStreamExt, quote};

#[derive(Debug, Clone, FromDeriveInput)]
#[darling(attributes(obix))]
pub struct OutboxEvent {
    ident: syn::Ident,
    data: darling::ast::Data<OutboxVariant, ()>,
    #[darling(default = "default_crate_name", rename = "crate")]
    crate_name: syn::LitStr,
}

#[derive(Debug, Clone, FromVariant)]
#[darling(attributes(serde))]
struct OutboxVariant {
    ident: syn::Ident,
    fields: darling::ast::Fields<syn::Type>,
    #[darling(default)]
    other: bool,
}

fn default_crate_name() -> syn::LitStr {
    syn::LitStr::new("obix", proc_macro2::Span::call_site())
}

pub fn derive(ast: syn::DeriveInput) -> darling::Result<proc_macro2::TokenStream> {
    let event = OutboxEvent::from_derive_input(&ast)?;
    Ok(quote!(#event))
}

impl ToTokens for OutboxEvent {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let enum_ident = &self.ident;
        let crate_name: syn::Path = self.crate_name.parse().expect("invalid crate path");

        let variants = match &self.data {
            darling::ast::Data::Enum(variants) => variants,
            _ => panic!("OutboxEvent can only be derived for enums"),
        };

        for variant in variants {
            // Skip variants marked with #[serde(other)]
            if variant.other {
                continue;
            }

            // Only handle tuple variants with exactly one field
            let fields = match &variant.fields.style {
                darling::ast::Style::Tuple => &variant.fields.fields,
                _ => continue,
            };

            if fields.len() != 1 {
                continue;
            }

            let variant_ident = &variant.ident;
            let field_type = &fields[0];

            tokens.append_all(quote! {
                impl #crate_name::out::OutboxEventMarker<#field_type> for #enum_ident {
                    fn as_event(&self) -> Option<&#field_type> {
                        match self {
                            Self::#variant_ident(event) => Some(event),
                            _ => None,
                        }
                    }
                }

                impl From<#field_type> for #enum_ident {
                    fn from(event: #field_type) -> Self {
                        Self::#variant_ident(event)
                    }
                }
            });
        }
    }
}
