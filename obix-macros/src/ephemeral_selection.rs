use darling::{FromDeriveInput, FromVariant};
use proc_macro2::TokenStream;
use quote::quote;

#[derive(Debug, Clone, FromDeriveInput)]
#[darling(attributes(obix), supports(enum_any))]
pub struct EphemeralSelection {
    ident: syn::Ident,
    generics: syn::Generics,
    data: darling::ast::Data<SelectionVariant, ()>,
    #[darling(default = "default_crate_name", rename = "crate")]
    crate_name: syn::LitStr,
}

#[derive(Debug, Clone, FromVariant)]
struct SelectionVariant {
    ident: syn::Ident,
    fields: darling::ast::Fields<syn::Type>,
}

fn default_crate_name() -> syn::LitStr {
    syn::LitStr::new("obix", proc_macro2::Span::call_site())
}

pub fn derive(ast: syn::DeriveInput) -> darling::Result<TokenStream> {
    let selection = EphemeralSelection::from_derive_input(&ast)?;
    let crate_name: syn::Path = selection.crate_name.parse().map_err(|_| {
        darling::Error::custom("`crate` must be a path").with_span(&selection.crate_name)
    })?;
    let members = selection.members()?;
    Ok(selection.expand(&crate_name, &members))
}

struct Member<'a> {
    variant: &'a syn::Ident,
    ty: &'a syn::Type,
}

impl EphemeralSelection {
    fn members(&self) -> darling::Result<Vec<Member<'_>>> {
        let mut errors = darling::Error::accumulator();

        if !self.generics.params.is_empty() {
            errors.push(
                darling::Error::custom("EphemeralSelection cannot be derived for a generic enum")
                    .with_span(&self.generics),
            );
        }

        let variants = match &self.data {
            darling::ast::Data::Enum(variants) => variants,
            darling::ast::Data::Struct(_) => {
                errors.push(
                    darling::Error::custom("EphemeralSelection can only be derived for enums")
                        .with_span(&self.ident),
                );
                return errors.finish_with(Vec::new());
            }
        };

        let mut members = Vec::new();
        for variant in variants {
            match (&variant.fields.style, variant.fields.fields.as_slice()) {
                (darling::ast::Style::Tuple, [ty]) => members.push(Member {
                    variant: &variant.ident,
                    ty,
                }),
                _ => errors.push(
                    darling::Error::custom(
                        "every variant of an EphemeralSelection must wrap exactly one published \
                         type, as a tuple variant: `Variant(PublishedType)`",
                    )
                    .with_span(&variant.ident),
                ),
            }
        }

        errors.finish_with(members)
    }

    fn expand(&self, crate_name: &syn::Path, members: &[Member<'_>]) -> TokenStream {
        let enum_ident = &self.ident;
        let event_types = members.iter().map(|member| {
            let ty = member.ty;
            quote! { <#ty as #crate_name::out::EphemeralEvent>::EVENT_TYPE }
        });
        let decode_arms = members.iter().map(|member| {
            let ty = member.ty;
            let variant = member.variant;
            quote! {
                if *event_type == <#ty as #crate_name::out::EphemeralEvent>::EVENT_TYPE {
                    return Some(
                        #crate_name::prelude::serde_json::from_str(payload.get())
                            .map(Self::#variant),
                    );
                }
            }
        });

        quote! {
            impl #crate_name::out::EphemeralSelection for #enum_ident {
                const EVENT_TYPES: &'static [#crate_name::out::EphemeralEventType] =
                    &[#(#event_types),*];

                fn decode(
                    event_type: &#crate_name::out::EphemeralEventType,
                    payload: &#crate_name::prelude::serde_json::value::RawValue,
                ) -> Option<Result<Self, #crate_name::prelude::serde_json::Error>> {
                    #(#decode_arms)*
                    None
                }
            }
        }
    }
}
