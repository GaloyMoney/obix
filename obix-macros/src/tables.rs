use darling::{FromDeriveInput, ToTokens};
use proc_macro2::TokenStream;
use quote::{TokenStreamExt, quote};

#[derive(Debug, Clone, FromDeriveInput)]
#[darling(attributes(obix))]
pub struct MailboxTables {
    ident: syn::Ident,
}

pub fn derive(ast: syn::DeriveInput) -> darling::Result<proc_macro2::TokenStream> {
    let tables = MailboxTables::from_derive_input(&ast)?;
    Ok(quote!(#tables))
}

impl ToTokens for MailboxTables {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let ident = &self.ident;
        tokens.append_all(quote! {
            impl obix::MailboxTables for #ident {
            }
        });
    }
}
