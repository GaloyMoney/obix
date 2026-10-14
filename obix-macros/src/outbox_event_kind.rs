use proc_macro2::TokenStream;
use quote::quote;

pub fn derive(input: &syn::DeriveInput) -> syn::Result<TokenStream> {
    let enum_data = match &input.data {
        syn::Data::Enum(data) => data,
        _ => {
            return Err(syn::Error::new_spanned(
                input,
                "OutboxEventKind can only be derived for enums",
            ));
        }
    };

    let ident = &input.ident;
    let crate_path = parse_crate_path(&input.attrs)?;
    let rename_all = parse_enum_rename_all(&input.attrs)?;

    let variants = enum_data
        .variants
        .iter()
        .map(|variant| parse_variant(variant, rename_all))
        .collect::<syn::Result<Vec<_>>>()?;

    let ephemeral_tags = variants
        .iter()
        .filter(|variant| variant.ephemeral)
        .map(|variant| variant.tag.clone());

    let is_ephemeral_arms = variants.iter().map(|variant| {
        let variant_ident = &variant.ident;
        let ephemeral = variant.ephemeral;
        match &variant.fields {
            syn::Fields::Unnamed(fields) if fields.unnamed.len() == 1 && !ephemeral => {
                quote! {
                    Self::#variant_ident(inner) =>
                        #crate_path::out::OutboxEventKind::is_ephemeral(inner),
                }
            }
            syn::Fields::Unnamed(fields) if fields.unnamed.len() == 1 => quote! {
                Self::#variant_ident(_) => true,
            },
            syn::Fields::Named(_) => quote! {
                Self::#variant_ident { .. } => #ephemeral,
            },
            syn::Fields::Unnamed(_) => quote! {
                Self::#variant_ident(..) => #ephemeral,
            },
            syn::Fields::Unit => quote! {
                Self::#variant_ident => #ephemeral,
            },
        }
    });

    let event_type_pushes = variants.iter().map(|variant| {
        let tag = &variant.tag;
        if variant.ephemeral {
            quote! {
                out.push((#tag, "*"));
            }
        } else if let syn::Fields::Unnamed(fields) = &variant.fields
            && fields.unnamed.len() == 1
        {
            let inner_type = &fields.unnamed[0].ty;
            quote! {
                for inner_tag in <#inner_type as #crate_path::out::OutboxEventKind>::EPHEMERAL_VARIANTS {
                    out.push((#tag, *inner_tag));
                }
            }
        } else {
            quote! {}
        }
    });

    Ok(quote! {
        #[automatically_derived]
        impl #crate_path::out::OutboxEventKind for #ident {
            const EPHEMERAL_VARIANTS: &'static [&'static str] = &[#(#ephemeral_tags),*];

            fn is_ephemeral(&self) -> bool {
                match self {
                    #(#is_ephemeral_arms)*
                }
            }

            fn ephemeral_event_types() -> ::std::vec::Vec<(&'static str, &'static str)> {
                let mut out = ::std::vec::Vec::new();
                #(#event_type_pushes)*
                out
            }
        }
    })
}

struct VariantInfo {
    ident: syn::Ident,
    /// The value serde writes for this variant's internal tag: the variant
    /// name, after `#[serde(rename)]` / `rename_all`.
    tag: String,
    ephemeral: bool,
    fields: syn::Fields,
}

fn parse_variant(
    variant: &syn::Variant,
    rename_all: Option<RenameRule>,
) -> syn::Result<VariantInfo> {
    let mut ephemeral = false;
    let mut rename = None;
    let mut is_other = false;

    for attr in &variant.attrs {
        if attr.path().is_ident("obix") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("ephemeral") {
                    ephemeral = true;
                    Ok(())
                } else {
                    Err(meta.error("unknown obix attribute; expected `ephemeral`"))
                }
            })?;
        } else if attr.path().is_ident("serde") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("rename") {
                    rename = Some(meta.value()?.parse::<syn::LitStr>()?);
                    Ok(())
                } else {
                    if meta.path.is_ident("other") {
                        is_other = true;
                    }
                    // Every other serde attribute is irrelevant to
                    // classification; serde's own derive validates them.
                    skip_value(&meta)
                }
            })?;
        }
    }

    if is_other && ephemeral {
        return Err(syn::Error::new_spanned(
            variant,
            "a #[serde(other)] catch-all variant cannot be marked #[obix(ephemeral)]",
        ));
    }

    let ident = variant.ident.clone();
    let tag = match (&rename, rename_all) {
        (Some(lit), _) => lit.value(),
        (None, Some(rule)) => rule.apply(&ident.to_string()),
        (None, None) => ident.to_string(),
    };

    Ok(VariantInfo {
        ident,
        tag,
        ephemeral,
        fields: variant.fields.clone(),
    })
}

/// Consume an ignored meta item's `= <value>` so `parse_nested_meta` can
/// continue past it. The value grammar here is a single literal or path —
/// one token tree — as in Rust's structured-attribute convention.
fn skip_value(meta: &syn::meta::ParseNestedMeta) -> syn::Result<()> {
    if meta.input.peek(syn::Token![=]) {
        meta.value()?.parse::<proc_macro2::TokenTree>()?;
    }
    Ok(())
}

fn parse_crate_path(attrs: &[syn::Attribute]) -> syn::Result<syn::Path> {
    let mut crate_path = None;
    for attr in attrs {
        if !attr.path().is_ident("obix") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("crate") {
                crate_path = Some(meta.value()?.parse::<syn::LitStr>()?);
                Ok(())
            } else {
                Err(meta.error(
                    "unknown obix attribute on the enum; `ephemeral` marks variants, \
                     `crate` may be set here",
                ))
            }
        })?;
    }
    match crate_path {
        Some(lit) => lit.parse().map_err(|_| {
            syn::Error::new_spanned(&lit, format!("`{}` is not a valid crate path", lit.value()))
        }),
        None => Ok(syn::Path::from(syn::Ident::new(
            "obix",
            proc_macro2::Span::call_site(),
        ))),
    }
}

fn parse_enum_rename_all(attrs: &[syn::Attribute]) -> syn::Result<Option<RenameRule>> {
    let mut rename_all = None;
    for attr in attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename_all") {
                let lit = meta.value()?.parse::<syn::LitStr>()?;
                let rule = RenameRule::parse(&lit.value(), lit.span())?;
                if rename_all.replace(rule).is_some() {
                    return Err(meta.error("duplicate serde rename_all"));
                }
                Ok(())
            } else {
                skip_value(&meta)
            }
        })?;
    }
    Ok(rename_all)
}

/// serde's `rename_all` rules, as serde applies them to *variant* names
/// (a faithful port of `serde_derive`'s internals `RenameRule::apply_to_variant`),
/// so the tags line up with what serde — and schema generators honoring
/// serde attributes — put on the wire.
#[allow(clippy::enum_variant_names)] // mirrors serde's rule names verbatim
#[derive(Copy, Clone)]
enum RenameRule {
    LowerCase,
    UpperCase,
    PascalCase,
    CamelCase,
    SnakeCase,
    ScreamingSnakeCase,
    KebabCase,
    ScreamingKebabCase,
}

impl RenameRule {
    fn parse(value: &str, span: proc_macro2::Span) -> syn::Result<Self> {
        let rule = match value {
            "lowercase" => Self::LowerCase,
            "UPPERCASE" => Self::UpperCase,
            "PascalCase" => Self::PascalCase,
            "camelCase" => Self::CamelCase,
            "snake_case" => Self::SnakeCase,
            "SCREAMING_SNAKE_CASE" => Self::ScreamingSnakeCase,
            "kebab-case" => Self::KebabCase,
            "SCREAMING-KEBAB-CASE" => Self::ScreamingKebabCase,
            other => {
                return Err(syn::Error::new(
                    span,
                    format!("unknown serde rename_all rule `{other}`"),
                ));
            }
        };
        Ok(rule)
    }

    fn apply(&self, variant: &str) -> String {
        match self {
            Self::PascalCase => variant.to_owned(),
            Self::LowerCase => variant.to_ascii_lowercase(),
            Self::UpperCase => variant.to_ascii_uppercase(),
            Self::CamelCase => {
                let mut chars = variant.chars();
                match chars.next() {
                    Some(first) => first.to_ascii_lowercase().to_string() + chars.as_str(),
                    None => String::new(),
                }
            }
            Self::SnakeCase => {
                let mut snake = String::new();
                for (i, ch) in variant.char_indices() {
                    if i > 0 && ch.is_uppercase() {
                        snake.push('_');
                    }
                    snake.push(ch.to_ascii_lowercase());
                }
                snake
            }
            Self::ScreamingSnakeCase => Self::SnakeCase.apply(variant).to_ascii_uppercase(),
            Self::KebabCase => Self::SnakeCase.apply(variant).replace('_', "-"),
            Self::ScreamingKebabCase => Self::ScreamingSnakeCase.apply(variant).replace('_', "-"),
        }
    }
}
