//! `#[derive(Crdt)]` — generate a recrr `TableSpec` and typed column references
//! from a struct, so the schema is single-sourced from the type and column
//! names are compile-checked.
//!
//! This crate is an implementation detail of `recrr`; use it through the
//! re-export `recrr::Crdt`, not directly.
//!
//! # Example
//!
//! ```ignore
//! use recrr::Crdt;
//!
//! #[derive(Crdt)]
//! #[crdt(table = "papers")]
//! struct Paper {
//!     #[crdt(pk)]
//!     id: String,
//!     title: String,
//!     #[crdt(skeleton = "\"[]\"")]
//!     authors: String,
//!     #[crdt(rename = "is_favorite")]
//!     favorite: bool,
//!     #[crdt(skip)]
//!     local_cache: Option<String>,
//! }
//! ```
//!
//! generates `impl recrr::CrdtTable for Paper`, the column constants
//! `Paper::TITLE` / `Paper::AUTHORS` / `Paper::FAVORITE` (honoring `rename`) /
//! `Paper::ID`, and `Paper::ALL` (every tracked column, in declaration order).

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parse_macro_input, spanned::Spanned, Data, DeriveInput, Expr, ExprLit, Fields, Lit, LitStr,
    Meta, Token,
};

/// Derive `recrr::CrdtTable` plus typed column constants for a struct.
///
/// See the crate-level docs and the recrr README for the attribute grammar.
#[proc_macro_derive(Crdt, attributes(crdt))]
pub fn derive_crdt(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// One tracked, non-pk column: its DB name and the `SCREAMING_SNAKE` const name.
struct Column {
    /// The DB column name (field name, or the `rename` target).
    db_name: String,
    /// The associated-const identifier, e.g. `TITLE`.
    const_ident: syn::Ident,
    /// The `SkeletonValue` construction expression, if a `skeleton` was given.
    skeleton: Option<proc_macro2::TokenStream>,
}

/// The two shapes a primary key can take, resolved from the attributes.
enum Pk {
    /// A single-column key from a `#[crdt(pk)]` field. Carries the DB column name
    /// and the const identifier so the pk also gets a typed constant.
    Single {
        db_name: String,
        const_ident: syn::Ident,
    },
    /// A composite key declared on the container: `pk = (a, b; sep = ':')`.
    Composite { a: String, b: String, sep: char },
}

fn expand(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let ident = &input.ident;

    if !input.generics.params.is_empty() {
        return Err(syn::Error::new(
            input.generics.span(),
            "#[derive(Crdt)] does not support generic types",
        ));
    }

    let Container { table, composite } = parse_container(&input)?;

    let fields = match &input.data {
        Data::Struct(s) => match &s.fields {
            Fields::Named(named) => &named.named,
            _ => {
                return Err(syn::Error::new(
                    input.span(),
                    "#[derive(Crdt)] requires a struct with named fields",
                ))
            }
        },
        _ => {
            return Err(syn::Error::new(
                input.span(),
                "#[derive(Crdt)] can only be applied to structs",
            ))
        }
    };

    let mut columns: Vec<Column> = Vec::new();
    let mut field_pk: Option<Pk> = None;

    // A composite key names its columns on the container; fields matching those
    // names are key columns, not tracked columns, so exclude them silently.
    let composite_keys: Vec<String> = composite
        .as_ref()
        .map(|(a, b, _, _)| vec![a.clone(), b.clone()])
        .unwrap_or_default();

    for field in fields {
        let attrs = parse_field_attrs(field)?;
        let field_name = field.ident.as_ref().expect("named field").to_string();

        if attrs.skip {
            if attrs.rename.is_some() || attrs.skeleton.is_some() {
                return Err(syn::Error::new(
                    field.span(),
                    "#[crdt(skip)] cannot be combined with rename or skeleton",
                ));
            }
            continue;
        }

        if attrs.is_pk {
            if attrs.skeleton.is_some() {
                return Err(syn::Error::new(
                    field.span(),
                    "a #[crdt(pk)] field cannot also have a skeleton default",
                ));
            }
            if field_pk.is_some() {
                return Err(syn::Error::new(
                    field.span(),
                    "only one #[crdt(pk)] field is allowed",
                ));
            }
            // The const is named after the Rust field; the value honors rename.
            let const_ident = screaming_snake(&field_name, field.span());
            let db_name = attrs.rename.unwrap_or(field_name);
            field_pk = Some(Pk::Single {
                const_ident,
                db_name,
            });
            continue;
        }

        // Composite key columns are keys, not tracked columns. Match on the DB
        // name (rename-aware) so `pk = (a, b)` lines up with the real columns.
        let db_name = attrs.rename.clone().unwrap_or_else(|| field_name.clone());
        if composite_keys.iter().any(|k| k == &db_name) {
            if attrs.skeleton.is_some() {
                return Err(syn::Error::new(
                    field.span(),
                    "a composite key column cannot also have a skeleton default",
                ));
            }
            continue;
        }

        // Const named after the Rust field; value is the (possibly renamed) column.
        let const_ident = screaming_snake(&field_name, field.span());
        columns.push(Column {
            skeleton: attrs.skeleton,
            const_ident,
            db_name,
        });
    }

    // Resolve the primary key: exactly one of field-pk / container-composite.
    let pk = match (field_pk, composite) {
        (Some(_), Some((_, _, _, span))) => {
            return Err(syn::Error::new(
                span,
                "a struct cannot have both a #[crdt(pk)] field and a composite pk = (...)",
            ))
        }
        (Some(pk), None) => pk,
        (None, Some((a, b, sep, _span))) => Pk::Composite { a, b, sep },
        (None, None) => {
            return Err(syn::Error::new(
                ident.span(),
                "a Crdt struct needs a primary key: mark a field with #[crdt(pk)] \
                 or declare a composite key with #[crdt(table = \"...\", pk = (a, b; sep = ':'))]",
            ))
        }
    };

    Ok(codegen(ident, &table, &pk, &columns))
}

/// Build the `impl CrdtTable` + column constants token stream.
fn codegen(
    ident: &syn::Ident,
    table: &str,
    pk: &Pk,
    columns: &[Column],
) -> proc_macro2::TokenStream {
    let col_names: Vec<&str> = columns.iter().map(|c| c.db_name.as_str()).collect();

    // TableSpec::new(table, [cols...]) .with_pk(...) .with_skeleton([...])
    let with_pk = match pk {
        Pk::Single { db_name, .. } => quote! {
            .with_pk(recrr::PkSpec::single(#db_name))
        },
        Pk::Composite { a, b, sep } => quote! {
            .with_pk(recrr::PkSpec::composite(#a, #b, #sep))
        },
    };

    let skeleton_entries: Vec<proc_macro2::TokenStream> = columns
        .iter()
        .filter_map(|c| {
            let name = &c.db_name;
            c.skeleton.as_ref().map(|sk| quote! { (#name, #sk) })
        })
        .collect();
    let with_skeleton = if skeleton_entries.is_empty() {
        quote! {}
    } else {
        quote! { .with_skeleton([ #(#skeleton_entries),* ]) }
    };

    // Column constants (tracked columns + the pk, if single).
    let mut const_defs: Vec<proc_macro2::TokenStream> = columns
        .iter()
        .map(|c| {
            let id = &c.const_ident;
            let name = &c.db_name;
            quote! { pub const #id: &'static str = #name; }
        })
        .collect();
    if let Pk::Single {
        db_name,
        const_ident,
    } = pk
    {
        const_defs.push(quote! { pub const #const_ident: &'static str = #db_name; });
    }

    let all_consts: Vec<&syn::Ident> = columns.iter().map(|c| &c.const_ident).collect();

    quote! {
        impl recrr::CrdtTable for #ident {
            const TABLE: &'static str = #table;
            fn table_spec() -> recrr::TableSpec {
                recrr::TableSpec::new(#table, [ #(#col_names),* ])
                    #with_pk
                    #with_skeleton
            }
        }

        impl #ident {
            #(#const_defs)*
            /// Every tracked column, in declaration order — pass to
            /// [`track_insert`](recrr::Crr::track_insert).
            pub const ALL: &'static [&'static str] = &[ #(Self::#all_consts),* ];
        }
    }
}

// --- attribute parsing -------------------------------------------------------

/// The resolved container attributes: the (required) table name and an optional
/// composite pk `(a, b, sep, span)`.
struct Container {
    table: String,
    composite: Option<(String, String, char, proc_macro2::Span)>,
}

/// Parse all `#[crdt(...)]` container arguments in one pass.
///
/// Hand-written rather than via `parse_nested_meta`, because the composite-pk
/// form `pk = (a, b; sep = ':')` contains a `;` that `parse_nested_meta` cannot
/// model.
fn parse_container(input: &DeriveInput) -> syn::Result<Container> {
    let mut table: Option<String> = None;
    let mut composite: Option<(String, String, char, proc_macro2::Span)> = None;

    for attr in &input.attrs {
        if !attr.path().is_ident("crdt") {
            continue;
        }
        let Meta::List(list) = &attr.meta else {
            continue;
        };
        let parsed = list
            .parse_args_with(syn::punctuated::Punctuated::<CrdtArg, Token![,]>::parse_terminated)?;
        for arg in parsed {
            match arg {
                CrdtArg::Table(t) => table = Some(t),
                CrdtArg::CompositePk { a, b, sep, span } => {
                    composite = Some((a, b, sep, span));
                }
            }
        }
    }

    let table = table.ok_or_else(|| {
        syn::Error::new(
            input.span(),
            "#[derive(Crdt)] requires #[crdt(table = \"...\")] on the struct",
        )
    })?;
    Ok(Container { table, composite })
}

/// One comma-separated container argument: `table = "..."` or the composite pk.
enum CrdtArg {
    Table(String),
    CompositePk {
        a: String,
        b: String,
        sep: char,
        span: proc_macro2::Span,
    },
}

impl syn::parse::Parse for CrdtArg {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let key: syn::Ident = input.parse()?;
        if key == "table" {
            let _: Token![=] = input.parse()?;
            let s: LitStr = input.parse()?;
            return Ok(CrdtArg::Table(s.value()));
        }
        if key == "pk" {
            let _: Token![=] = input.parse()?;
            let content;
            let paren = syn::parenthesized!(content in input);
            // (a, b; sep = ':')
            let a: syn::Ident = content.parse()?;
            let _: Token![,] = content.parse()?;
            let b: syn::Ident = content.parse()?;
            let mut sep = ':';
            if content.peek(Token![;]) {
                let _: Token![;] = content.parse()?;
                let sep_key: syn::Ident = content.parse()?;
                if sep_key != "sep" {
                    return Err(syn::Error::new(
                        sep_key.span(),
                        "expected `sep = '<char>'` in composite pk",
                    ));
                }
                let _: Token![=] = content.parse()?;
                let ch: syn::LitChar = content.parse()?;
                sep = ch.value();
            }
            return Ok(CrdtArg::CompositePk {
                a: a.to_string(),
                b: b.to_string(),
                sep,
                span: paren.span.join(),
            });
        }
        Err(syn::Error::new(
            key.span(),
            format!("unknown crdt(...) key `{key}` on the struct; expected `table` or `pk`"),
        ))
    }
}

/// The parsed field-level `#[crdt(...)]` attributes.
#[derive(Default)]
struct FieldAttrs {
    is_pk: bool,
    skip: bool,
    rename: Option<String>,
    skeleton: Option<proc_macro2::TokenStream>,
}

fn parse_field_attrs(field: &syn::Field) -> syn::Result<FieldAttrs> {
    let mut out = FieldAttrs::default();
    for attr in &field.attrs {
        if !attr.path().is_ident("crdt") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("pk") {
                out.is_pk = true;
                Ok(())
            } else if meta.path.is_ident("skip") {
                out.skip = true;
                Ok(())
            } else if meta.path.is_ident("rename") {
                let s: LitStr = meta.value()?.parse()?;
                out.rename = Some(s.value());
                Ok(())
            } else if meta.path.is_ident("skeleton") {
                let expr: Expr = meta.value()?.parse()?;
                out.skeleton = Some(skeleton_tokens(&expr)?);
                Ok(())
            } else {
                Err(meta.error(format!(
                    "unknown crdt(...) key `{}` on this field; expected \
                     `pk`, `skip`, `rename`, or `skeleton`",
                    path_ident(&meta.path)
                )))
            }
        })?;
    }
    Ok(out)
}

/// Map a `skeleton = <expr>` value to a `SkeletonValue` construction.
///
/// Accepts the keyword `now_rfc3339` (→ `SkeletonValue::NowRfc3339`) and literals
/// (string / integer / bool → `SkeletonValue::Literal(Value::…)`).
fn skeleton_tokens(expr: &Expr) -> syn::Result<proc_macro2::TokenStream> {
    // Keyword form: skeleton = now_rfc3339
    if let Expr::Path(p) = expr {
        if p.path.is_ident("now_rfc3339") {
            return Ok(quote! { recrr::SkeletonValue::NowRfc3339 });
        }
        return Err(syn::Error::new(
            expr.span(),
            "unknown skeleton keyword; expected `now_rfc3339` or a literal",
        ));
    }

    if let Expr::Lit(ExprLit { lit, .. }) = expr {
        let value = match lit {
            Lit::Str(s) => quote! { recrr::Value::Text(#s.to_string()) },
            Lit::Int(i) => quote! { recrr::Value::Integer(#i) },
            Lit::Bool(b) => quote! { recrr::Value::Integer(#b as i64) },
            Lit::Float(f) => quote! { recrr::Value::Real(#f) },
            _ => {
                return Err(syn::Error::new(
                    lit.span(),
                    "unsupported skeleton literal; use a string, integer, float, or bool",
                ))
            }
        };
        return Ok(quote! { recrr::SkeletonValue::Literal(#value) });
    }

    Err(syn::Error::new(
        expr.span(),
        "skeleton must be a literal (string / integer / float / bool) or `now_rfc3339`",
    ))
}

// --- small helpers -----------------------------------------------------------

/// Convert a column name to a `SCREAMING_SNAKE_CASE` const identifier.
fn screaming_snake(name: &str, span: proc_macro2::Span) -> syn::Ident {
    let upper: String = name
        .chars()
        .map(|c| {
            if c == '-' {
                '_'
            } else {
                c.to_ascii_uppercase()
            }
        })
        .collect();
    syn::Ident::new(&upper, span)
}

/// The last path segment as a string, for diagnostics.
fn path_ident(path: &syn::Path) -> String {
    path.segments
        .last()
        .map(|s| s.ident.to_string())
        .unwrap_or_default()
}
