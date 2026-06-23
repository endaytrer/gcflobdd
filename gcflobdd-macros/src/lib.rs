//! Procedural macro backing the `declare_grammar! { ... }` DSL.
//!
//! For each rule `Lhs => T0, T1, ..., Tn;` the macro emits:
//!   * a marker `struct Lhs;` (a *named* nominal type, so its trait impls below
//!     are local to the caller's crate and don't fall foul of the orphan rule)
//!   * `impl GhddGrammar for Lhs` (`NUM_VARS` summed over the components)
//!   * `impl RecursiveGrammar for Lhs` (`Connection` = the wrapper for `[T0..Tn]`)
//!
//! ## Connection wrappers
//!
//! A grammar's connection diagram is a generic `gcflobdd` type
//! (`Connection1<T>` / `ConnectionK<T, C>`), and every suffix of the component
//! list is itself such a type that needs its own interning table (`CONN_TABLE`).
//!
//! That table must be a `static`, and Rust has no generic statics — so it has
//! to be attached to a concrete type. But the connection types are defined in
//! `gcflobdd`, and a downstream crate may neither write an inherent `impl` on
//! them (E0116) nor a foreign-trait `impl` for them (E0117) — they are foreign
//! and not `#[fundamental]`.
//!
//! So for each *distinct* connection type we emit a **local** newtype
//! `__GcflobddConn{N}` that wraps the corresponding `gcflobdd` connection type
//! (with its children pointing at the *child* wrappers, so every level owns a
//! table), implements `Connection` by delegating to the wrapped type, and
//! carries its own inherent `CONN_TABLE`. Because the wrapper is local, all of
//! that is legal in any crate.
//!
//! Distinct connection types are deduplicated by their component-list suffix —
//! grammars frequently share suffixes (`G2 => G1, Unit` and `G3 => G1, G1, Unit`
//! both contain `G1, Unit`) — so each wrapper is emitted exactly once. This is
//! something `macro_rules!` cannot do, since it cannot compare two `:ty`s.

use std::collections::HashMap;

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::{
    Ident, Token, Type,
    parse::{Parse, ParseStream},
    parse_macro_input, parse_quote,
    punctuated::Punctuated,
};

/// A single `Lhs => T0, T1, ..., Tn;` rule.
struct Rule {
    lhs: Ident,
    components: Vec<Type>,
}

/// The whole `declare_grammar! { ... }` block: a sequence of rules.
struct Grammars {
    rules: Vec<Rule>,
}

impl Parse for Grammars {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut rules = Vec::new();
        while !input.is_empty() {
            let lhs: Ident = input.parse()?;
            input.parse::<Token![=>]>()?;
            let components = Punctuated::<Type, Token![,]>::parse_separated_nonempty(input)?;
            input.parse::<Token![;]>()?;
            rules.push(Rule {
                lhs,
                components: components.into_iter().collect(),
            });
        }
        Ok(Grammars { rules })
    }
}

/// Canonical dedup key for a component-list suffix. Two suffixes denote the
/// same connection type iff their component lists are equal.
fn components_key(components: &[Type]) -> String {
    components
        .iter()
        .map(|ty| quote!(#ty).to_string())
        .collect::<Vec<_>>()
        .join(",")
}

#[proc_macro]
pub fn declare_grammar(input: TokenStream) -> TokenStream {
    let Grammars { rules } = parse_macro_input!(input as Grammars);

    // Pass 1: assign one wrapper type to each distinct connection type (= each
    // distinct component-list suffix), deduplicating shared suffixes.
    let mut wrapper_of: HashMap<String, Ident> = HashMap::new();
    let mut distinct: Vec<Vec<Type>> = Vec::new();
    for rule in &rules {
        for start in 0..rule.components.len() {
            let suffix = &rule.components[start..];
            let key = components_key(suffix);
            if !wrapper_of.contains_key(&key) {
                let ident = Ident::new(
                    &format!("__GcflobddConn{}", wrapper_of.len()),
                    Span::call_site(),
                );
                wrapper_of.insert(key, ident);
                distinct.push(suffix.to_vec());
            }
        }
    }
    let wrapper_ident =
        |components: &[Type]| -> Ident { wrapper_of[&components_key(components)].clone() };

    // One grouping wrapper per declared grammar: its grouping is
    // `RecursiveGrouping<G>`, a foreign generic type that can't own a table
    // downstream, so we wrap it in a local type that does.
    let group_wrapper: Vec<Ident> = (0..rules.len())
        .map(|i| Ident::new(&format!("__GcflobddGroup{}", i), Span::call_site()))
        .collect();

    // Pass 2: grammar marker types and their trait impls.
    let mut grammar_items = Vec::new();
    for (gi, rule) in rules.iter().enumerate() {
        let lhs = &rule.lhs;
        let components = &rule.components;

        // const NUM_VARS = <T0>::NUM_VARS + <T1>::NUM_VARS + ...
        let mut num_vars = quote!();
        for (i, ty) in components.iter().enumerate() {
            if i == 0 {
                num_vars = quote!(<#ty>::NUM_VARS);
            } else {
                num_vars = quote!(#num_vars + <#ty>::NUM_VARS);
            }
        }

        let connection = wrapper_ident(components);
        let grouping = &group_wrapper[gi];

        grammar_items.push(quote! {
            pub struct #lhs;

            impl gcflobdd::grammar::GhddGrammar for #lhs {
                const NUM_VARS: usize = #num_vars;
                type Grouping = #grouping;
            }

            impl gcflobdd::grammar::RecursiveGrammar for #lhs {
                type Connection = #connection;
            }
        });
    }

    // Pass 2b: the grouping wrappers themselves (one per grammar).
    for (gi, rule) in rules.iter().enumerate() {
        let lhs = &rule.lhs;
        let ident = &group_wrapper[gi];
        grammar_items.push(quote! {
            #[derive(Hash, PartialEq, Eq)]
            pub struct #ident(gcflobdd::gcflobdd::grouping::RecursiveGrouping<#lhs>);

            impl gcflobdd::gcflobdd::grouping::Grouping for #ident {
                type Grammar = #lhs;
                fn group_table() -> &'static ::std::thread::LocalKey<
                    ::std::cell::RefCell<gcflobdd::utils::HashSet<gcflobdd::utils::hash_cache::Rch<Self>>>,
                > {
                    &Self::GROUP_TABLE
                }
                fn mk_no_distinction() -> gcflobdd::utils::hash_cache::Rch<Self> {
                    <Self as gcflobdd::gcflobdd::grouping::Grouping>::intern(#ident(
                        gcflobdd::gcflobdd::grouping::RecursiveGrouping::<#lhs>::mk_no_distinction(),
                    ))
                }
                fn mk_distinction(x: usize) -> gcflobdd::utils::hash_cache::Rch<Self> {
                    <Self as gcflobdd::gcflobdd::grouping::Grouping>::intern(#ident(
                        gcflobdd::gcflobdd::grouping::RecursiveGrouping::<#lhs>::mk_distinction(x),
                    ))
                }
                fn num_exits(&self) -> usize {
                    self.0.num_exits()
                }
                fn pair_product_inner(
                    lhs: &gcflobdd::utils::hash_cache::Rch<Self>,
                    rhs: &gcflobdd::utils::hash_cache::Rch<Self>,
                ) -> (Self, ::std::vec::Vec<(usize, usize)>) {
                    let (rg, map) =
                        gcflobdd::gcflobdd::grouping::RecursiveGrouping::<#lhs>::pair_product(
                            &lhs.0, &rhs.0,
                        );
                    (#ident(rg), map)
                }
                fn reduce_inner(
                    this: &gcflobdd::utils::hash_cache::Rch<Self>,
                    reduce_map: &[usize],
                    num_exits: usize,
                ) -> Self {
                    #ident(gcflobdd::gcflobdd::grouping::RecursiveGrouping::<#lhs>::reduce(
                        &this.0, reduce_map, num_exits,
                    ))
                }
            }

            impl #ident {
                thread_local! {
                    static GROUP_TABLE: ::std::cell::RefCell<
                        gcflobdd::utils::HashSet<gcflobdd::utils::hash_cache::Rch<#ident>>,
                    > = ::std::cell::RefCell::new(gcflobdd::utils::HashSet::default());
                }
            }

            gcflobdd::__gcflobdd_op_cache_storage!(#ident);
        });
    }

    // Pass 3: one local connection wrapper per distinct connection type, each
    // owning its own CONN_TABLE.
    let mut wrapper_items = Vec::new();
    for components in &distinct {
        let ident = wrapper_ident(components);
        let (head, rest) = components.split_first().expect("suffix is non-empty");

        // The wrapped `gcflobdd` connection type. Its child (if any) is the
        // *child wrapper*, so every nesting level is a table-owning local type.
        let inner: Type = if rest.is_empty() {
            parse_quote!(gcflobdd::gcflobdd::connection::Connection1<#head>)
        } else {
            let child = wrapper_ident(rest);
            parse_quote!(gcflobdd::gcflobdd::connection::ConnectionK<#head, #child>)
        };

        wrapper_items.push(quote! {
            #[derive(Hash, PartialEq, Eq)]
            pub struct #ident(gcflobdd::utils::hash_cache::Rch<#inner>);

            impl gcflobdd::gcflobdd::connection::Connection for #ident {
                fn conn_table() -> &'static ::std::thread::LocalKey<
                    ::std::cell::RefCell<gcflobdd::utils::HashSet<gcflobdd::utils::hash_cache::Rch<Self>>>,
                > {
                    &Self::CONN_TABLE
                }
                fn mk_distinction(x: usize) -> gcflobdd::utils::hash_cache::Rch<Self> {
                    <Self as gcflobdd::gcflobdd::connection::Connection>::intern(#ident(
                        <#inner>::mk_distinction(x),
                    ))
                }
                fn mk_no_distinction() -> gcflobdd::utils::hash_cache::Rch<Self> {
                    <Self as gcflobdd::gcflobdd::connection::Connection>::intern(#ident(
                        <#inner>::mk_no_distinction(),
                    ))
                }
                fn num_exits(&self) -> usize {
                    self.0.num_exits()
                }
                fn pair_product_inner(
                    lhs: &gcflobdd::utils::hash_cache::Rch<Self>,
                    rhs: &gcflobdd::utils::hash_cache::Rch<Self>,
                ) -> (Self, ::std::vec::Vec<(usize, usize)>) {
                    let (v, map) = <#inner>::pair_product(&lhs.0, &rhs.0);
                    (
                        #ident(::std::rc::Rc::new(gcflobdd::utils::hash_cache::HashCached::new(v))),
                        map,
                    )
                }
                fn reduce_inner(
                    this: &gcflobdd::utils::hash_cache::Rch<Self>,
                    reduce_map: &[usize],
                    num_exits: usize,
                ) -> Self {
                    #ident(::std::rc::Rc::new(gcflobdd::utils::hash_cache::HashCached::new(
                        <#inner>::reduce(&this.0, reduce_map, num_exits),
                    )))
                }
            }

            impl #ident {
                thread_local! {
                    static CONN_TABLE: ::std::cell::RefCell<
                        gcflobdd::utils::HashSet<gcflobdd::utils::hash_cache::Rch<#ident>>,
                    > = ::std::cell::RefCell::new(gcflobdd::utils::HashSet::default());
                }
            }

            gcflobdd::__gcflobdd_op_cache_storage!(#ident);
        });
    }

    quote! {
        #( #grammar_items )*
        #( #wrapper_items )*
    }
    .into()
}
