//! GCFLOBDD with a compile-time grammar.
//!
//! A grammar is declared with the [`declare_grammar!`] macro; each grammar
//! level becomes a distinct, statically-typed node type that hash-conses itself
//! into its own thread-local interning table.

pub mod gcflobdd;
pub mod grammar;
pub mod utils;

pub use gcflobdd_macros::declare_grammar;

#[cfg(feature = "fx-hash")]
extern crate rustc_hash;
