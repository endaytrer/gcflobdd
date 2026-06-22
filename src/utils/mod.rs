pub mod hash_cache;

// A single canonical `HashSet` alias that follows this crate's `fx-hash`
// feature. Re-exported (rather than left private to each module) so generated
// code — e.g. `declare_grammar!`'s connection tables in downstream crates —
// can name the same set type without taking a direct `rustc-hash` dependency.
#[cfg(feature = "fx-hash")]
pub use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
#[cfg(not(feature = "fx-hash"))]
pub use std::collections::{HashMap, HashSet};
