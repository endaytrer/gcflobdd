pub(crate) mod hash_cache;

#[cfg(feature = "fx-hash")]
pub(crate) use rustc_hash::FxHashMap as HashMap;
#[cfg(not(feature = "fx-hash"))]
pub(crate) use std::collections::HashMap;

/// An empty map, spelled the same way whichever hasher the build selected.
/// (`HashMap::default()` alone cannot infer the hasher of the `std` map.)
#[inline]
pub(crate) fn new_hash_map<K, V>() -> HashMap<K, V> {
    HashMap::default()
}
