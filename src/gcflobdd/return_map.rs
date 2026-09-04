use std::rc::Rc;

use smallvec::SmallVec;

/// Entries a map holds inline before it reaches the heap.
///
/// The maps this crate builds in bulk are tiny -- a boolean diagram's return map
/// has at most two entries, a boolean operation's reduce matrix at most four --
/// and building them dominated the cost of every operation that missed the
/// operation cache. Under an allocation profile of the atomic-predicate
/// workload, a conjunction allocated 49.5 times, almost all of it in the
/// 8-to-63-byte size classes: maps of one to seven words. The profiling harness
/// that measured this is gone; the budget it justified is what remains.
pub(super) const RETURN_MAP_INLINE: usize = 4;

/// A return map built during an operation: inline until it outgrows the budget.
pub(super) type ReturnMapT<T> = SmallVec<[T; RETURN_MAP_INLINE]>;

/// A reduce matrix or reduce map: exit indices, same shape and same inline
/// budget as a return map.
pub(super) type ExitVec = ReturnMapT<usize>;

/// The return map of a finished diagram, shared by reference rather than copied.
///
/// Two decisions here, both measured (`cargo test --release --test opbench`).
///
/// **Behind an `Rc`.** A [`GcflobddT`](crate::gcflobdd::GcflobddT) is cloned
/// three times per operation-cache hit -- twice to build the probe key, once to
/// take the answer back out -- and copying the map each time was a heap
/// allocation. A refcount bump took a cached `and` from 86 ns to 19 ns.
///
/// Plain `Rc`, not an interned handle: the element type reaches the caller and
/// `rug::Complex` is neither `Hash` nor `Eq`, so it cannot go in a hash-consing
/// table. `Rc`'s `Hash` hashes the pointee, so equal diagrams built by different
/// routes still hash alike -- the property every table keyed on a diagram needs.
///
/// **A `Vec` inside it, not a [`ReturnMapT`].** This is the one map that is read
/// far more often than it is built, and `SmallVec` pays a branch on every access
/// to decide inline-versus-spilled. Making it inline cost 10 ns on every cache
/// hit *and* 9% on the atomic-predicate loop, against the single allocation it
/// saved per diagram. The transient maps above are the opposite trade and go
/// inline; this one does not.
pub(super) type SharedReturnMap<T> = Rc<Vec<T>>;

pub(super) type ReturnMap = ReturnMapT<usize>;

pub(super) fn inverse_lookup<T: Eq>(return_map: &[T], value: &T) -> Option<usize> {
    return_map.iter().position(|x| *x == *value)
}
pub(super) fn complement(return_map: &[bool]) -> Vec<bool> {
    return_map.iter().map(|x| !x).collect()
}
