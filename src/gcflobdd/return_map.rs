use std::rc::Rc;

pub(super) type ReturnMapT<T> = Vec<T>;

/// A return map shared by reference rather than copied.
///
/// [`GcflobddT`](crate::gcflobdd::GcflobddT) values are cloned constantly --
/// twice to probe the operation cache and once more to take the answer out of
/// it -- and while the map itself is tiny (a boolean diagram's has at most two
/// entries), a `Vec` clone is a heap allocation every time. Behind an `Rc` the
/// clone is a refcount bump, which is the whole of the difference: three
/// allocations and two frees per cache *hit*, gone.
///
/// Plain `Rc`, not an interned handle: the return map's element type reaches
/// the caller, and `rug::Complex` is neither `Hash` nor `Eq`, so it cannot go
/// in a hash-consing table. Equality still falls back to comparing contents
/// when two maps were built separately -- but for `T: Eq`, `Rc`'s own
/// `PartialEq` shortcuts on pointer identity first.
pub(super) type SharedReturnMap<T> = Rc<ReturnMapT<T>>;
pub(super) type ReturnMap = ReturnMapT<usize>;

pub(super) fn inverse_lookup<T: Eq>(return_map: &ReturnMapT<T>, value: &T) -> Option<usize> {
    return_map.iter().position(|x| *x == *value)
}
pub(super) fn complement(return_map: &ReturnMapT<bool>) -> ReturnMapT<bool> {
    return_map.iter().map(|x| !x).collect()
}
