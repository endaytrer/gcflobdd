//! The user-facing decision-diagram type: a `GhddGrammar`'s entry grouping
//! paired with a typed return map (one terminal value per grouping exit).
//!
//! Unlike the main engine this carries **no** per-`GcflobddT` operation cache —
//! the grouping/connection operations are already memoized on their own per-type
//! caches (`OpCached`), which is enough for correctness. The wrapper is a thin
//! generic over the grammar `G` and the terminal value type `T`.

use crate::gcflobdd::grouping::Grouping;
use crate::grammar::GhddGrammar;
use crate::utils::hash_cache::Rch;

/// A GCFLOBDD over grammar `G` whose terminals carry values of type `T`.
pub struct GcflobddT<G: GhddGrammar, T> {
    entry_point: Rch<G::Grouping>,
    return_map: Vec<T>,
}

impl<G: GhddGrammar, T: Clone> Clone for GcflobddT<G, T> {
    fn clone(&self) -> Self {
        Self {
            entry_point: self.entry_point.clone(),
            return_map: self.return_map.clone(),
        }
    }
}

impl<G: GhddGrammar, T: std::fmt::Debug> std::fmt::Debug for GcflobddT<G, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcflobddT")
            .field("return_map", &self.return_map)
            .finish_non_exhaustive()
    }
}

/// The common boolean specialization.
pub type Gcflobdd<G> = GcflobddT<G, bool>;

impl<G: GhddGrammar, T> GcflobddT<G, T> {
    /// Relabel each terminal via `f`, merging exits that map to equal values and
    /// reducing the diagram to the new (smaller) exit space.
    pub fn map<V: Eq>(&self, f: impl Fn(&T) -> V) -> GcflobddT<G, V> {
        let mut new_return_map: Vec<V> = Vec::new();
        let reduce_map = self
            .return_map
            .iter()
            .map(|t| {
                let v = f(t);
                new_return_map.iter().position(|x| *x == v).unwrap_or_else(|| {
                    new_return_map.push(v);
                    new_return_map.len() - 1
                })
            })
            .collect::<Vec<_>>();
        let num_exits = new_return_map.len();
        let entry_point = G::Grouping::reduce(&self.entry_point, &reduce_map, num_exits);
        GcflobddT { entry_point, return_map: new_return_map }
    }
}

impl<G: GhddGrammar, T: Copy> GcflobddT<G, T> {
    /// Pair this diagram with `rhs`, producing a diagram whose terminals are the
    /// `(self, rhs)` value pairs reachable together.
    pub fn pair_product(&self, rhs: &Self) -> GcflobddT<G, (T, T)> {
        let (entry_point, return_map) =
            G::Grouping::pair_product(&self.entry_point, &rhs.entry_point);
        let return_map = return_map
            .into_iter()
            .map(|(i, j)| (self.return_map[i], rhs.return_map[j]))
            .collect();
        GcflobddT { entry_point, return_map }
    }
}

impl<G: GhddGrammar, T: Copy + Eq> GcflobddT<G, T> {
    /// Apply a binary operation pointwise: `pair_product` then relabel/reduce by
    /// `op`. (`pair_map` is internally `pair_product + reduce` as well, so this is
    /// the same work; the grouping-level caches memoize both sub-steps.)
    pub fn mk_op(&self, rhs: &Self, op: impl Fn(&T, &T) -> T) -> Self {
        self.pair_product(rhs).map(|(a, b)| op(a, b))
    }
}

impl<G: GhddGrammar, T: Eq> GcflobddT<G, T> {
    /// One variable assignment (`None` = don't care) under which this diagram
    /// evaluates to `value`, or `None` if no exit carries that value.
    pub fn find_one_path_to(&self, value: &T) -> Option<Vec<Option<bool>>> {
        let index = self.return_map.iter().position(|x| x == value)?;
        Some(self.entry_point.find_one_path_to(index))
    }
}

impl<G: GhddGrammar> Gcflobdd<G> {
    /// The projection onto variable `i` (`x_i`).
    pub fn mk_projection(i: usize) -> Self {
        Self {
            entry_point: G::mk_distinction(i),
            return_map: vec![false, true],
        }
    }
    pub fn mk_true() -> Self {
        Self {
            entry_point: G::mk_no_distinction(),
            return_map: vec![true],
        }
    }
    pub fn mk_false() -> Self {
        Self {
            entry_point: G::mk_no_distinction(),
            return_map: vec![false],
        }
    }
    /// Logical negation: flips the terminals, structure unchanged.
    pub fn mk_not(&self) -> Self {
        Self {
            entry_point: self.entry_point.clone(),
            return_map: self.return_map.iter().map(|b| !b).collect(),
        }
    }

    pub fn mk_and(&self, rhs: &Self) -> Self {
        self.mk_op(rhs, |&a, &b| a && b)
    }
    pub fn mk_or(&self, rhs: &Self) -> Self {
        self.mk_op(rhs, |&a, &b| a || b)
    }
    pub fn mk_xor(&self, rhs: &Self) -> Self {
        self.mk_op(rhs, |&a, &b| a ^ b)
    }
    pub fn mk_nand(&self, rhs: &Self) -> Self {
        self.mk_op(rhs, |&a, &b| !(a && b))
    }
    pub fn mk_nor(&self, rhs: &Self) -> Self {
        self.mk_op(rhs, |&a, &b| !(a || b))
    }
    pub fn mk_xnor(&self, rhs: &Self) -> Self {
        self.mk_op(rhs, |&a, &b| !(a ^ b))
    }
    pub fn mk_implies(&self, rhs: &Self) -> Self {
        self.mk_op(rhs, |&a, &b| !a || b)
    }

    /// A satisfying assignment if one exists.
    pub fn find_one_satisfiable_assignment(&self) -> Option<Vec<Option<bool>>> {
        self.find_one_path_to(&true)
    }
}
