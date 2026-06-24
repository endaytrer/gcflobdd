use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::thread::LocalKey;
use std::{hash::Hash, rc::Rc};

use crate::{
    gcflobdd::{
        OpCached, PairMapResult, PairProductResult, grouping::Grouping, intern_in,
    },
    grammar::GhddGrammar,
    utils::{
        HashMap, HashSet,
        hash_cache::{HashCached, Rch},
    },
};

pub trait Connection: OpCached {
    /// The thread-local interning table for this concrete connection type.
    fn conn_table() -> &'static LocalKey<RefCell<HashSet<Rch<Self>>>>;

    fn mk_distinction(x: usize) -> Rch<Self>;
    fn mk_no_distinction() -> Rch<Self>;
    fn num_exits(&self) -> usize;

    /// One assignment of this connection's variables (`None` = don't care) that
    /// reaches `exit`.
    fn find_one_path_to(&self, exit: usize) -> Vec<Option<bool>>;

    /// Hash-cons `value` into this type's `conn_table`.
    fn intern(value: Self) -> Rch<Self> {
        Self::conn_table().with(|table| intern_in(table, value))
    }

    // --- type-specific cores (provided by the wrapper, delegating to the
    // wrapped `Connection1`/`ConnectionK`) -------------------------------------

    fn pair_product_inner(lhs: &Rch<Self>, rhs: &Rch<Self>) -> (Self, Vec<(usize, usize)>);
    fn reduce_inner(this: &Rch<Self>, reduce_map: &[usize], num_exits: usize) -> Self;

    // --- cache + interning boilerplate (shared) -------------------------------

    fn pair_product(lhs: &Rch<Self>, rhs: &Rch<Self>) -> PairProductResult<Self> {
        if let Some(c) = Self::get_pair_product_cache(lhs, rhs) {
            return c;
        }
        if Rc::ptr_eq(lhs, rhs) {
            let n = lhs.num_exits();
            let ans = (lhs.clone(), (0..n).map(|i| (i, i)).collect());
            Self::set_pair_product_cache(lhs, rhs, ans.clone());
            return ans;
        }
        let (val, map) = Self::pair_product_inner(lhs, rhs);
        let ans = (Self::intern(val), map);
        Self::set_pair_product_cache(lhs, rhs, ans.clone());
        ans
    }

    fn reduce(this: &Rch<Self>, reduce_map: &[usize], num_exits: usize) -> Rch<Self> {
        if num_exits == 1 {
            return Self::mk_no_distinction();
        }
        // identity map ⇒ unchanged (the generation process guarantees this shape)
        if num_exits == reduce_map.len() {
            debug_assert!(reduce_map.iter().enumerate().all(|(i, x)| *x == i));
            return this.clone();
        }
        if let Some(c) = Self::get_reduction_cache(this, reduce_map) {
            return c;
        }
        let val = Self::reduce_inner(this, reduce_map, num_exits);
        let ans = Self::intern(val);
        Self::set_reduction_cache(this, reduce_map, ans.clone());
        ans
    }

    /// `pair_map(f, g, op) = λx. op(f(x), g(x))`: pair the two diagrams, relabel
    /// each product exit by `op_matrix` (indexed `lhs_exit * rhs_num_exits +
    /// rhs_exit`), and reduce. `pair_product` and `reduce` are individually
    /// cached, so a fused Apply (avoiding the materialized product) is only a
    /// future memory optimization, not a correctness need.
    fn pair_map(
        lhs: &Rch<Self>,
        rhs: &Rch<Self>,
        op_matrix: &Rch<Vec<usize>>,
        num_exits: usize,
    ) -> PairMapResult<Self> {
        if num_exits == 1 {
            return (Self::mk_no_distinction(), vec![op_matrix[0]]);
        }
        if let Some(c) = Self::get_pair_map_cache(lhs, rhs, op_matrix) {
            return c;
        }
        let rhs_ne = rhs.num_exits();
        let (prod, prod_map) = Self::pair_product(lhs, rhs);
        // relabel product exits through the op, renumbering values to canonical
        // scan order (`outer_map[result_exit] = op value`)
        let mut value_lookup = vec![usize::MAX; num_exits];
        let mut outer_map: Vec<usize> = Vec::new();
        let reduce_map: Vec<usize> = prod_map
            .iter()
            .map(|&(i, j)| {
                let v = op_matrix[i * rhs_ne + j];
                if value_lookup[v] == usize::MAX {
                    value_lookup[v] = outer_map.len();
                    outer_map.push(v);
                }
                value_lookup[v]
            })
            .collect();
        let result = Self::reduce(&prod, &reduce_map, outer_map.len());
        let ans = (result, outer_map);
        Self::set_pair_map_cache(lhs, rhs, op_matrix, ans.clone());
        ans
    }
}

/// A connection over a single-component grammar: just the entry grouping. The
/// table-owning `Connection` type is the macro-generated local wrapper around
/// this; the struct itself is never interned directly. Its exits are exactly its
/// grouping's exits (identity return map), so every operation pushes through to
/// the grouping.
pub struct Connection1<T: GhddGrammar> {
    grouping: Rch<T::Grouping>,
}

impl<T: GhddGrammar> Connection1<T> {
    pub fn mk_distinction(x: usize) -> Rch<Self> {
        debug_assert!(x < T::NUM_VARS);
        Rc::new(HashCached::new(Self {
            grouping: T::Grouping::mk_distinction(x),
        }))
    }
    pub fn mk_no_distinction() -> Rch<Self> {
        Rc::new(HashCached::new(Self {
            grouping: T::Grouping::mk_no_distinction(),
        }))
    }
    pub fn num_exits(&self) -> usize {
        self.grouping.num_exits()
    }

    pub fn pair_product(lhs: &Rch<Self>, rhs: &Rch<Self>) -> (Self, Vec<(usize, usize)>) {
        let (grouping, pairs) = T::Grouping::pair_product(&lhs.grouping, &rhs.grouping);
        (Self { grouping }, pairs)
    }
    pub fn reduce(this: &Rch<Self>, reduce_map: &[usize], num_exits: usize) -> Self {
        Self {
            grouping: T::Grouping::reduce(&this.grouping, reduce_map, num_exits),
        }
    }
    pub fn find_one_path_to(&self, exit: usize) -> Vec<Option<bool>> {
        self.grouping.find_one_path_to(exit)
    }
}

impl<T: GhddGrammar> Hash for Connection1<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.grouping.hash(state);
    }
}
impl<T: GhddGrammar> PartialEq for Connection1<T> {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.grouping, &other.grouping)
    }
}
impl<T: GhddGrammar> Eq for Connection1<T> {}

/// A connection over a K-component grammar: an entry grouping plus a child
/// connection per grouping exit (`first_child` = exit 0 with an *identity* return
/// map, `rest_children` = the rest, each carrying a return map into this
/// connection's exit space). `num_exits` is the size of that exit space; it is
/// derived from the children + return maps but stored (and excluded from
/// `Hash`/`Eq`) because every operation reads it.
pub struct ConnectionK<T: GhddGrammar, C: Connection> {
    grouping: Rch<T::Grouping>,
    first_child: Rch<C>,
    rest_children: Vec<ReturnPoint<C>>,
    num_exits: usize,
}

impl<T: GhddGrammar, C: Connection> ConnectionK<T, C> {
    pub fn mk_distinction(x: usize) -> Rch<Self> {
        let node = if x < T::NUM_VARS {
            // distinguish within the head grouping: exit 0 → outer 0, exit 1 → 1
            Self {
                grouping: T::Grouping::mk_distinction(x),
                first_child: C::mk_no_distinction(),
                rest_children: vec![ReturnPoint {
                    connection: C::mk_no_distinction(),
                    return_map: vec![1],
                }],
                num_exits: 2,
            }
        } else {
            // head is no-distinction; the single child carries the distinction
            Self {
                grouping: T::Grouping::mk_no_distinction(),
                first_child: C::mk_distinction(x - T::NUM_VARS),
                rest_children: Vec::new(),
                num_exits: 2,
            }
        };
        Rc::new(HashCached::new(node))
    }
    pub fn mk_no_distinction() -> Rch<Self> {
        Rc::new(HashCached::new(Self {
            grouping: T::Grouping::mk_no_distinction(),
            first_child: C::mk_no_distinction(),
            rest_children: Vec::new(),
            num_exits: 1,
        }))
    }
    pub fn num_exits(&self) -> usize {
        self.num_exits
    }

    /// The child connection for grouping exit `k`.
    fn child(&self, k: usize) -> &Rch<C> {
        if k == 0 {
            &self.first_child
        } else {
            &self.rest_children[k - 1].connection
        }
    }
    /// Where exit `e` of the child for grouping exit `k` lands in this
    /// connection's exit space (`first_child` has an identity return map).
    fn rm_at(&self, k: usize, e: usize) -> usize {
        if k == 0 {
            e
        } else {
            self.rest_children[k - 1].return_map[e]
        }
    }

    pub fn pair_product(lhs: &Rch<Self>, rhs: &Rch<Self>) -> (Self, Vec<(usize, usize)>) {
        let (grouping, inner_pairs) = T::Grouping::pair_product(&lhs.grouping, &rhs.grouping);
        let rhs_ne = rhs.num_exits;
        // (lhs_exit, rhs_exit) → this connection's exit, assigned in scan order
        let mut exit_lookup = vec![usize::MAX; lhs.num_exits * rhs_ne];
        let mut outer_return_map: Vec<(usize, usize)> = Vec::new();
        let mut first_child: Option<Rch<C>> = None;
        let mut rest_children: Vec<ReturnPoint<C>> = Vec::new();
        for (p, &(i, j)) in inner_pairs.iter().enumerate() {
            let (prod_child, child_pairs) = C::pair_product(lhs.child(i), rhs.child(j));
            let rm: Vec<usize> = child_pairs
                .iter()
                .map(|&(a, b)| {
                    let oi = lhs.rm_at(i, a);
                    let oj = rhs.rm_at(j, b);
                    let idx = oi * rhs_ne + oj;
                    if exit_lookup[idx] == usize::MAX {
                        exit_lookup[idx] = outer_return_map.len();
                        outer_return_map.push((oi, oj));
                    }
                    exit_lookup[idx]
                })
                .collect();
            if p == 0 {
                // for canonical inputs the first product exit pairs (0, 0) and
                // both children have identity maps, so `rm` is the identity
                debug_assert!(rm.iter().enumerate().all(|(e, &v)| e == v));
                first_child = Some(prod_child);
            } else {
                rest_children.push(ReturnPoint {
                    connection: prod_child,
                    return_map: rm,
                });
            }
        }
        let num_exits = outer_return_map.len();
        (
            Self {
                grouping,
                first_child: first_child.expect("a grouping has at least one exit"),
                rest_children,
                num_exits,
            },
            outer_return_map,
        )
    }

    pub fn reduce(this: &Rch<Self>, reduce_map: &[usize], num_exits: usize) -> Self {
        let g = this.grouping.num_exits();
        // dedup (reduced child pointer, its return map) so collapsed grouping
        // exits merge; `grouping_reduce_map` then reduces the entry grouping.
        let mut dedup: HashMap<(usize, Vec<usize>), usize> = HashMap::default();
        let mut grouping_reduce_map: Vec<usize> = Vec::with_capacity(g);
        let mut first_child: Option<Rch<C>> = None;
        let mut rest_children: Vec<ReturnPoint<C>> = Vec::new();
        for k in 0..g {
            let child = this.child(k);
            let child_ne = child.num_exits();
            // child exit e → result exit, via reduce_map ∘ rm_k, renumbered to
            // the child's local canonical exit space
            let mut local_lookup = vec![usize::MAX; num_exits];
            let mut child_rm: Vec<usize> = Vec::new(); // local exit → result exit
            let local_map: Vec<usize> = (0..child_ne)
                .map(|e| {
                    let target = reduce_map[this.rm_at(k, e)];
                    if local_lookup[target] == usize::MAX {
                        local_lookup[target] = child_rm.len();
                        child_rm.push(target);
                    }
                    local_lookup[target]
                })
                .collect();
            let reduced_child = C::reduce(child, &local_map, child_rm.len());

            let key = (Rc::as_ptr(&reduced_child) as usize, child_rm.clone());
            let exit = match dedup.entry(key) {
                Entry::Occupied(e) => *e.get(),
                Entry::Vacant(slot) => {
                    let idx = if first_child.is_none() {
                        0
                    } else {
                        1 + rest_children.len()
                    };
                    if first_child.is_none() {
                        debug_assert!(child_rm.iter().enumerate().all(|(e, &v)| e == v));
                        first_child = Some(reduced_child);
                    } else {
                        rest_children.push(ReturnPoint {
                            connection: reduced_child,
                            return_map: child_rm,
                        });
                    }
                    slot.insert(idx);
                    idx
                }
            };
            grouping_reduce_map.push(exit);
        }
        let distinct = grouping_reduce_map.iter().max().map(|m| m + 1).unwrap_or(0);
        let grouping = T::Grouping::reduce(&this.grouping, &grouping_reduce_map, distinct);
        Self {
            grouping,
            first_child: first_child.expect("a grouping has at least one exit"),
            rest_children,
            num_exits,
        }
    }

    pub fn find_one_path_to(&self, exit: usize) -> Vec<Option<bool>> {
        // find a grouping exit `k` whose child has an exit `ce` landing on `exit`,
        // then concatenate the head path with the child's path.
        for k in 0..self.grouping.num_exits() {
            let child = self.child(k);
            for ce in 0..child.num_exits() {
                if self.rm_at(k, ce) == exit {
                    let mut path = self.grouping.find_one_path_to(k);
                    path.extend(child.find_one_path_to(ce));
                    return path;
                }
            }
        }
        unreachable!("exit {exit} is unreachable in this connection")
    }
}

impl<T: GhddGrammar, C: Connection> Hash for ConnectionK<T, C> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.grouping.hash(state);
        self.first_child.hash(state);
        self.rest_children.hash(state);
    }
}
impl<T: GhddGrammar, C: Connection> PartialEq for ConnectionK<T, C> {
    fn eq(&self, other: &Self) -> bool {
        // grouping and children are interned, so pointer identity suffices;
        // `num_exits` is derived, so it is not compared
        Rc::ptr_eq(&self.grouping, &other.grouping)
            && Rc::ptr_eq(&self.first_child, &other.first_child)
            && self.rest_children == other.rest_children
    }
}
impl<T: GhddGrammar, C: Connection> Eq for ConnectionK<T, C> {}

struct ReturnPoint<C: Connection> {
    connection: Rch<C>,
    return_map: Vec<usize>,
}
impl<C: Connection> Hash for ReturnPoint<C> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.connection.hash(state);
        self.return_map.hash(state);
    }
}
impl<C: Connection> PartialEq for ReturnPoint<C> {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.connection, &other.connection) && self.return_map == other.return_map
    }
}
impl<C: Connection> Eq for ReturnPoint<C> {}
