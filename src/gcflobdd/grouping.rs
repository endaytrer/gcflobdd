use std::cell::RefCell;
use std::thread::LocalKey;
use std::{hash::Hash, rc::Rc};

use crate::{
    __gcflobdd_op_cache_storage,
    gcflobdd::{
        OpCached, PairMapResult, PairProductResult, connection::Connection, intern_in,
    },
    grammar::{BddGrammar, GhddGrammar, RecursiveGrammar, Unit},
    utils::{HashMap, HashSet, hash_cache::Rch},
};

pub trait Grouping: OpCached {
    type Grammar: GhddGrammar<Grouping = Self>;

    /// The thread-local interning table for this concrete grouping type.
    fn group_table() -> &'static LocalKey<RefCell<HashSet<Rch<Self>>>>;

    fn mk_no_distinction() -> Rch<Self>;
    fn mk_distinction(x: usize) -> Rch<Self>;
    fn num_exits(&self) -> usize;

    /// Hash-cons `value` into this type's `group_table`.
    fn intern(value: Self) -> Rch<Self> {
        Self::group_table().with(|table| intern_in(table, value))
    }

    // --- type-specific cores --------------------------------------------------

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

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum UnitGrouping {
    DontCare,
    Fork,
}
impl UnitGrouping {
    thread_local! {
        static GROUP_TABLE: RefCell<HashSet<Rch<UnitGrouping>>> = RefCell::new(HashSet::default());
    }
}
__gcflobdd_op_cache_storage!(UnitGrouping);
impl Grouping for UnitGrouping {
    type Grammar = Unit;
    fn group_table() -> &'static LocalKey<RefCell<HashSet<Rch<Self>>>> {
        &Self::GROUP_TABLE
    }
    fn mk_no_distinction() -> Rch<Self> {
        Self::intern(Self::DontCare)
    }
    fn num_exits(&self) -> usize {
        match self {
            Self::DontCare => 1,
            Self::Fork => 2,
        }
    }
    fn mk_distinction(x: usize) -> Rch<Self> {
        debug_assert!(x == 0);
        Self::intern(Self::Fork)
    }

    fn pair_product_inner(lhs: &Rch<Self>, rhs: &Rch<Self>) -> (Self, Vec<(usize, usize)>) {
        let l: &UnitGrouping = lhs;
        let r: &UnitGrouping = rhs;
        match (l, r) {
            (Self::DontCare, Self::DontCare) => (Self::DontCare, vec![(0, 0)]),
            (Self::DontCare, Self::Fork) => (Self::Fork, vec![(0, 0), (0, 1)]),
            (Self::Fork, Self::DontCare) => (Self::Fork, vec![(0, 0), (1, 0)]),
            (Self::Fork, Self::Fork) => (Self::Fork, vec![(0, 0), (1, 1)]),
        }
    }

    fn reduce_inner(_this: &Rch<Self>, _reduce_map: &[usize], _num_exits: usize) -> Self {
        // A `UnitGrouping` has at most 2 exits; every canonical reduce of it is
        // handled by the `num_exits == 1` / identity short-circuits.
        unreachable!("reduce on UnitGrouping is always a short-circuit")
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum BddGrouping<const N: usize> {
    DontCare,
    Bdd(BDD<N>),
}

impl<const N: usize> Grouping for BddGrouping<N> {
    type Grammar = BddGrammar<N>;
    // BddGrouping<N> is const-generic, so it cannot own a single per-`N`
    // thread-local (generic statics are not allowed). A specific `N` would need
    // its own table; unimplemented until BDD groupings are wired up.
    fn group_table() -> &'static LocalKey<RefCell<HashSet<Rch<Self>>>> {
        todo!()
    }
    fn mk_no_distinction() -> Rch<Self> {
        todo!()
    }
    fn num_exits(&self) -> usize {
        match self {
            Self::DontCare => 1,
            Self::Bdd(_) => todo!(),
        }
    }
    fn mk_distinction(_x: usize) -> Rch<Self> {
        todo!()
    }
    fn pair_product_inner(_lhs: &Rch<Self>, _rhs: &Rch<Self>) -> (Self, Vec<(usize, usize)>) {
        todo!()
    }
    fn reduce_inner(_this: &Rch<Self>, _reduce_map: &[usize], _num_exits: usize) -> Self {
        todo!()
    }
}
impl<const N: usize> OpCached for BddGrouping<N> {
    // See `group_table` above: const-generic, so no per-`N` static yet.
    fn pair_product_cache()
    -> &'static LocalKey<RefCell<HashMap<(usize, usize), PairProductResult<Self>>>> {
        todo!()
    }
    fn pair_map_cache()
    -> &'static LocalKey<RefCell<HashMap<(usize, usize, usize), PairMapResult<Self>>>> {
        todo!()
    }
    fn reduction_cache() -> &'static LocalKey<RefCell<HashMap<(usize, Vec<usize>), Rch<Self>>>> {
        todo!()
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct BDD<const N: usize>;

/// Inner data of a recursive grouping. The table-owning `Grouping` type is the
/// macro-generated local wrapper around this; this enum only carries structure.
pub enum RecursiveGrouping<T: RecursiveGrammar> {
    DontCare,
    Connection {
        connection_diagram: Rch<T::Connection>,
    },
}
impl<T: RecursiveGrammar> RecursiveGrouping<T> {
    pub fn mk_no_distinction() -> Self {
        Self::DontCare
    }
    pub fn mk_distinction(x: usize) -> Self {
        debug_assert!(x < T::NUM_VARS);
        Self::Connection {
            connection_diagram: T::Connection::mk_distinction(x),
        }
    }
    pub fn num_exits(&self) -> usize {
        match self {
            Self::DontCare => 1,
            Self::Connection { connection_diagram } => connection_diagram.num_exits(),
        }
    }

    pub fn pair_product(lhs: &Self, rhs: &Self) -> (Self, Vec<(usize, usize)>) {
        match (lhs, rhs) {
            (Self::DontCare, Self::DontCare) => (Self::DontCare, vec![(0, 0)]),
            (Self::DontCare, Self::Connection { connection_diagram }) => {
                let n = connection_diagram.num_exits();
                (rhs.clone(), (0..n).map(|j| (0, j)).collect())
            }
            (Self::Connection { connection_diagram }, Self::DontCare) => {
                let n = connection_diagram.num_exits();
                (lhs.clone(), (0..n).map(|i| (i, 0)).collect())
            }
            (
                Self::Connection { connection_diagram: a },
                Self::Connection { connection_diagram: b },
            ) => {
                let (conn, map) = T::Connection::pair_product(a, b);
                if map.len() == 1 {
                    (Self::DontCare, map)
                } else {
                    (Self::Connection { connection_diagram: conn }, map)
                }
            }
        }
    }

    pub fn reduce(this: &Self, reduce_map: &[usize], num_exits: usize) -> Self {
        match this {
            Self::DontCare => {
                unreachable!("reduce on a DontCare grouping is a num_exits == 1 short-circuit")
            }
            Self::Connection { connection_diagram } => {
                // num_exits > 1 here, so the reduced connection keeps > 1 exit
                let conn = T::Connection::reduce(connection_diagram, reduce_map, num_exits);
                Self::Connection { connection_diagram: conn }
            }
        }
    }
}
impl<T: RecursiveGrammar> Clone for RecursiveGrouping<T> {
    fn clone(&self) -> Self {
        match self {
            Self::DontCare => Self::DontCare,
            Self::Connection { connection_diagram } => Self::Connection {
                connection_diagram: connection_diagram.clone(),
            },
        }
    }
}
impl<T: RecursiveGrammar> Hash for RecursiveGrouping<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Self::DontCare => {
                0.hash(state);
            }
            Self::Connection { connection_diagram } => {
                1.hash(state);
                connection_diagram.hash(state);
            }
        }
    }
}
impl<T: RecursiveGrammar> PartialEq for RecursiveGrouping<T> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::DontCare, Self::DontCare) => true,
            // children are interned, so pointer identity is value equality
            (
                Self::Connection {
                    connection_diagram: a,
                },
                Self::Connection {
                    connection_diagram: b,
                },
            ) => Rc::ptr_eq(a, b),
            _ => false,
        }
    }
}
impl<T: RecursiveGrammar> Eq for RecursiveGrouping<T> {}
