use std::cell::RefCell;
use std::thread::LocalKey;
use std::{hash::Hash, rc::Rc};

use crate::{
    __gcflobdd_op_cache_storage,
    gcflobdd::{OpCached, PairMapResult, PairProductResult, connection::Connection, intern_in},
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
            Self::Connection { .. } => todo!(),
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
