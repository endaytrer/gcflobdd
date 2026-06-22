use crate::{
    grammar::{BddGrammar, GhddGrammar, RecursiveGrammar, Unit},
    utils::{
        HashMap, HashSet,
        hash_cache::{HashCached, Rch},
    },
};
use std::cell::RefCell;
use std::thread::LocalKey;
use std::{hash::Hash, rc::Rc};

/// Intern `value` into a per-type table: if an equal value is already present,
/// return the canonical `Rch`; otherwise insert and return the new one. After
/// interning, `Rc::as_ptr` is a valid identity for the value (used by the
/// operation caches). Mirrors `Context::add_*` from the old engine.
fn intern_in<T: Hash + Eq + 'static>(table: &RefCell<HashSet<Rch<T>>>, value: T) -> Rch<T> {
    let hashed = HashCached::new(value);
    let mut table = table.borrow_mut();
    if let Some(existing) = table.get(&hashed) {
        return existing.clone();
    }
    let rch = Rc::new(hashed);
    table.insert(rch.clone());
    rch
}

/// Result of `pair_product`: the product node plus, for each of its exits, the
/// `(lhs_exit, rhs_exit)` pair it came from.
pub type PairProductResult<T> = (Rch<T>, Vec<(usize, usize)>);
/// Result of `pair_map`: the mapped node plus its outer return map.
pub type PairMapResult<T> = (Rch<T>, Vec<usize>);

thread_local! {
    /// Global interning table for operation matrices (`Vec<usize>`), so that
    /// `pair_map` can key its cache on the matrix's pointer identity. Concrete
    /// (non-generic) element type, so a single thread-local suffices.
    static OP_MATRIX_TABLE: RefCell<HashSet<Rch<Vec<usize>>>> = RefCell::new(HashSet::default());
}

/// Hash-cons an operation matrix; see [`OP_MATRIX_TABLE`].
pub fn intern_op_matrix(matrix: Vec<usize>) -> Rch<Vec<usize>> {
    OP_MATRIX_TABLE.with(|table| intern_in(table, matrix))
}

/// Per-type operation caches, keyed by the `Rc::as_ptr` identity of the
/// (interned) operands — the analogue of the old central `Context` caches. The
/// storage accessors are provided per concrete/wrapper type (by
/// [`__gcflobdd_op_cache_storage`] / `declare_grammar!`); the get/set logic is
/// shared here.
pub trait OpCached: Hash + Eq + Sized + 'static {
    fn pair_product_cache()
    -> &'static LocalKey<RefCell<HashMap<(usize, usize), PairProductResult<Self>>>>;
    fn pair_map_cache()
    -> &'static LocalKey<RefCell<HashMap<(usize, usize, usize), PairMapResult<Self>>>>;
    fn reduction_cache() -> &'static LocalKey<RefCell<HashMap<(usize, Vec<usize>), Rch<Self>>>>;

    fn get_pair_product_cache(
        lhs: &Rch<Self>,
        rhs: &Rch<Self>,
    ) -> Option<PairProductResult<Self>> {
        let key = (Rc::as_ptr(lhs) as usize, Rc::as_ptr(rhs) as usize);
        Self::pair_product_cache().with(|c| c.borrow().get(&key).cloned())
    }
    fn set_pair_product_cache(
        lhs: &Rch<Self>,
        rhs: &Rch<Self>,
        result: PairProductResult<Self>,
    ) {
        let key = (Rc::as_ptr(lhs) as usize, Rc::as_ptr(rhs) as usize);
        Self::pair_product_cache().with(|c| {
            c.borrow_mut().insert(key, result);
        });
    }
    fn get_pair_map_cache(
        lhs: &Rch<Self>,
        rhs: &Rch<Self>,
        op_matrix: &Rch<Vec<usize>>,
    ) -> Option<PairMapResult<Self>> {
        let key = (
            Rc::as_ptr(lhs) as usize,
            Rc::as_ptr(rhs) as usize,
            Rc::as_ptr(op_matrix) as usize,
        );
        Self::pair_map_cache().with(|c| c.borrow().get(&key).cloned())
    }
    fn set_pair_map_cache(
        lhs: &Rch<Self>,
        rhs: &Rch<Self>,
        op_matrix: &Rch<Vec<usize>>,
        result: PairMapResult<Self>,
    ) {
        let key = (
            Rc::as_ptr(lhs) as usize,
            Rc::as_ptr(rhs) as usize,
            Rc::as_ptr(op_matrix) as usize,
        );
        Self::pair_map_cache().with(|c| {
            c.borrow_mut().insert(key, result);
        });
    }
    fn get_reduction_cache(node: &Rch<Self>, reduce_map: &[usize]) -> Option<Rch<Self>> {
        let key = (Rc::as_ptr(node) as usize, reduce_map.to_vec());
        Self::reduction_cache().with(|c| c.borrow().get(&key).cloned())
    }
    fn set_reduction_cache(node: &Rch<Self>, reduce_map: &[usize], result: Rch<Self>) {
        let key = (Rc::as_ptr(node) as usize, reduce_map.to_vec());
        Self::reduction_cache().with(|c| {
            c.borrow_mut().insert(key, result);
        });
    }
}

/// Define the three operation-cache thread-locals for a concrete type and
/// implement [`OpCached`] for it. Exported so `declare_grammar!`'s wrappers can
/// reuse the exact same storage shape as the in-crate concrete types.
#[macro_export]
macro_rules! __gcflobdd_op_cache_storage {
    ($ty:ty) => {
        impl $ty {
            thread_local! {
                static PAIR_PRODUCT_CACHE: ::std::cell::RefCell<$crate::utils::HashMap<
                    (usize, usize),
                    $crate::gcflobdd::connection_graph::PairProductResult<$ty>,
                >> = ::std::cell::RefCell::new($crate::utils::HashMap::default());
                static PAIR_MAP_CACHE: ::std::cell::RefCell<$crate::utils::HashMap<
                    (usize, usize, usize),
                    $crate::gcflobdd::connection_graph::PairMapResult<$ty>,
                >> = ::std::cell::RefCell::new($crate::utils::HashMap::default());
                static REDUCTION_CACHE: ::std::cell::RefCell<$crate::utils::HashMap<
                    (usize, ::std::vec::Vec<usize>),
                    $crate::utils::hash_cache::Rch<$ty>,
                >> = ::std::cell::RefCell::new($crate::utils::HashMap::default());
            }
        }
        impl $crate::gcflobdd::connection_graph::OpCached for $ty {
            fn pair_product_cache() -> &'static ::std::thread::LocalKey<
                ::std::cell::RefCell<$crate::utils::HashMap<
                    (usize, usize),
                    $crate::gcflobdd::connection_graph::PairProductResult<Self>,
                >>,
            > {
                &Self::PAIR_PRODUCT_CACHE
            }
            fn pair_map_cache() -> &'static ::std::thread::LocalKey<
                ::std::cell::RefCell<$crate::utils::HashMap<
                    (usize, usize, usize),
                    $crate::gcflobdd::connection_graph::PairMapResult<Self>,
                >>,
            > {
                &Self::PAIR_MAP_CACHE
            }
            fn reduction_cache() -> &'static ::std::thread::LocalKey<
                ::std::cell::RefCell<$crate::utils::HashMap<
                    (usize, ::std::vec::Vec<usize>),
                    $crate::utils::hash_cache::Rch<Self>,
                >>,
            > {
                &Self::REDUCTION_CACHE
            }
        }
    };
}

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
    Connection { connection_diagram: Rch<T::Connection> },
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
                Self::Connection { connection_diagram: a },
                Self::Connection { connection_diagram: b },
            ) => Rc::ptr_eq(a, b),
            _ => false,
        }
    }
}
impl<T: RecursiveGrammar> Eq for RecursiveGrouping<T> {}

pub trait Connection: OpCached {
    /// The thread-local interning table for this concrete connection type.
    fn conn_table() -> &'static LocalKey<RefCell<HashSet<Rch<Self>>>>;

    fn mk_distinction(x: usize) -> Rch<Self>;
    fn mk_no_distinction() -> Rch<Self>;

    /// Hash-cons `value` into this type's `conn_table`.
    fn intern(value: Self) -> Rch<Self> {
        Self::conn_table().with(|table| intern_in(table, value))
    }
}

/// A connection over a single-component grammar: just the entry grouping. The
/// table-owning `Connection` type is the macro-generated local wrapper around
/// this; the struct itself is never interned directly.
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
/// connection per exit (`first_child` = exit 0, `rest_children` = the rest).
pub struct ConnectionK<T: GhddGrammar, C: Connection> {
    grouping: Rch<T::Grouping>,
    first_child: Rch<C>,
    rest_children: Vec<ReturnPoint<C>>,
}

impl<T: GhddGrammar, C: Connection> ConnectionK<T, C> {
    pub fn mk_distinction(x: usize) -> Rch<Self> {
        debug_assert!(x < T::NUM_VARS);
        Rc::new(HashCached::new(Self {
            grouping: T::Grouping::mk_distinction(x),
            first_child: C::mk_no_distinction(),
            rest_children: vec![ReturnPoint {
                connection: C::mk_no_distinction(),
                return_map: vec![1],
            }],
        }))
    }
    pub fn mk_no_distinction() -> Rch<Self> {
        Rc::new(HashCached::new(Self {
            grouping: T::Grouping::mk_no_distinction(),
            first_child: C::mk_no_distinction(),
            rest_children: Vec::new(),
        }))
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
        // grouping and children are interned, so pointer identity suffices
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
