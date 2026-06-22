use std::cell::RefCell;
use std::thread::LocalKey;
use std::{hash::Hash, rc::Rc};

use crate::{
    gcflobdd::{OpCached, grouping::Grouping, intern_in},
    grammar::GhddGrammar,
    utils::{
        HashSet,
        hash_cache::{HashCached, Rch},
    },
};

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
