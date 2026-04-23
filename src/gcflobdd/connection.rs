use std::{cell::RefCell, marker::PhantomData};

use crate::{
    gcflobdd::{ReturnMapT, context::Context, node::GcflobddNode, return_map::ReturnMap},
    utils::hash_cache::Rch,
};

#[derive(Clone)]
pub(super) struct ConnectionT<'grammar, Handle> {
    pub entry_point: usize,
    pub return_map: Handle,
    pub phantom: PhantomData<GcflobddNode<'grammar>>,
}

impl<'grammar, Handle: std::hash::Hash> std::hash::Hash for ConnectionT<'grammar, Handle> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.entry_point.hash(state);
        self.return_map.hash(state);
    }
}

impl<'grammar, Handle: PartialEq> PartialEq for ConnectionT<'grammar, Handle> {
    fn eq(&self, other: &Self) -> bool {
        self.entry_point == other.entry_point && self.return_map == other.return_map
    }
}
impl<'grammar, Handle: Eq> Eq for ConnectionT<'grammar, Handle> {}

impl<'grammar, T: std::fmt::Debug> std::fmt::Debug for ConnectionT<'grammar, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionT")
            .field("entry_point", &self.entry_point)
            .field("return_map", &self.return_map)
            .finish()
    }
}

pub(crate) type Connection<'grammar> = ConnectionT<'grammar, Rch<ReturnMap>>;
pub(crate) type ConnectionPair<'grammar> = ConnectionT<'grammar, ReturnMapT<(usize, usize)>>;

impl<'grammar> Connection<'grammar> {
    pub fn new_sequential(
        entry_point: usize,
        num_exits: usize,
        context: &RefCell<Context<'grammar>>,
    ) -> Self {
        Self {
            return_map: context
                .borrow()
                .add_return_map((0..num_exits).collect()),
            entry_point,
            phantom: PhantomData,
        }
    }
    pub fn new(
        entry_point: usize,
        return_map: ReturnMap,
        context: &RefCell<Context<'grammar>>,
    ) -> Self {
        Self {
            return_map: context.borrow().add_return_map(return_map),
            entry_point,
            phantom: PhantomData,
        }
    }
}

impl<'grammar> ConnectionPair<'grammar> {
    pub fn flipped(&self) -> Self {
        Self {
            entry_point: self.entry_point,
            return_map: self.return_map.iter().map(|(i, j)| (*j, *i)).collect(),
            phantom: PhantomData,
        }
    }
}
