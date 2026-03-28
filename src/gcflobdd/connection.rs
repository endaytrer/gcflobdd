use std::{cell::RefCell, rc::Rc};

use crate::{
    gcflobdd::{ReturnMapT, context::Context, node::GcflobddNode, return_map::ReturnMap},
    utils::hash_cache::Rch,
};

#[derive(Clone, Hash)]
pub(super) struct ConnectionT<'grammar, Handle> {
    pub entry_point: Rch<GcflobddNode<'grammar>>,
    pub return_map: Handle,
}

impl<'grammar, T: std::fmt::Debug> std::fmt::Debug for ConnectionT<'grammar, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionT")
            .field("entry_point", &Rc::as_ptr(&self.entry_point))
            .field("return_map", &self.return_map)
            .finish()
    }
}

pub(crate) type Connection<'grammar> = ConnectionT<'grammar, Rch<ReturnMap>>;
pub(crate) type ConnectionPair<'grammar> = ConnectionT<'grammar, ReturnMapT<(usize, usize)>>;

impl<'grammar> Connection<'grammar> {
    pub fn new_sequential(
        entry_point: Rch<GcflobddNode<'grammar>>,
        context: &RefCell<Context<'grammar>>,
    ) -> Self {
        Self {
            return_map: context
                .borrow_mut()
                .add_return_map((0..entry_point.get_num_exits()).collect()),
            entry_point,
        }
    }
    pub fn new(
        entry_point: Rch<GcflobddNode<'grammar>>,
        return_map: ReturnMap,
        context: &RefCell<Context<'grammar>>,
    ) -> Self {
        Self {
            return_map: context.borrow_mut().add_return_map(return_map),
            entry_point,
        }
    }
}

impl<'grammar> ConnectionPair<'grammar> {
    pub fn flipped(&self) -> Self {
        Self {
            entry_point: self.entry_point.clone(),
            return_map: self.return_map.iter().map(|(i, j)| (*j, *i)).collect(),
        }
    }
}

impl PartialEq for Connection<'_> {
    fn eq(&self, other: &Self) -> bool {
        Rc::as_ptr(&self.entry_point) == Rc::as_ptr(&other.entry_point)
            && Rc::as_ptr(&self.return_map) == Rc::as_ptr(&other.return_map)
    }
}
impl Eq for Connection<'_> {}
