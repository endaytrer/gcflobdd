use std::cell::RefCell;

use super::connection::ConnectionT;
use super::return_map::ReturnMapT;
use crate::gcflobdd::connection::Connection;
use crate::gcflobdd::context::Context;
use crate::gcflobdd::node::GcflobddNode;
use crate::gcflobdd::return_map::ReturnMap;
use crate::grammar::Grammar;

pub(super) struct GcflobddTopNodeT<'grammar, T> {
    connection: ConnectionT<'grammar, ReturnMapT<T>>,
    grammar: &'grammar Grammar,
}
impl<'grammar, T: std::fmt::Debug> std::fmt::Debug for GcflobddTopNodeT<'grammar, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("GcflobddTopNodeT")
            .field(&self.connection)
            .finish()
    }
}

pub(super) type GcflobddTopNode<'grammar> = GcflobddTopNodeT<'grammar, i32>;

impl<'grammar> GcflobddTopNode<'grammar> {
    pub fn new(connection: ConnectionT<'grammar, ReturnMap>, grammar: &'grammar Grammar) -> Self {
        Self {
            connection,
            grammar,
        }
    }

    pub fn mk_distinction(
        i: usize,
        grammar: &'grammar Grammar,
        context: &RefCell<Context<'grammar>>,
    ) -> Self {
        let node = GcflobddNode::mk_distinction(i, &grammar.root, context);
        Self::new(Connection::new(node), grammar)
    }
    pub fn mk_true(grammar: &'grammar Grammar, context: &RefCell<Context<'grammar>>) -> Self {
        let node = GcflobddNode::mk_no_distinction(&grammar.root, context);
        let mut connection = Connection::new(node);
        connection.return_map = connection.return_map.complement();
        Self::new(connection, grammar)
    }
    pub fn mk_false(grammar: &'grammar Grammar, context: &RefCell<Context<'grammar>>) -> Self {
        let node = GcflobddNode::mk_no_distinction(&grammar.root, context);
        Self::new(Connection::new(node), grammar)
    }
    pub fn mk_not(&self) -> Self {
        let mut connection = self.connection.clone();
        connection.return_map = connection.return_map.complement();
        Self {
            connection,
            grammar: self.grammar,
        }
    }
}

impl<'grammar, T: Eq> GcflobddTopNodeT<'grammar, T> {
    pub fn find_one_path_to(&self, value: &T) -> Option<Vec<Option<bool>>> {
        let index = self.connection.return_map.inverse_lookup(value)?;
        Some(self.connection.entry_point.find_one_path_to(index))
    }
}
impl<'grammar, T: Copy> GcflobddTopNodeT<'grammar, T> {
    pub fn pair_product(
        &self,
        rhs: &Self,
        context: &RefCell<Context<'grammar>>,
    ) -> GcflobddTopNodeT<'grammar, (T, T)> {
        let ConnectionT {
            entry_point,
            return_map,
        } = self
            .connection
            .entry_point
            .pair_product(rhs.connection.entry_point.as_ref(), context);
        let mapped_return_map = return_map
            .map_array
            .into_iter()
            .map(|(i, j)| {
                (
                    self.connection.return_map.lookup(i),
                    rhs.connection.return_map.lookup(j),
                )
            })
            .collect();

        GcflobddTopNodeT {
            connection: ConnectionT {
                entry_point,
                return_map: ReturnMapT::new(mapped_return_map),
            },
            grammar: self.grammar,
        }
    }
}
