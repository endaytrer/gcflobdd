mod bdd;
mod connection;
pub mod context;
mod node;
mod return_map;
#[cfg(test)]
mod tests;
use std::cell::RefCell;
use std::ops::Not;
use std::rc::Rc;

use crate::gcflobdd::context::Context;
use crate::gcflobdd::node::GcflobddNode;
use crate::gcflobdd::return_map::{complement, inverse_lookup};
use crate::grammar::Grammar;
use connection::ConnectionT;
use return_map::ReturnMapT;

#[derive(Clone)]
pub struct GcflobddT<'grammar, T> {
    connection: ConnectionT<'grammar, ReturnMapT<T>>,
    grammar: &'grammar Grammar,
}
impl<'grammar, T: std::fmt::Debug> std::fmt::Debug for GcflobddT<'grammar, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("GcflobddT").field(&self.connection).finish()
    }
}

pub type Gcflobdd<'grammar> = GcflobddT<'grammar, bool>;

impl<'grammar> Gcflobdd<'grammar> {
    fn new(
        connection: ConnectionT<'grammar, ReturnMapT<bool>>,
        grammar: &'grammar Grammar,
    ) -> Self {
        Self {
            connection,
            grammar,
        }
    }

    pub fn mk_projection(
        i: usize,
        grammar: &'grammar Grammar,
        context: &RefCell<Context<'grammar>>,
    ) -> Self {
        let node = GcflobddNode::mk_distinction(i, &grammar.root, context);
        Self::new(
            ConnectionT {
                entry_point: node,
                return_map: vec![false, true],
            },
            grammar,
        )
    }
    pub fn mk_true(grammar: &'grammar Grammar, context: &RefCell<Context<'grammar>>) -> Self {
        let node = GcflobddNode::mk_no_distinction(&grammar.root, context);
        Self::new(
            ConnectionT {
                entry_point: node,
                return_map: vec![true],
            },
            grammar,
        )
    }
    pub fn mk_false(grammar: &'grammar Grammar, context: &RefCell<Context<'grammar>>) -> Self {
        let node = GcflobddNode::mk_no_distinction(&grammar.root, context);
        Self::new(
            ConnectionT {
                entry_point: node,
                return_map: vec![false],
            },
            grammar,
        )
    }
    pub fn mk_not(&self) -> Self {
        let mut connection = self.connection.clone();
        connection.return_map = complement(&connection.return_map);
        Self {
            connection,
            grammar: self.grammar,
        }
    }
    pub fn mk_and(&self, rhs: &Self, context: &RefCell<Context<'grammar>>) -> Self {
        self.mk_op(rhs, |a, b| *a && *b, context)
    }
    pub fn mk_or(&self, rhs: &Self, context: &RefCell<Context<'grammar>>) -> Self {
        self.mk_op(rhs, |a, b| *a || *b, context)
    }
    pub fn mk_xor(&self, rhs: &Self, context: &RefCell<Context<'grammar>>) -> Self {
        self.mk_op(rhs, |a, b| *a ^ *b, context)
    }
    pub fn mk_nand(&self, rhs: &Self, context: &RefCell<Context<'grammar>>) -> Self {
        self.mk_op(rhs, |a, b| !(*a && *b), context)
    }
    pub fn mk_nor(&self, rhs: &Self, context: &RefCell<Context<'grammar>>) -> Self {
        self.mk_op(rhs, |a, b| !(*a || *b), context)
    }
    pub fn mk_xnor(&self, rhs: &Self, context: &RefCell<Context<'grammar>>) -> Self {
        self.mk_op(rhs, |a, b| !(*a ^ *b), context)
    }
    pub fn mk_implies(&self, rhs: &Self, context: &RefCell<Context<'grammar>>) -> Self {
        self.mk_op(rhs, |a, b| !(*a) || *b, context)
    }

    pub fn find_one_satisfiable_assignment(&self) -> Option<Vec<Option<bool>>> {
        self.find_one_path_to(&true)
    }
}

impl<'grammar, T: Eq> GcflobddT<'grammar, T> {
    pub fn find_one_path_to(&self, value: &T) -> Option<Vec<Option<bool>>> {
        let index = inverse_lookup(&self.connection.return_map, value)?;
        Some(self.connection.entry_point.find_one_path_to(index))
    }
}
impl<'grammar, T: Copy> GcflobddT<'grammar, T> {
    pub fn pair_product(
        &self,
        rhs: &Self,
        context: &RefCell<Context<'grammar>>,
    ) -> GcflobddT<'grammar, (T, T)> {
        let ConnectionT {
            entry_point,
            return_map,
        } = GcflobddNode::pair_product(
            &self.connection.entry_point,
            &rhs.connection.entry_point,
            context,
        );
        let mapped_return_map = return_map
            .into_iter()
            .map(|(i, j)| (self.connection.return_map[i], rhs.connection.return_map[j]))
            .collect();

        GcflobddT {
            connection: ConnectionT {
                entry_point,
                return_map: mapped_return_map,
            },
            grammar: self.grammar,
        }
    }
}

impl<'grammar, T> GcflobddT<'grammar, T> {
    pub fn map<V: Eq>(
        &self,
        f: impl Fn(&T) -> V,
        context: &RefCell<Context<'grammar>>,
    ) -> GcflobddT<'grammar, V> {
        let mut new_return_handle = vec![];
        let mapping_array = self
            .connection
            .return_map
            .iter()
            .map(|t| {
                let v = f(t);
                new_return_handle
                    .iter()
                    .position(|x| *x == v)
                    .unwrap_or_else(|| {
                        new_return_handle.push(v);
                        new_return_handle.len() - 1
                    })
            })
            .collect::<Vec<_>>();
        let entry_point =
            GcflobddNode::reduce(&self.connection.entry_point, mapping_array, context);

        GcflobddT {
            connection: ConnectionT {
                entry_point,
                return_map: new_return_handle,
            },
            grammar: self.grammar,
        }
    }
}
impl<'grammar, T: Copy + Eq> GcflobddT<'grammar, T> {
    pub fn mk_op(
        &self,
        rhs: &Self,
        op: impl Fn(&T, &T) -> T,
        context: &RefCell<Context<'grammar>>,
    ) -> Self {
        let start_pair = std::time::Instant::now();
        let pair_product = self.pair_product(rhs, context);
        let pair_time = start_pair.elapsed();

        let start_map = std::time::Instant::now();
        let mapped = pair_product.map(|(a, b)| op(a, b), context);
        let map_time = start_map.elapsed();

        println!(
            "pair_product took: {:?}, map took: {:?}",
            pair_time, map_time
        );
        mapped
    }
}

impl Not for Gcflobdd<'_> {
    type Output = Self;
    #[inline]
    fn not(self) -> Self {
        self.mk_not()
    }
}

impl PartialEq for Gcflobdd<'_> {
    fn eq(&self, other: &Self) -> bool {
        Rc::as_ptr(&self.connection.entry_point) == Rc::as_ptr(&other.connection.entry_point)
            && self.connection.return_map == other.connection.return_map
    }
}
