use std::{cell::RefCell, ops::Not};

use top_node::GcflobddTopNodeT;

use crate::{
    gcflobdd::{context::Context, top_node::GcflobddTopNode},
    grammar::Grammar,
};

mod bdd;
mod connection;
pub mod context;
mod node;
mod return_map;
mod top_node;

#[cfg(test)]
mod tests;

#[derive(Debug)]
pub struct GcflobddT<'grammar, T> {
    root: GcflobddTopNodeT<'grammar, T>,
}

pub type Gcflobdd<'grammar> = GcflobddT<'grammar, i32>;

impl<'grammar> Gcflobdd<'grammar> {
    pub fn mk_projection(
        i: usize,
        grammar: &'grammar Grammar,
        context: &RefCell<Context<'grammar>>,
    ) -> Self {
        Self::new(GcflobddTopNode::mk_distinction(i, grammar, context))
    }
    pub fn mk_true(grammar: &'grammar Grammar, context: &RefCell<Context<'grammar>>) -> Self {
        Self::new(GcflobddTopNode::mk_true(grammar, context))
    }
    pub fn mk_false(grammar: &'grammar Grammar, context: &RefCell<Context<'grammar>>) -> Self {
        Self::new(GcflobddTopNode::mk_false(grammar, context))
    }
    pub fn mk_not(&self) -> Self {
        Self::new(self.root.mk_not())
    }
    pub fn find_one_satisfiable_assignment(&self) -> Option<Vec<Option<bool>>> {
        self.root.find_one_path_to(&1)
    }
}
impl Not for Gcflobdd<'_> {
    type Output = Self;
    #[inline]
    fn not(self) -> Self {
        self.mk_not()
    }
}
impl<'grammar, T: Eq> GcflobddT<'grammar, T> {
    pub fn find_one_path_to(&self, value: &T) -> Option<Vec<Option<bool>>> {
        self.root.find_one_path_to(value)
    }
}
impl<'grammar, T> GcflobddT<'grammar, T> {
    fn new(root: GcflobddTopNodeT<'grammar, T>) -> Self {
        Self { root }
    }
}
impl<'grammar, T: Copy> GcflobddT<'grammar, T> {
    pub fn pair_product(
        &self,
        rhs: &Self,
        context: &RefCell<Context<'grammar>>,
    ) -> GcflobddT<'grammar, (T, T)> {
        GcflobddT::new(self.root.pair_product(&rhs.root, context))
    }
}
