use std::{cell::RefCell, rc::Rc};

use crate::{
    gcflobdd::{
        bdd::{
            connection::{BddConnection, BddConnectionPair},
            node::BddNode,
        },
        context::Context,
    },
    utils::hash_cache::Rch,
};

pub(super) mod connection;
pub(super) mod node;

#[cfg(test)]
mod tests;

#[derive(Debug)]
pub(super) struct Bdd(pub Rch<BddNode>);

impl Bdd {
    pub fn mk_projection(i: usize, context: &RefCell<Context<'_>>) -> Self {
        Self(BddNode::mk_distinction(i, context))
    }
    pub fn mk_hadamard_2(context: &RefCell<Context<'_>>) -> Self {
        Self(BddNode::mk_hadamard_2(context))
    }
    #[cfg(test)]
    pub(super) fn mk_inverse_projection(i: usize, context: &RefCell<Context<'_>>) -> Self {
        Self(BddNode::mk_inverse_distinction(i, context))
    }
    #[cfg(test)]
    pub(super) fn mk_false(context: &RefCell<Context<'_>>) -> Self {
        Self(context.borrow().add_bdd_node(BddNode::Terminal(0)))
    }
    #[cfg(test)]
    pub(super) fn mk_true(context: &RefCell<Context<'_>>) -> Self {
        Self(context.borrow().add_bdd_node(BddNode::Terminal(1)))
    }
    pub fn find_one_path_to(&self, value: usize, bdd_size: usize) -> Option<Vec<Option<bool>>> {
        self.0.find_one_path_to(value, 0, bdd_size)
    }
    pub fn pair_product(
        &self,
        rhs: &Self,
        lhs_num_exits: usize,
        rhs_num_exits: usize,
        context: &RefCell<Context<'_>>,
    ) -> BddConnectionPair {
        BddNode::pair_product(&self.0, &rhs.0, lhs_num_exits, rhs_num_exits, context)
    }
    pub fn reduce(
        &self,
        reduce_map: &[usize],
        num_exits: usize,
        context: &RefCell<Context<'_>>,
    ) -> Self {
        Self(BddNode::reduce(&self.0, reduce_map, num_exits, context))
    }
    pub fn pair_map(
        &self,
        rhs: &Self,
        reduce_map: &Rch<Vec<usize>>,
        lhs_num_exits: usize,
        rhs_num_exits: usize,
        context: &RefCell<Context<'_>>,
    ) -> BddConnection {
        BddNode::pair_map(
            &self.0,
            &rhs.0,
            reduce_map,
            lhs_num_exits,
            rhs_num_exits,
            context,
        )
    }
}

impl PartialEq for Bdd {
    fn eq(&self, other: &Self) -> bool {
        Rc::as_ptr(&self.0) == Rc::as_ptr(&other.0)
    }
}
impl Eq for Bdd {}

impl std::hash::Hash for Bdd {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash_code().hash(state);
    }
}
