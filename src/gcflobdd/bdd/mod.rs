use std::cell::RefCell;

use crate::{
    gcflobdd::{
        bdd::{connection::BddConnectionPair, node::BddNode},
        context::Context,
    },
    utils::hash_cache::Rch,
};

pub(super) mod connection;
pub(super) mod node;

#[derive(Debug, Hash)]
pub(super) struct Bdd(pub Rch<BddNode>);

impl Bdd {
    pub fn mk_projection(i: usize, context: &RefCell<Context<'_>>) -> Self {
        Self(BddNode::mk_distinction(i, context))
    }
    pub fn find_one_path_to(&self, value: usize, bdd_size: usize) -> Option<Vec<Option<bool>>> {
        self.0.find_one_path_to(value, 0, bdd_size)
    }
    pub fn pair_product(&self, rhs: &Self, context: &RefCell<Context<'_>>) -> BddConnectionPair {
        BddNode::pair_product(&self.0, &rhs.0, context)
    }
    pub fn reduce(&self, reduce_map: &[usize], num_exits: usize, context: &RefCell<Context<'_>>) -> Self {
        Self(BddNode::reduce(&self.0, reduce_map, num_exits, context))
    }
}
