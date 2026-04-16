use std::rc::Rc;

use crate::{
    gcflobdd::{ReturnMapT, bdd::node::BddNode, return_map::ReturnMap},
    utils::hash_cache::Rch,
};

#[derive(Debug, Clone)]
pub(crate) struct BddConnectionT<Handle> {
    pub entry_point: Rch<BddNode>,
    pub return_map: Handle,
}

impl<Handle: std::hash::Hash> std::hash::Hash for BddConnectionT<Handle> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.entry_point.hash(state);
        self.return_map.hash(state);
    }
}

impl<Handle: PartialEq> PartialEq for BddConnectionT<Handle> {
    fn eq(&self, other: &Self) -> bool {
        Rc::as_ptr(&self.entry_point) == Rc::as_ptr(&other.entry_point)
            && self.return_map == other.return_map
    }
}
impl<Handle: Eq> Eq for BddConnectionT<Handle> {}

pub(crate) type BddConnection = BddConnectionT<Rch<ReturnMap>>;
pub(crate) type BddConnectionPair = BddConnectionT<ReturnMapT<(usize, usize)>>;

impl BddConnectionPair {
    pub fn flipped(&self) -> Self {
        Self {
            entry_point: self.entry_point.clone(),
            return_map: self.return_map.iter().map(|(a, b)| (*b, *a)).collect(),
        }
    }
}
