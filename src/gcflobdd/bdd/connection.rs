use std::rc::Rc;

use crate::{
    gcflobdd::{ReturnMapT, bdd::node::BddNode, return_map::ReturnMap},
    utils::hash_cache::Rch,
};

#[derive(Debug, Clone, Hash)]
pub(crate) struct BddConnectionT<Handle> {
    pub entry_point: Rch<BddNode>,
    pub return_map: Handle,
}

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

impl PartialEq for BddConnection {
    fn eq(&self, other: &Self) -> bool {
        Rc::as_ptr(&self.entry_point) == Rc::as_ptr(&other.entry_point)
            && Rc::as_ptr(&self.return_map) == Rc::as_ptr(&other.return_map)
    }
}
impl Eq for BddConnection {}
