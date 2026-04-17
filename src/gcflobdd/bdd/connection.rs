use std::rc::{Rc, Weak};

use crate::{
    gcflobdd::{ReturnMapT, bdd::node::BddNode, return_map::ReturnMap},
    utils::hash_cache::{Rch, Weakh},
};

#[derive(Debug, Clone)]
pub(crate) struct BddConnectionT<Handle> {
    pub entry_point: Rch<BddNode>,
    pub return_map: Handle,
}
pub(crate) struct WeakBddConnectionT<Handle> {
    pub entry_point: Weakh<BddNode>,
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
/// The return map should be strongly refrenced.
/// It keeps the return-map cleaning (strong count == 1) work.
pub(crate) type WeakBddConnection = WeakBddConnectionT<Rch<ReturnMap>>;
pub(crate) type BddConnectionPair = BddConnectionT<ReturnMapT<(usize, usize)>>;
pub(crate) type WeakBddConnectionPair = WeakBddConnectionT<ReturnMapT<(usize, usize)>>;

impl BddConnectionPair {
    pub fn flipped(&self) -> Self {
        Self {
            entry_point: self.entry_point.clone(),
            return_map: self.return_map.iter().map(|(a, b)| (*b, *a)).collect(),
        }
    }
}

impl<Handle> BddConnectionT<Handle> {
    pub fn into_weak(self) -> WeakBddConnectionT<Handle> {
        WeakBddConnectionT {
            entry_point: Rc::downgrade(&self.entry_point),
            return_map: self.return_map,
        }
    }
}
impl<Handle: Clone> WeakBddConnectionT<Handle> {
    pub fn upgrade(&self) -> Option<BddConnectionT<Handle>> {
        Some(BddConnectionT {
            entry_point: Weak::upgrade(&self.entry_point)?,
            return_map: self.return_map.clone(),
        })
    }
}
