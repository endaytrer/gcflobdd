use crate::gcflobdd::connection::ConnectionPair;
use crate::gcflobdd::node::GcflobddNode;
use crate::gcflobdd::{ConnectionT, ReturnMapT};
use std::cell::RefCell;
use std::hash::{Hash, Hasher};
use std::{collections::HashMap, rc::Rc};

#[derive(Default)]
pub struct Context<'grammar> {
    gcflobdd_node_cache: HashMap<u64, Rc<GcflobddNode<'grammar>>>,
    pair_product_cache: HashMap<(u64, u64), ConnectionPair<'grammar>>,
    reduction_cache: HashMap<(u64, Vec<usize>), Rc<GcflobddNode<'grammar>>>,
}
impl<'grammar> Context<'grammar> {
    pub fn new() -> RefCell<Self> {
        RefCell::new(Self::default())
    }
    pub(super) fn add_gcflobdd_node(
        &mut self,
        node: GcflobddNode<'grammar>,
    ) -> Rc<GcflobddNode<'grammar>> {
        let mut hasher = std::hash::DefaultHasher::new();
        node.hash(&mut hasher);
        let hash = hasher.finish();
        self.gcflobdd_node_cache
            .entry(hash)
            .or_insert(Rc::new(node))
            .clone()
    }

    pub(super) fn get_pair_product_cache(
        &self,
        n1: &GcflobddNode,
        n2: &GcflobddNode,
    ) -> Option<ConnectionT<'grammar, ReturnMapT<(usize, usize)>>> {
        let mut hasher = std::hash::DefaultHasher::new();
        n1.hash(&mut hasher);
        n2.hash(&mut hasher);
        let hash1 = hasher.finish();
        let hash2 = hasher.finish();
        if let Some(t) = self.pair_product_cache.get(&(hash1, hash2)).cloned() {
            return Some(t);
        }
        if let Some(t) = self.pair_product_cache.get(&(hash2, hash1)) {
            return Some(t.flipped());
        }
        None
    }
    pub(super) fn get_reduction_cache(
        &self,
        n: &GcflobddNode,
        indices: &[usize],
    ) -> Option<Rc<GcflobddNode<'grammar>>> {
        let mut hasher = std::hash::DefaultHasher::new();
        n.hash(&mut hasher);
        let hash = hasher.finish();
        self.reduction_cache.get(&(hash, indices.to_vec())).cloned()
    }
    pub(super) fn set_pair_product_cache(
        &mut self,
        n1: &GcflobddNode,
        n2: &GcflobddNode,
        conn: ConnectionT<'grammar, ReturnMapT<(usize, usize)>>,
    ) {
        let mut hasher = std::hash::DefaultHasher::new();
        n1.hash(&mut hasher);
        n2.hash(&mut hasher);
        let hash1 = hasher.finish();
        let hash2 = hasher.finish();
        self.pair_product_cache.insert((hash1, hash2), conn);
    }
    pub(super) fn set_reduction_cache(
        &mut self,
        n: &GcflobddNode,
        indices: Vec<usize>,
        node: Rc<GcflobddNode<'grammar>>,
    ) {
        let mut hasher = std::hash::DefaultHasher::new();
        n.hash(&mut hasher);
        let hash = hasher.finish();
        self.reduction_cache.insert((hash, indices), node);
    }

    // won't create a new node if it is not in the cache.
    pub(super) fn get_gcflobdd_node(
        &mut self,
        node: &GcflobddNode<'grammar>,
    ) -> Option<Rc<GcflobddNode<'grammar>>> {
        let mut hasher = std::hash::DefaultHasher::new();
        node.hash(&mut hasher);
        let hash = hasher.finish();
        self.gcflobdd_node_cache.get(&hash).cloned()
    }

    pub fn node_count(&self) -> usize {
        self.gcflobdd_node_cache.len()
    }

    /// Cleaning out the nodes that is only in the context cache,
    /// meaning that it is not in any GcflobddNode, having strong count of 1.
    /// It should be done recursively, since a node in table might have childrens that is only in the table.
    pub fn gc(&mut self) {
        self.pair_product_cache.clear();
        self.reduction_cache.clear();
        let mut to_remove = Vec::new();
        for (k, v) in &self.gcflobdd_node_cache {
            if Rc::strong_count(v) == 1 {
                to_remove.push(*k);
            }
        }

        while let Some(k) = to_remove.pop() {
            let v = self.gcflobdd_node_cache.remove(&k).unwrap();
            let node = Rc::try_unwrap(v).unwrap();
            let mut children_hashes = Vec::new();
            if let crate::gcflobdd::node::GcflobddNodeType::Internal(internal) = &node.node {
                for layer in &internal.connections {
                    for conn in layer {
                        let mut hasher = std::hash::DefaultHasher::new();
                        conn.entry_point.hash(&mut hasher);
                        children_hashes.push(hasher.finish());
                    }
                }
            }
            children_hashes.sort_unstable();
            children_hashes.dedup();

            drop(node);

            for child_hash in children_hashes {
                if let Some(child) = self.gcflobdd_node_cache.get(&child_hash)
                    && Rc::strong_count(child) == 1
                {
                    to_remove.push(child_hash);
                }
            }
        }
    }
}
impl<'grammar> std::fmt::Debug for Context<'grammar> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("Context");
        for (k, v) in &self.gcflobdd_node_cache {
            s.field(
                format!("[#{:016x?} @ 0x{:016x}]", k, Rc::as_ptr(v) as usize).as_str(),
                v,
            );
        }
        s.finish()
    }
}
