use crate::gcflobdd::connection::ConnectionPair;
use crate::gcflobdd::node::GcflobddNode;
use crate::gcflobdd::return_map::ReturnMap;
use crate::utils::hash_cache::{HashCached, Rch};
use std::cell::RefCell;
use std::hash::{Hash, Hasher};
use std::{collections::HashMap, rc::Rc};

#[derive(Clone, Hash, PartialEq, Eq)]
struct ReductionCacheKey(u64, u64);

impl ReductionCacheKey {
    fn new(node: &Rch<GcflobddNode<'_>>, reduction_map: &[usize]) -> Self {
        let hash_1 = node.hash_code();
        let mut hasher = std::hash::DefaultHasher::new();
        reduction_map.iter().for_each(|&i| i.hash(&mut hasher));
        let hash_2 = hasher.finish();
        Self(hash_1, hash_2)
    }
}

#[derive(Default)]
pub struct Context<'grammar> {
    gcflobdd_node_table: HashMap<u64, Rch<GcflobddNode<'grammar>>>,
    return_map_table: HashMap<u64, Rch<ReturnMap>>,
    pair_product_cache: HashMap<(u64, u64), ConnectionPair<'grammar>>,
    reduction_cache: HashMap<ReductionCacheKey, Rch<GcflobddNode<'grammar>>>,
}
impl<'grammar> Context<'grammar> {
    pub fn new() -> RefCell<Self> {
        RefCell::new(Self::default())
    }
    pub(super) fn add_gcflobdd_node(
        &mut self,
        node: GcflobddNode<'grammar>,
    ) -> Rch<GcflobddNode<'grammar>> {
        let mut hasher = std::hash::DefaultHasher::new();
        node.hash(&mut hasher);
        let hash = hasher.finish();
        self.gcflobdd_node_table
            .entry(hash)
            .or_insert(Rc::new(HashCached::with_hash(node, hash)))
            .clone()
    }
    pub(super) fn add_return_map(&mut self, return_map: ReturnMap) -> Rch<ReturnMap> {
        let mut hasher = std::hash::DefaultHasher::new();
        return_map.hash(&mut hasher);
        let hash = hasher.finish();
        self.return_map_table
            .entry(hash)
            .or_insert(Rc::new(HashCached::with_hash(return_map, hash)))
            .clone()
    }

    pub(super) fn get_pair_product_cache(
        &self,
        n1: &Rch<GcflobddNode>,
        n2: &Rch<GcflobddNode>,
    ) -> Option<ConnectionPair<'grammar>> {
        let hash1 = n1.hash_code();
        let hash2 = n2.hash_code();
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
        n: &Rch<GcflobddNode>,
        indices: &[usize],
    ) -> Option<Rch<GcflobddNode<'grammar>>> {
        let key = ReductionCacheKey::new(n, indices);
        self.reduction_cache.get(&key).cloned()
    }
    pub(super) fn set_pair_product_cache(
        &mut self,
        n1: &Rch<GcflobddNode>,
        n2: &Rch<GcflobddNode>,
        conn: ConnectionPair<'grammar>,
    ) {
        let hash1 = n1.hash_code();
        let hash2 = n2.hash_code();
        self.pair_product_cache.insert((hash1, hash2), conn);
    }
    pub(super) fn set_reduction_cache(
        &mut self,
        n: &Rch<GcflobddNode>,
        indices: &[usize],
        node: Rch<GcflobddNode<'grammar>>,
    ) {
        let key = ReductionCacheKey::new(n, indices);
        self.reduction_cache.insert(key, node);
    }

    // won't create a new node if it is not in the cache.
    pub(super) fn get_gcflobdd_node(
        &mut self,
        node: &Rch<GcflobddNode<'grammar>>,
    ) -> Option<Rch<GcflobddNode<'grammar>>> {
        let hash = node.hash_code();
        self.gcflobdd_node_table.get(&hash).cloned()
    }

    pub fn node_count(&self) -> usize {
        self.gcflobdd_node_table.len()
    }

    /// Cleaning out the nodes that is only in the context cache,
    /// meaning that it is not in any GcflobddNode, having strong count of 1.
    /// It should be done recursively, since a node in table might have childrens that is only in the table.
    pub fn gc(&mut self) {
        self.return_map_table.clear();
        self.pair_product_cache.clear();
        self.reduction_cache.clear();
        let mut to_remove = Vec::new();
        for (k, v) in &self.gcflobdd_node_table {
            if Rc::strong_count(v) == 1 {
                to_remove.push(*k);
            }
        }

        while let Some(k) = to_remove.pop() {
            let v = self.gcflobdd_node_table.remove(&k).unwrap();
            let node = Rc::try_unwrap(v).unwrap();
            let mut children_hashes = Vec::new();
            if let crate::gcflobdd::node::GcflobddNodeType::Internal(internal) = &node.node {
                for layer in &internal.connections {
                    for conn in layer {
                        children_hashes.push(conn.entry_point.hash_code());
                    }
                }
            }
            children_hashes.sort_unstable();
            children_hashes.dedup();

            drop(node);

            for child_hash in children_hashes {
                if let Some(child) = self.gcflobdd_node_table.get(&child_hash)
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
        for (k, v) in &self.gcflobdd_node_table {
            s.field(
                format!("[#{:016x?} @ 0x{:016x}]", k, Rc::as_ptr(v) as usize).as_str(),
                v,
            );
        }
        s.finish()
    }
}
