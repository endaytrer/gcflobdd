use crate::gcflobdd::Gcflobdd;
use crate::gcflobdd::bdd::connection::{BddConnection, BddConnectionPair};
use crate::gcflobdd::bdd::node::BddNode;
use crate::gcflobdd::connection::{Connection, ConnectionPair};
use crate::gcflobdd::node::GcflobddNode;
use crate::gcflobdd::return_map::ReturnMap;
use crate::utils::hash_cache::{HashCached, Rch};
use std::cell::RefCell;
use std::hash::{Hash, Hasher};
use std::{collections::HashMap, rc::Rc};

#[derive(Clone, Hash, PartialEq, Eq)]
struct ReductionCacheKey(u64, u64);

impl ReductionCacheKey {
    fn new<T: Hash>(node: &Rch<T>, reduction_map: &[usize]) -> Self {
        let hash_1 = node.hash_code();
        let mut hasher = std::hash::DefaultHasher::new();
        reduction_map.iter().for_each(|&i| i.hash(&mut hasher));
        let hash_2 = hasher.finish();
        Self(hash_1, hash_2)
    }
}
type Operation = usize;
pub const OP_AND: Operation = 0usize;
pub const OP_OR: Operation = 1usize;
pub const OP_XOR: Operation = 2usize;
pub const OP_NAND: Operation = 3usize;
pub const OP_NOR: Operation = 4usize;
pub const OP_XNOR: Operation = 5usize;
pub const OP_IMPLIES: Operation = 6usize;
const OP_END: Operation = 7usize;

#[derive(Default)]
pub struct Context<'grammar> {
    // Node tables
    gcflobdd_node_table: HashMap<u64, Rch<GcflobddNode<'grammar>>>,
    bdd_node_table: HashMap<u64, Rch<BddNode>>,

    return_map_table: HashMap<u64, Rch<ReturnMap>>,
    reduce_matrix_table: HashMap<u64, Rch<Vec<usize>>>,

    // caches
    pair_product_cache: HashMap<(u64, u64), ConnectionPair<'grammar>>,
    bdd_pair_product_cache: HashMap<(u64, u64), BddConnectionPair>,
    /// (lhs, rhs, op_matrix) -> Connection
    pair_map_cache: HashMap<(u64, u64, u64), Connection<'grammar>>,
    bdd_pair_map_cache: HashMap<(u64, u64, u64), BddConnection>,
    reduction_cache: HashMap<ReductionCacheKey, Rch<GcflobddNode<'grammar>>>,
    bdd_reduction_cache: HashMap<ReductionCacheKey, Rch<BddNode>>,

    op_cache: [HashMap<(Gcflobdd<'grammar>, Gcflobdd<'grammar>), Gcflobdd<'grammar>>; OP_END],
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
    pub(super) fn add_bdd_node(&mut self, node: BddNode) -> Rch<BddNode> {
        let mut hasher = std::hash::DefaultHasher::new();
        node.hash(&mut hasher);
        let hash = hasher.finish();
        self.bdd_node_table
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
    pub(super) fn add_reduce_matrix(&mut self, op_matrix: Vec<usize>) -> Rch<Vec<usize>> {
        let mut hasher = std::hash::DefaultHasher::new();
        op_matrix.hash(&mut hasher);
        let hash = hasher.finish();
        self.reduce_matrix_table
            .entry(hash)
            .or_insert(Rc::new(HashCached::with_hash(op_matrix, hash)))
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
    pub(crate) fn get_bdd_pair_product_cache(
        &self,
        n1: &Rch<BddNode>,
        n2: &Rch<BddNode>,
    ) -> Option<BddConnectionPair> {
        let hash1 = n1.hash_code();
        let hash2 = n2.hash_code();
        if let Some(t) = self.bdd_pair_product_cache.get(&(hash1, hash2)).cloned() {
            return Some(t);
        }
        if let Some(t) = self.bdd_pair_product_cache.get(&(hash2, hash1)) {
            return Some(t.flipped());
        }
        None
    }
    pub(super) fn get_pair_map_cache(
        &self,
        n1: &Rch<GcflobddNode>,
        n2: &Rch<GcflobddNode>,
        op_matrix: &Rch<Vec<usize>>,
    ) -> Option<Connection<'grammar>> {
        let hash1 = n1.hash_code();
        let hash2 = n2.hash_code();
        let hash3 = op_matrix.hash_code();
        self.pair_map_cache.get(&(hash1, hash2, hash3)).cloned()
    }
    pub(super) fn get_bdd_pair_map_cache(
        &self,
        n1: &Rch<BddNode>,
        n2: &Rch<BddNode>,
        op_matrix: &Rch<Vec<usize>>,
    ) -> Option<BddConnection> {
        let hash1 = n1.hash_code();
        let hash2 = n2.hash_code();
        let hash3 = op_matrix.hash_code();
        self.bdd_pair_map_cache.get(&(hash1, hash2, hash3)).cloned()
    }
    pub(super) fn get_reduction_cache(
        &self,
        n: &Rch<GcflobddNode>,
        indices: &[usize],
    ) -> Option<Rch<GcflobddNode<'grammar>>> {
        let key = ReductionCacheKey::new(n, indices);
        self.reduction_cache.get(&key).cloned()
    }
    pub(super) fn get_bdd_reduction_cache(
        &self,
        n: &Rch<BddNode>,
        indices: &[usize],
    ) -> Option<Rch<BddNode>> {
        let key = ReductionCacheKey::new(n, indices);
        self.bdd_reduction_cache.get(&key).cloned()
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
    pub(super) fn set_bdd_pair_product_cache(
        &mut self,
        n1: &Rch<BddNode>,
        n2: &Rch<BddNode>,
        conn: BddConnectionPair,
    ) {
        let hash1 = n1.hash_code();
        let hash2 = n2.hash_code();
        self.bdd_pair_product_cache.insert((hash1, hash2), conn);
    }
    pub(super) fn set_pair_map_cache(
        &mut self,
        n1: &Rch<GcflobddNode>,
        n2: &Rch<GcflobddNode>,
        op_matrix: &Rch<Vec<usize>>,
        conn: Connection<'grammar>,
    ) {
        let hash1 = n1.hash_code();
        let hash2 = n2.hash_code();
        let hash3 = op_matrix.hash_code();
        self.pair_map_cache.insert((hash1, hash2, hash3), conn);
    }
    pub(super) fn set_bdd_pair_map_cache(
        &mut self,
        n1: &Rch<BddNode>,
        n2: &Rch<BddNode>,
        op_matrix: &Rch<Vec<usize>>,
        conn: BddConnection,
    ) {
        let hash1 = n1.hash_code();
        let hash2 = n2.hash_code();
        let hash3 = op_matrix.hash_code();
        self.bdd_pair_map_cache.insert((hash1, hash2, hash3), conn);
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
    pub(super) fn set_bdd_reduction_cache(
        &mut self,
        n: &Rch<BddNode>,
        indices: &[usize],
        node: Rch<BddNode>,
    ) {
        let key = ReductionCacheKey::new(n, indices);
        self.bdd_reduction_cache.insert(key, node);
    }

    // won't create a new node if it is not in the cache.
    pub(super) fn get_gcflobdd_node(
        &mut self,
        node: &Rch<GcflobddNode<'grammar>>,
    ) -> Option<Rch<GcflobddNode<'grammar>>> {
        let hash = node.hash_code();
        self.gcflobdd_node_table.get(&hash).cloned()
    }

    pub(super) fn get_op_cache<const O: Operation>(
        &self,
        lhs: Gcflobdd<'grammar>,
        rhs: Gcflobdd<'grammar>,
    ) -> Option<Gcflobdd<'grammar>> {
        self.op_cache[O].get(&(lhs, rhs)).cloned()
    }
    pub(super) fn set_op_cache<const O: Operation>(
        &mut self,
        lhs: Gcflobdd<'grammar>,
        rhs: Gcflobdd<'grammar>,
        node: Gcflobdd<'grammar>,
    ) {
        self.op_cache[O].insert((lhs, rhs), node);
    }

    pub fn node_count(&self) -> usize {
        self.gcflobdd_node_table.len()
    }
    pub fn size_estimate(&self) -> usize {
        let mut total_size = 0;
        total_size += self.gcflobdd_node_table.len()
            * (size_of::<Rch<GcflobddNode<'grammar>>>() + size_of::<GcflobddNode<'grammar>>());
        total_size +=
            self.bdd_node_table.len() * (size_of::<Rch<BddNode>>() + size_of::<BddNode>());
        total_size +=
            self.return_map_table.len() * (size_of::<Rch<ReturnMap>>() + size_of::<ReturnMap>());
        total_size += self.reduce_matrix_table.len()
            * (size_of::<Rch<Vec<usize>>>() + size_of::<Vec<usize>>());

        total_size += self.pair_product_cache.len()
            * (size_of::<(u64, u64)>() + size_of::<ConnectionPair<'grammar>>());
        total_size += self.bdd_pair_product_cache.len()
            * (size_of::<(u64, u64)>() + size_of::<BddConnectionPair>());

        total_size += self.pair_map_cache.len()
            * (size_of::<(u64, u64, u64)>() + size_of::<Connection<'grammar>>());
        total_size += self.bdd_pair_map_cache.len()
            * (size_of::<(u64, u64, u64)>() + size_of::<BddConnection>());

        total_size += self.reduction_cache.len()
            * (size_of::<ReductionCacheKey>() + size_of::<Rch<GcflobddNode<'grammar>>>());
        total_size += self.bdd_reduction_cache.len()
            * (size_of::<ReductionCacheKey>() + size_of::<Rch<BddNode>>());

        total_size += self.op_cache.iter().fold(0, |acc, cache| {
            acc + cache.len() * (3 * size_of::<Gcflobdd<'grammar>>())
        });

        total_size
    }

    fn gcflobdd_node_table_gc(node_table: &mut HashMap<u64, Rch<GcflobddNode<'grammar>>>) {
        let mut to_remove = Vec::new();
        for (k, v) in node_table.iter() {
            if Rc::strong_count(v) == 1 {
                to_remove.push(*k);
            }
        }

        while let Some(k) = to_remove.pop() {
            let v = node_table.remove(&k).unwrap();
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
                if let Some(child) = node_table.get(&child_hash)
                    && Rc::strong_count(child) == 1
                {
                    to_remove.push(child_hash);
                }
            }
        }
    }
    fn bdd_node_table_gc(node_table: &mut HashMap<u64, Rch<BddNode>>) {
        let mut to_remove = Vec::new();
        for (k, v) in node_table.iter() {
            if Rc::strong_count(v) == 1 {
                to_remove.push(*k);
            }
        }

        while let Some(k) = to_remove.pop() {
            let v = node_table.remove(&k).unwrap();
            let node = Rc::try_unwrap(v).unwrap();
            let mut children_hashes = Vec::new();
            if let BddNode::Internal(internal) = &*node {
                let zero_hash = internal.zero_branch.hash_code();
                let one_hash = internal.one_branch.hash_code();
                children_hashes.push(zero_hash);
                children_hashes.push(one_hash);
            }

            drop(node);

            for child_hash in children_hashes {
                if let Some(child) = node_table.get(&child_hash)
                    && Rc::strong_count(child) == 1
                {
                    to_remove.push(child_hash);
                }
            }
        }
    }

    /// Cleaning out the nodes that is only in the context cache,
    /// meaning that it is not in any GcflobddNode, having strong count of 1.
    /// It should be done recursively, since a node in table might have childrens that is only in the table.
    pub fn gc(&mut self) {
        self.pair_product_cache.clear();
        self.reduction_cache.clear();
        self.bdd_reduction_cache.clear();
        self.bdd_pair_product_cache.clear();
        self.op_cache.iter_mut().for_each(|map| map.clear());
        Self::gcflobdd_node_table_gc(&mut self.gcflobdd_node_table);
        Self::bdd_node_table_gc(&mut self.bdd_node_table);
        // clear return map after node table gc
        self.return_map_table = self
            .return_map_table
            .drain()
            .filter(|(_, v)| Rc::strong_count(v) > 1)
            .collect();
        self.reduce_matrix_table = self
            .reduce_matrix_table
            .drain()
            .filter(|(_, v)| Rc::strong_count(v) > 1)
            .collect();
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
