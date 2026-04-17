use crate::gcflobdd::Gcflobdd;
use crate::gcflobdd::GcflobddInt;
use crate::gcflobdd::bdd::connection::{BddConnection, BddConnectionPair};
use crate::gcflobdd::bdd::node::BddNode;
use crate::gcflobdd::connection::{Connection, ConnectionPair};
use crate::gcflobdd::node::GcflobddNode;
use crate::gcflobdd::return_map::ReturnMap;
use crate::utils::hash_cache::{HashCached, Rch};
use std::cell::RefCell;
use std::hash::{Hash, Hasher};
use std::rc::Rc;

#[cfg(feature = "fx-hash")]
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet, FxHasher as DefaultHasher};
#[cfg(not(feature = "fx-hash"))]
use std::{
    collections::{HashMap, HashSet},
    hash::DefaultHasher,
};

#[derive(Clone, Hash, PartialEq, Eq)]
struct ReductionCacheKey(usize, Vec<usize>);

impl ReductionCacheKey {
    fn new<T: Hash>(node: &Rch<T>, reduction_map: &[usize]) -> Self {
        Self(Rc::as_ptr(node) as usize, reduction_map.to_vec())
    }
}

#[repr(usize)]
pub enum BoolOperation {
    And,
    Or,
    Xor,
    Nand,
    Nor,
    Xnor,
    Implies,
    End,
}

#[repr(usize)]
pub enum IntOperation {
    Add,
    Sub,
    Mul,
    End,
}

#[derive(Default)]
pub struct Context<'grammar> {
    // Node tables
    gcflobdd_node_table: HashSet<Rch<GcflobddNode<'grammar>>>,
    bdd_node_table: HashSet<Rch<BddNode>>,

    return_map_table: HashSet<Rch<ReturnMap>>,
    reduce_matrix_table: HashSet<Rch<Vec<usize>>>,

    // caches
    pair_product_cache: HashMap<(usize, usize), ConnectionPair<'grammar>>,
    bdd_pair_product_cache: HashMap<(usize, usize), BddConnectionPair>,
    /// (lhs, rhs, op_matrix) -> Connection
    pair_map_cache: HashMap<(usize, usize, usize), Connection<'grammar>>,
    bdd_pair_map_cache: HashMap<(usize, usize, usize), BddConnection>,
    reduction_cache: HashMap<ReductionCacheKey, Rch<GcflobddNode<'grammar>>>,
    bdd_reduction_cache: HashMap<ReductionCacheKey, Rch<BddNode>>,

    op_cache: [HashMap<(Gcflobdd<'grammar>, Gcflobdd<'grammar>), Gcflobdd<'grammar>>;
        BoolOperation::End as usize],
    int_op_cache: [HashMap<(GcflobddInt<'grammar>, GcflobddInt<'grammar>), GcflobddInt<'grammar>>;
        IntOperation::End as usize],
}
impl<'grammar> Context<'grammar> {
    pub fn new() -> RefCell<Self> {
        RefCell::new(Self::default())
    }
    pub(super) fn add_gcflobdd_node(
        &mut self,
        node: GcflobddNode<'grammar>,
    ) -> Rch<GcflobddNode<'grammar>> {
        let mut hasher = DefaultHasher::default();
        node.hash(&mut hasher);
        let hash = hasher.finish();
        let hc_node = HashCached::with_hash(node, hash);
        if let Some(rch) = self.gcflobdd_node_table.get(&hc_node) {
            return rch.clone();
        }
        let rch = Rc::new(hc_node);
        self.gcflobdd_node_table.insert(rch.clone());
        rch
    }
    pub(super) fn add_bdd_node(&mut self, node: BddNode) -> Rch<BddNode> {
        let mut hasher = DefaultHasher::default();
        node.hash(&mut hasher);
        let hash = hasher.finish();
        let hc_node = HashCached::with_hash(node, hash);
        if let Some(rch) = self.bdd_node_table.get(&hc_node) {
            return rch.clone();
        }
        let rch = Rc::new(hc_node);
        self.bdd_node_table.insert(rch.clone());
        rch
    }
    pub(super) fn add_return_map(&mut self, return_map: ReturnMap) -> Rch<ReturnMap> {
        let mut hasher = DefaultHasher::default();
        return_map.hash(&mut hasher);
        let hash = hasher.finish();
        let hc_node = HashCached::with_hash(return_map, hash);
        if let Some(rch) = self.return_map_table.get(&hc_node) {
            return rch.clone();
        }
        let rch = Rc::new(hc_node);
        self.return_map_table.insert(rch.clone());
        rch
    }
    pub(super) fn add_reduce_matrix(&mut self, op_matrix: Vec<usize>) -> Rch<Vec<usize>> {
        let mut hasher = DefaultHasher::default();
        op_matrix.hash(&mut hasher);
        let hash = hasher.finish();
        let hc_node = HashCached::with_hash(op_matrix, hash);
        if let Some(rch) = self.reduce_matrix_table.get(&hc_node) {
            return rch.clone();
        }
        let rch = Rc::new(hc_node);
        self.reduce_matrix_table.insert(rch.clone());
        rch
    }
    pub(super) fn get_pair_product_cache(
        &self,
        n1: &Rch<GcflobddNode>,
        n2: &Rch<GcflobddNode>,
    ) -> Option<ConnectionPair<'grammar>> {
        let hash1 = Rc::as_ptr(n1) as usize;
        let hash2 = Rc::as_ptr(n2) as usize;
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
        let hash1 = Rc::as_ptr(n1) as usize;
        let hash2 = Rc::as_ptr(n2) as usize;
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
        let hash1 = Rc::as_ptr(n1) as usize;
        let hash2 = Rc::as_ptr(n2) as usize;
        let hash3 = Rc::as_ptr(op_matrix) as usize;
        self.pair_map_cache.get(&(hash1, hash2, hash3)).cloned()
    }
    pub(super) fn get_bdd_pair_map_cache(
        &self,
        n1: &Rch<BddNode>,
        n2: &Rch<BddNode>,
        op_matrix: &Rch<Vec<usize>>,
    ) -> Option<BddConnection> {
        let hash1 = Rc::as_ptr(n1) as usize;
        let hash2 = Rc::as_ptr(n2) as usize;
        let hash3 = Rc::as_ptr(op_matrix) as usize;
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
        let hash1 = Rc::as_ptr(n1) as usize;
        let hash2 = Rc::as_ptr(n2) as usize;
        self.pair_product_cache.insert((hash1, hash2), conn);
    }
    pub(super) fn set_bdd_pair_product_cache(
        &mut self,
        n1: &Rch<BddNode>,
        n2: &Rch<BddNode>,
        conn: BddConnectionPair,
    ) {
        let hash1 = Rc::as_ptr(n1) as usize;
        let hash2 = Rc::as_ptr(n2) as usize;
        self.bdd_pair_product_cache.insert((hash1, hash2), conn);
    }
    pub(super) fn set_pair_map_cache(
        &mut self,
        n1: &Rch<GcflobddNode>,
        n2: &Rch<GcflobddNode>,
        op_matrix: &Rch<Vec<usize>>,
        conn: Connection<'grammar>,
    ) {
        let hash1 = Rc::as_ptr(n1) as usize;
        let hash2 = Rc::as_ptr(n2) as usize;
        let hash3 = Rc::as_ptr(op_matrix) as usize;
        self.pair_map_cache.insert((hash1, hash2, hash3), conn);
    }
    pub(super) fn set_bdd_pair_map_cache(
        &mut self,
        n1: &Rch<BddNode>,
        n2: &Rch<BddNode>,
        op_matrix: &Rch<Vec<usize>>,
        conn: BddConnection,
    ) {
        let hash1 = Rc::as_ptr(n1) as usize;
        let hash2 = Rc::as_ptr(n2) as usize;
        let hash3 = Rc::as_ptr(op_matrix) as usize;
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
        self.gcflobdd_node_table.get(node.as_ref()).cloned()
    }

    pub(super) fn get_op_cache<const O: usize>(
        &self,
        lhs: Gcflobdd<'grammar>,
        rhs: Gcflobdd<'grammar>,
    ) -> Option<Gcflobdd<'grammar>> {
        self.op_cache[O].get(&(lhs, rhs)).cloned()
    }
    pub(super) fn get_int_op_cache<const O: usize>(
        &self,
        lhs: GcflobddInt<'grammar>,
        rhs: GcflobddInt<'grammar>,
    ) -> Option<GcflobddInt<'grammar>> {
        self.int_op_cache[O].get(&(lhs, rhs)).cloned()
    }
    pub(super) fn set_op_cache<const O: usize>(
        &mut self,
        lhs: Gcflobdd<'grammar>,
        rhs: Gcflobdd<'grammar>,
        node: Gcflobdd<'grammar>,
    ) {
        self.op_cache[O].insert((lhs, rhs), node);
    }

    pub(super) fn set_int_op_cache<const O: usize>(
        &mut self,
        lhs: GcflobddInt<'grammar>,
        rhs: GcflobddInt<'grammar>,
        node: GcflobddInt<'grammar>,
    ) {
        self.int_op_cache[O].insert((lhs, rhs), node);
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

    fn gcflobdd_node_table_gc(node_table: &mut HashSet<Rch<GcflobddNode<'grammar>>>) {
        let mut to_remove = Vec::new();
        for v in node_table.iter() {
            if Rc::strong_count(v) == 1 {
                to_remove.push(v.clone());
            }
        }

        while let Some(v) = to_remove.pop() {
            if !node_table.remove(&*v) {
                continue;
            }
            let node = Rc::try_unwrap(v).unwrap();
            let mut children = Vec::new();
            if let crate::gcflobdd::node::GcflobddNodeType::Internal(internal) = &node.node {
                for layer in &internal.connections {
                    for conn in layer {
                        children.push(conn.entry_point.clone());
                    }
                }
            }

            drop(node);

            for child in children {
                if node_table.contains(&*child) && Rc::strong_count(&child) == 2 {
                    to_remove.push(child);
                }
            }
        }
    }

    fn bdd_node_table_gc(node_table: &mut HashSet<Rch<BddNode>>) {
        let mut to_remove = Vec::new();
        for v in node_table.iter() {
            if Rc::strong_count(v) == 1 {
                to_remove.push(v.clone());
            }
        }

        while let Some(v) = to_remove.pop() {
            if !node_table.remove(&*v) {
                continue;
            }
            let node = Rc::try_unwrap(v).unwrap();
            let mut children = Vec::new();
            if let BddNode::Internal(internal) = &*node {
                children.push(internal.zero_branch.clone());
                children.push(internal.one_branch.clone());
            }

            drop(node);

            for child in children {
                if node_table.contains(&*child) && Rc::strong_count(&child) == 2 {
                    to_remove.push(child);
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
        self.int_op_cache.iter_mut().for_each(|map| map.clear());
        self.pair_map_cache.clear();
        self.bdd_pair_map_cache.clear();
        Self::gcflobdd_node_table_gc(&mut self.gcflobdd_node_table);
        Self::bdd_node_table_gc(&mut self.bdd_node_table);
        // clear return map after node table gc
        self.return_map_table = self
            .return_map_table
            .drain()
            .filter(|v| Rc::strong_count(v) > 1)
            .collect();
        self.reduce_matrix_table = self
            .reduce_matrix_table
            .drain()
            .filter(|v| Rc::strong_count(v) > 1)
            .collect();
    }
}
impl<'grammar> std::fmt::Debug for Context<'grammar> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("Context");
        for v in &self.gcflobdd_node_table {
            s.field(
                format!(
                    "[#{:016x?} @ 0x{:016x}]",
                    v.hash_code(),
                    Rc::as_ptr(v) as usize
                )
                .as_str(),
                v,
            );
        }
        s.finish()
    }
}
