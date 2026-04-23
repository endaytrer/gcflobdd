use crate::gcflobdd::Gcflobdd;
use crate::gcflobdd::GcflobddInt;
use crate::gcflobdd::bdd::connection::{BddConnection, BddConnectionPair};
use crate::gcflobdd::bdd::node::BddNode;
use crate::gcflobdd::connection::{Connection, ConnectionPair};
use crate::gcflobdd::node::GcflobddNode;
use crate::gcflobdd::return_map::ReturnMap;
use crate::gcflobdd::soa::HashSetValueView;
use crate::gcflobdd::soa::RefGcflobddNode;
use crate::gcflobdd::soa::SoaGcflobddNode;
use crate::gcflobdd::soa::SoaNodeTable;
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

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct ReductionCacheKey(usize, Vec<usize>);

impl ReductionCacheKey {
    fn new(node: usize, reduction_map: &[usize]) -> Self {
        Self(node, reduction_map.to_vec())
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

/// All Context state that is mutated during normal operations, behind a single
/// `RefCell`. Borrows are always short-scoped (one expression), so there is no
/// dynamic-borrow conflict with concurrent recursive callers — each caller
/// takes the borrow, mutates, and drops it before recursing further.
#[derive(Debug, Default)]
struct ContextCaches<'grammar> {
    bdd_node_table: HashSet<Rch<BddNode>>,

    return_map_table: HashSet<Rch<ReturnMap>>,
    reduce_matrix_table: HashSet<Rch<Vec<usize>>>,

    pair_product_cache: HashMap<(usize, usize), ConnectionPair<'grammar>>,
    bdd_pair_product_cache: HashMap<(usize, usize), BddConnectionPair>,
    /// (lhs, rhs, op_matrix) -> Connection
    pair_map_cache: HashMap<(usize, usize, usize), Connection<'grammar>>,
    bdd_pair_map_cache: HashMap<(usize, usize, usize), BddConnection>,
    reduction_cache: HashMap<ReductionCacheKey, usize>,
    bdd_reduction_cache: HashMap<ReductionCacheKey, Rch<BddNode>>,

    op_cache: [HashMap<(Gcflobdd<'grammar>, Gcflobdd<'grammar>), Gcflobdd<'grammar>>;
        BoolOperation::End as usize],
    int_op_cache: [HashMap<(GcflobddInt<'grammar>, GcflobddInt<'grammar>), GcflobddInt<'grammar>>;
        IntOperation::End as usize],
}

#[derive(Debug, Default)]
pub struct Context<'grammar> {
    /// Pointer-stable chunked SoA table. `add_gcflobdd_node` appends through `&self`
    /// (the table's own `insert` is an `UnsafeCell`-backed append-only path).
    /// Views from `get_gcflobdd_node_view` remain valid across any number of
    /// subsequent inserts because chunk heap buffers never move.
    gcflobdd_node_table: SoaNodeTable<SoaGcflobddNode<'grammar>>,

    caches: RefCell<ContextCaches<'grammar>>,
}

impl<'grammar> Context<'grammar> {
    pub fn new() -> RefCell<Self> {
        RefCell::new(Self::default())
    }
    pub(super) fn add_gcflobdd_node(&self, node: GcflobddNode<'grammar>) -> usize {
        let mut hasher = DefaultHasher::default();
        node.hash(&mut hasher);
        let hash = hasher.finish();
        let ref_node = RefGcflobddNode::from(&node);
        if let Some(rch) = self.gcflobdd_node_table.get_index(hash, ref_node) {
            return rch;
        }
        let rc_node = HashCached::with_hash(node, hash);
        // SAFETY: single-threaded; the whole crate drives the node table from one
        // thread at a time, and no two `add_gcflobdd_node` calls overlap (the
        // call ends before returning the new slot index).
        unsafe { self.gcflobdd_node_table.insert(rc_node) }
    }
    /// View a slot. References inside the view point into chunk heap buffers
    /// that are pointer-stable for the lifetime of `&self`, so holding a view
    /// across further `add_gcflobdd_node` calls is safe.
    pub(super) fn get_gcflobdd_node_view<'a>(
        &'a self,
        index: usize,
    ) -> HashSetValueView<RefGcflobddNode<'a, 'grammar>> {
        // SAFETY: callers only pass indices returned by a prior
        // `add_gcflobdd_node` — those slots are initialised and never freed
        // outside `&mut self` GC paths.
        unsafe { self.gcflobdd_node_table.get_view(index) }
    }
    pub(super) fn add_bdd_node(&self, node: BddNode) -> Rch<BddNode> {
        let mut hasher = DefaultHasher::default();
        node.hash(&mut hasher);
        let hash = hasher.finish();
        let hc_node = HashCached::with_hash(node, hash);
        let mut caches = self.caches.borrow_mut();
        if let Some(rch) = caches.bdd_node_table.get(&hc_node) {
            return rch.clone();
        }
        let rch = Rc::new(hc_node);
        caches.bdd_node_table.insert(rch.clone());
        rch
    }
    pub(super) fn add_return_map(&self, return_map: ReturnMap) -> Rch<ReturnMap> {
        let mut hasher = DefaultHasher::default();
        return_map.hash(&mut hasher);
        let hash = hasher.finish();
        let hc_node = HashCached::with_hash(return_map, hash);
        let mut caches = self.caches.borrow_mut();
        if let Some(rch) = caches.return_map_table.get(&hc_node) {
            return rch.clone();
        }
        let rch = Rc::new(hc_node);
        caches.return_map_table.insert(rch.clone());
        rch
    }
    pub(super) fn add_reduce_matrix(&self, op_matrix: Vec<usize>) -> Rch<Vec<usize>> {
        let mut hasher = DefaultHasher::default();
        op_matrix.hash(&mut hasher);
        let hash = hasher.finish();
        let hc_node = HashCached::with_hash(op_matrix, hash);
        let mut caches = self.caches.borrow_mut();
        if let Some(rch) = caches.reduce_matrix_table.get(&hc_node) {
            return rch.clone();
        }
        let rch = Rc::new(hc_node);
        caches.reduce_matrix_table.insert(rch.clone());
        rch
    }
    pub(super) fn get_pair_product_cache(
        &self,
        n1: usize,
        n2: usize,
    ) -> Option<ConnectionPair<'grammar>> {
        let caches = self.caches.borrow();
        if let Some(t) = caches.pair_product_cache.get(&(n1, n2)).cloned() {
            return Some(t);
        }
        if let Some(t) = caches.pair_product_cache.get(&(n2, n1)) {
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
        let caches = self.caches.borrow();
        if let Some(t) = caches.bdd_pair_product_cache.get(&(hash1, hash2)).cloned() {
            return Some(t);
        }
        if let Some(t) = caches.bdd_pair_product_cache.get(&(hash2, hash1)) {
            return Some(t.flipped());
        }
        None
    }
    pub(super) fn get_pair_map_cache(
        &self,
        n1: usize,
        n2: usize,
        op_matrix: &Rch<Vec<usize>>,
    ) -> Option<Connection<'grammar>> {
        let hash3 = Rc::as_ptr(op_matrix) as usize;
        self.caches
            .borrow()
            .pair_map_cache
            .get(&(n1, n2, hash3))
            .cloned()
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
        self.caches
            .borrow()
            .bdd_pair_map_cache
            .get(&(hash1, hash2, hash3))
            .cloned()
    }
    pub(super) fn get_reduction_cache(&self, n: usize, indices: &[usize]) -> Option<usize> {
        let key = ReductionCacheKey::new(n, indices);
        self.caches.borrow().reduction_cache.get(&key).cloned()
    }
    pub(super) fn get_bdd_reduction_cache(
        &self,
        _n: &Rch<BddNode>,
        _indices: &[usize],
    ) -> Option<Rch<BddNode>> {
        // not supported yet since we don't have reduction for BDD, and the cache is only used in reduction.
        todo!("BDD reduction is not supported yet");
        // let key = ReductionCacheKey::new(n, indices);
        // self.bdd_reduction_cache.get(&key).cloned()
    }
    pub(super) fn set_pair_product_cache(
        &self,
        n1: usize,
        n2: usize,
        conn: ConnectionPair<'grammar>,
    ) {
        self.caches
            .borrow_mut()
            .pair_product_cache
            .insert((n1, n2), conn);
    }
    pub(super) fn set_bdd_pair_product_cache(
        &self,
        n1: &Rch<BddNode>,
        n2: &Rch<BddNode>,
        conn: BddConnectionPair,
    ) {
        let hash1 = Rc::as_ptr(n1) as usize;
        let hash2 = Rc::as_ptr(n2) as usize;
        self.caches
            .borrow_mut()
            .bdd_pair_product_cache
            .insert((hash1, hash2), conn);
    }
    pub(super) fn set_pair_map_cache(
        &self,
        n1: usize,
        n2: usize,
        op_matrix: &Rch<Vec<usize>>,
        conn: Connection<'grammar>,
    ) {
        let hash3 = Rc::as_ptr(op_matrix) as usize;
        self.caches
            .borrow_mut()
            .pair_map_cache
            .insert((n1, n2, hash3), conn);
    }
    pub(super) fn set_bdd_pair_map_cache(
        &self,
        n1: &Rch<BddNode>,
        n2: &Rch<BddNode>,
        op_matrix: &Rch<Vec<usize>>,
        conn: BddConnection,
    ) {
        let hash1 = Rc::as_ptr(n1) as usize;
        let hash2 = Rc::as_ptr(n2) as usize;
        let hash3 = Rc::as_ptr(op_matrix) as usize;
        self.caches
            .borrow_mut()
            .bdd_pair_map_cache
            .insert((hash1, hash2, hash3), conn);
    }
    pub(super) fn set_reduction_cache(&self, n: usize, indices: &[usize], node: usize) {
        let key = ReductionCacheKey::new(n, indices);
        self.caches.borrow_mut().reduction_cache.insert(key, node);
    }
    pub(super) fn set_bdd_reduction_cache(
        &self,
        _n: &Rch<BddNode>,
        _indices: &[usize],
        _node: Rch<BddNode>,
    ) {
        todo!("BDD reduction is not supported yet");
        // let key = ReductionCacheKey::new(n, indices);
        // self.bdd_reduction_cache.insert(key, node);
    }

    pub(super) fn get_op_cache<const O: usize>(
        &self,
        lhs: Gcflobdd<'grammar>,
        rhs: Gcflobdd<'grammar>,
    ) -> Option<Gcflobdd<'grammar>> {
        self.caches.borrow().op_cache[O].get(&(lhs, rhs)).cloned()
    }
    pub(super) fn get_int_op_cache<const O: usize>(
        &self,
        lhs: GcflobddInt<'grammar>,
        rhs: GcflobddInt<'grammar>,
    ) -> Option<GcflobddInt<'grammar>> {
        self.caches.borrow().int_op_cache[O]
            .get(&(lhs, rhs))
            .cloned()
    }
    pub(super) fn set_op_cache<const O: usize>(
        &self,
        lhs: Gcflobdd<'grammar>,
        rhs: Gcflobdd<'grammar>,
        node: Gcflobdd<'grammar>,
    ) {
        self.caches.borrow_mut().op_cache[O].insert((lhs, rhs), node);
    }

    pub(super) fn set_int_op_cache<const O: usize>(
        &self,
        lhs: GcflobddInt<'grammar>,
        rhs: GcflobddInt<'grammar>,
        node: GcflobddInt<'grammar>,
    ) {
        self.caches.borrow_mut().int_op_cache[O].insert((lhs, rhs), node);
    }
    pub fn node_count(&self) -> usize {
        self.gcflobdd_node_table.len()
    }
    pub fn size_estimate(&self) -> usize {
        let caches = self.caches.borrow();
        let mut total_size = 0;
        total_size += self.gcflobdd_node_table.len()
            * (size_of::<Rch<GcflobddNode<'grammar>>>() + size_of::<GcflobddNode<'grammar>>());
        total_size +=
            caches.bdd_node_table.len() * (size_of::<Rch<BddNode>>() + size_of::<BddNode>());
        total_size +=
            caches.return_map_table.len() * (size_of::<Rch<ReturnMap>>() + size_of::<ReturnMap>());
        total_size += caches.reduce_matrix_table.len()
            * (size_of::<Rch<Vec<usize>>>() + size_of::<Vec<usize>>());

        total_size += caches.pair_product_cache.len()
            * (size_of::<(u64, u64)>() + size_of::<ConnectionPair<'grammar>>());
        total_size += caches.bdd_pair_product_cache.len()
            * (size_of::<(u64, u64)>() + size_of::<BddConnectionPair>());

        total_size += caches.pair_map_cache.len()
            * (size_of::<(u64, u64, u64)>() + size_of::<Connection<'grammar>>());
        total_size += caches.bdd_pair_map_cache.len()
            * (size_of::<(u64, u64, u64)>() + size_of::<BddConnection>());

        total_size += caches.reduction_cache.len()
            * (size_of::<ReductionCacheKey>() + size_of::<Rch<GcflobddNode<'grammar>>>());
        total_size += caches.bdd_reduction_cache.len()
            * (size_of::<ReductionCacheKey>() + size_of::<Rch<BddNode>>());

        total_size += caches.op_cache.iter().fold(0, |acc, cache| {
            acc + cache.len() * (3 * size_of::<Gcflobdd<'grammar>>())
        });

        total_size
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
        let caches = self.caches.get_mut();
        caches.pair_product_cache.clear();
        caches.reduction_cache.clear();
        caches.bdd_reduction_cache.clear();
        caches.bdd_pair_product_cache.clear();
        caches.op_cache.iter_mut().for_each(|map| map.clear());
        caches.int_op_cache.iter_mut().for_each(|map| map.clear());
        caches.pair_map_cache.clear();
        caches.bdd_pair_map_cache.clear();
        // Self::gcflobdd_node_table_gc(&mut self.gcflobdd_node_table);
        Self::bdd_node_table_gc(&mut caches.bdd_node_table);
        // clear return map after node table gc
        caches.return_map_table = caches
            .return_map_table
            .drain()
            .filter(|v| Rc::strong_count(v) > 1)
            .collect();
        caches.reduce_matrix_table = caches
            .reduce_matrix_table
            .drain()
            .filter(|v| Rc::strong_count(v) > 1)
            .collect();
    }
}
