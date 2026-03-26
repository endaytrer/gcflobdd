use crate::gcflobdd::node::GcflobddNode;
use std::hash::{Hash, Hasher};
use std::{collections::HashMap, rc::Rc};

#[derive(Default)]
pub struct Context<'grammar> {
    gcflobdd_node_cache: HashMap<u64, Rc<GcflobddNode<'grammar>>>,
    // TODO: add operation cache
}
impl<'grammar> Context<'grammar> {
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
