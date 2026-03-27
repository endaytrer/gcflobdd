use crate::{
    gcflobdd::{
        bdd::Bdd,
        connection::{Connection, ConnectionT},
        context::Context,
        return_map::{ReturnMap, ReturnMapT},
    },
    grammar::{BddNodeType, GrammarNode, GrammarNodeType, InternalGrammarNodeType},
};
use std::{
    cell::RefCell,
    hash::{Hash, Hasher},
    rc::Rc,
};

pub(super) struct GcflobddNode<'grammar> {
    num_exits: usize,
    hash_cache: RefCell<Option<u64>>,
    pub(super) grammar: &'grammar Rc<GrammarNode>,
    pub(super) node: GcflobddNodeType<'grammar>,
}
impl std::fmt::Debug for GcflobddNode<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcflobddNode")
            .field("num_exits", &self.num_exits)
            .field("node", &self.node)
            .finish()
    }
}

impl Hash for GcflobddNode<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let hash_cache = self.hash_cache.borrow();
        if let Some(hash) = *hash_cache {
            hash.hash(state);
            return;
        }
        std::mem::drop(hash_cache);
        let mut hash_cache = self.hash_cache.borrow_mut();
        let mut hasher = std::hash::DefaultHasher::new();
        self.node.hash(&mut hasher);
        Rc::as_ptr(self.grammar).hash(&mut hasher);
        let value = hasher.finish();
        *hash_cache = Some(value);
        value.hash(state);
    }
}

impl PartialEq for GcflobddNode<'_> {
    fn eq(&self, other: &Self) -> bool {
        let mut hasher_1 = std::hash::DefaultHasher::new();
        let mut hasher_2 = std::hash::DefaultHasher::new();
        self.hash(&mut hasher_1);
        other.hash(&mut hasher_2);
        hasher_1.finish() == hasher_2.finish()
    }
}
impl Eq for GcflobddNode<'_> {}

#[derive(Debug, Hash)]
pub(super) enum GcflobddNodeType<'grammar> {
    DontCare,
    Fork,
    Internal(InternalNode<'grammar>),
    Bdd(BddNode<'grammar>),
}

pub(super) struct InternalNode<'grammar> {
    pub(super) connections: Vec<Vec<ConnectionT<'grammar, ReturnMapT<usize>>>>,
    pub(super) grammar: &'grammar InternalGrammarNodeType,
}

impl std::fmt::Debug for InternalNode<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InternalNode")
            .field("connections", &self.connections)
            .finish()
    }
}

impl Hash for InternalNode<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.connections.hash(state);
        self.grammar.hash(state);
    }
}
#[derive(Debug)]
pub(super) struct BddNode<'grammar> {
    bdd: Bdd,
    grammar: &'grammar BddNodeType,
}
impl Hash for BddNode<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.bdd.hash(state);
    }
}

impl<'grammar> GcflobddNode<'grammar> {
    pub fn dont_care(grammar: &'grammar Rc<GrammarNode>) -> Self {
        Self {
            num_exits: 1,
            hash_cache: RefCell::new(None),
            grammar,
            node: GcflobddNodeType::DontCare,
        }
    }
    pub fn fork(grammar: &'grammar Rc<GrammarNode>) -> Self {
        Self {
            num_exits: 2,
            hash_cache: RefCell::new(None),
            grammar,
            node: GcflobddNodeType::Fork,
        }
    }
    pub fn get_num_exits(&self) -> usize {
        self.num_exits
    }
    pub fn mk_distinction(
        i: usize,
        grammar: &'grammar Rc<GrammarNode>,
        context: &RefCell<Context<'grammar>>,
    ) -> Rc<Self> {
        let ans = Self {
            num_exits: 2,
            hash_cache: RefCell::new(None),
            grammar,
            node: match &grammar.node {
                GrammarNodeType::Internal(grammar_nodes) => {
                    let mut connections = vec![];
                    let mut position: Option<usize> = Some(i);
                    for gn in grammar_nodes {
                        if let Some(p) = &mut position {
                            if *p >= gn.num_vars {
                                *p -= gn.num_vars;
                                connections.push(vec![Connection::new(Self::mk_no_distinction(
                                    gn, context,
                                ))]);
                            } else {
                                connections.push(vec![Connection::new(Self::mk_distinction(
                                    *p, gn, context,
                                ))]);
                                position.take();
                            }
                        } else {
                            let false_branch =
                                Connection::new(Self::mk_no_distinction(gn, context));
                            let mut true_branch =
                                Connection::new(Self::mk_no_distinction(gn, context));
                            // true should always go to true
                            true_branch.return_map.set(0, 1);
                            connections.push(vec![false_branch, true_branch])
                        }
                    }
                    GcflobddNodeType::Internal(InternalNode {
                        connections,
                        grammar: grammar_nodes,
                    })
                }
                GrammarNodeType::Bdd(_) => todo!(),
                GrammarNodeType::Terminal => {
                    debug_assert_eq!(i, 0);
                    GcflobddNodeType::Fork
                }
            },
        };
        context.borrow_mut().add_gcflobdd_node(ans)
    }
    pub fn mk_no_distinction(
        grammar: &'grammar Rc<GrammarNode>,
        context: &RefCell<Context<'grammar>>,
    ) -> Rc<Self> {
        let ans = Self {
            num_exits: 1,
            hash_cache: RefCell::new(None),
            grammar,
            node: match &grammar.node {
                GrammarNodeType::Internal(grammar_nodes) => {
                    GcflobddNodeType::Internal(InternalNode {
                        connections: vec![],
                        grammar: grammar_nodes,
                    })
                }
                GrammarNodeType::Terminal => GcflobddNodeType::DontCare,
                GrammarNodeType::Bdd(_) => todo!(),
            },
        };
        context.borrow_mut().add_gcflobdd_node(ans)
    }
    pub fn find_one_path_to(&self, value: usize) -> Vec<Option<bool>> {
        if self.num_exits == 1 {
            debug_assert_eq!(value, 0);
            return vec![None; self.grammar.num_vars];
        }
        match &self.node {
            GcflobddNodeType::Internal(internal_node) => {
                internal_node.find_one_path_to(value, 0, 0).unwrap()
            }
            GcflobddNodeType::Bdd(_) => todo!(),
            GcflobddNodeType::DontCare => unreachable!(),
            GcflobddNodeType::Fork => {
                if value == 0 {
                    vec![Some(false)]
                } else {
                    vec![Some(true)]
                }
            }
        }
    }
    pub fn pair_product(
        &self,
        rhs: &Self,
        context: &RefCell<Context<'grammar>>,
    ) -> ConnectionT<'grammar, ReturnMapT<(usize, usize)>> {
        // should be the same grammar
        debug_assert_eq!(self.grammar, rhs.grammar);
        if let Some(t) = context.borrow_mut().get_pair_product_cache(self, rhs) {
            return t;
        }
        let ans = match (&self.node, &rhs.node) {
            (GcflobddNodeType::Internal(self_node), GcflobddNodeType::Internal(rhs_node)) => {
                if self.num_exits == 1 && rhs.num_exits == 1 {
                    return ConnectionT {
                        entry_point: Self::mk_no_distinction(self.grammar, context),
                        return_map: ReturnMapT::new(vec![(0, 0)]),
                    };
                }
                if self.num_exits == 1 {
                    return ConnectionT {
                        entry_point: context
                            .borrow_mut()
                            .get_gcflobdd_node(rhs)
                            .expect("Context error"),
                        return_map: ReturnMapT::new((0..rhs.num_exits).map(|i| (0, i)).collect()),
                    };
                }
                if rhs.num_exits == 1 {
                    return ConnectionT {
                        entry_point: context
                            .borrow_mut()
                            .get_gcflobdd_node(self)
                            .expect("Context error"),
                        return_map: ReturnMapT::new((0..self.num_exits).map(|i| (i, 0)).collect()),
                    };
                }
                let mut connection_pair_list = vec![(0usize, 0usize)];
                let mut product_connections = vec![];
                for i in 0..self_node.connections.len() {
                    let self_connection_list = &self_node.connections[i];
                    let rhs_connection_list = &rhs_node.connections[i];
                    let mut new_connection_pair_list = vec![];

                    fn merge_pair_list(
                        pair_list: &mut Vec<(usize, usize)>,
                        new_pair_list: impl IntoIterator<Item = (usize, usize)>,
                    ) -> Vec<usize> {
                        new_pair_list
                            .into_iter()
                            .map(|p| {
                                pair_list.iter().position(|x| *x == p).unwrap_or_else(|| {
                                    pair_list.push(p);
                                    pair_list.len() - 1
                                })
                            })
                            .collect()
                    }
                    let new_connections =
                        connection_pair_list
                            .into_iter()
                            .map(|(j, k)| {
                                let self_connection = &self_connection_list[j];
                                let rhs_connection = &rhs_connection_list[k];
                                let ConnectionT {
                                    entry_point,
                                    return_map: new_inner_pairs,
                                } = Self::pair_product(
                                    self_connection.entry_point.as_ref(),
                                    rhs_connection.entry_point.as_ref(),
                                    context,
                                );
                                let new_outer_pairs = new_inner_pairs.map_array.into_iter().map(
                                    |(inner_j, inner_k)| {
                                        (
                                            self_connection.return_map.lookup(inner_j),
                                            rhs_connection.return_map.lookup(inner_k),
                                        )
                                    },
                                );
                                let return_map = ReturnMap::new(merge_pair_list(
                                    &mut new_connection_pair_list,
                                    new_outer_pairs,
                                ));
                                Connection {
                                    entry_point,
                                    return_map,
                                }
                            })
                            .collect();
                    product_connections.push(new_connections);
                    connection_pair_list = new_connection_pair_list;
                }
                ConnectionT {
                    entry_point: context.borrow_mut().add_gcflobdd_node(Self {
                        num_exits: connection_pair_list.len(),
                        hash_cache: RefCell::new(None),
                        grammar: self.grammar,
                        node: GcflobddNodeType::Internal(InternalNode {
                            connections: product_connections,
                            grammar: self_node.grammar,
                        }),
                    }),
                    return_map: ReturnMapT::new(connection_pair_list),
                }
            }
            (GcflobddNodeType::Bdd(_), GcflobddNodeType::Bdd(_)) => todo!(),
            (GcflobddNodeType::DontCare, GcflobddNodeType::DontCare) => ConnectionT {
                entry_point: context
                    .borrow_mut()
                    .add_gcflobdd_node(Self::dont_care(self.grammar)),
                return_map: ReturnMapT::new(vec![(0, 0)]),
            },
            (GcflobddNodeType::Fork, GcflobddNodeType::DontCare) => ConnectionT {
                entry_point: context
                    .borrow_mut()
                    .add_gcflobdd_node(Self::fork(self.grammar)),
                return_map: ReturnMapT::new(vec![(0, 0), (1, 0)]),
            },
            (GcflobddNodeType::DontCare, GcflobddNodeType::Fork) => ConnectionT {
                entry_point: context
                    .borrow_mut()
                    .add_gcflobdd_node(Self::fork(self.grammar)),
                return_map: ReturnMapT::new(vec![(0, 0), (0, 1)]),
            },
            (GcflobddNodeType::Fork, GcflobddNodeType::Fork) => ConnectionT {
                entry_point: context
                    .borrow_mut()
                    .add_gcflobdd_node(Self::fork(self.grammar)),
                return_map: ReturnMapT::new(vec![(0, 0), (1, 1)]),
            },
            _ => unreachable!("Invalid configuration for grammar"),
        };
        context
            .borrow_mut()
            .set_pair_product_cache(self, rhs, ans.clone());
        ans
    }
    pub fn reduce(&self, reduce_map: Vec<usize>, context: &RefCell<Context<'grammar>>) -> Rc<Self> {
        if let Some(t) = context.borrow_mut().get_reduction_cache(self, &reduce_map) {
            return t;
        }
        let cache_reduce_map = reduce_map.clone();
        let ans = match &self.node {
            GcflobddNodeType::DontCare => {
                debug_assert!(reduce_map == [0]);
                context.borrow_mut().get_gcflobdd_node(self).unwrap()
            }
            GcflobddNodeType::Fork => {
                if reduce_map == [0, 0] {
                    return context
                        .borrow_mut()
                        .add_gcflobdd_node(Self::dont_care(self.grammar));
                } else {
                    debug_assert!(reduce_map == [0, 1]);
                    return context.borrow_mut().get_gcflobdd_node(self).unwrap();
                }
            }
            GcflobddNodeType::Internal(internal_node) => {
                if reduce_map.iter().all(|x| *x == 0) {
                    // all reduce to 0, return don't care
                    return context.borrow_mut().add_gcflobdd_node(Self {
                        num_exits: 1,
                        hash_cache: RefCell::new(None),
                        grammar: self.grammar,
                        node: GcflobddNodeType::Internal(InternalNode {
                            connections: vec![],
                            grammar: internal_node.grammar,
                        }),
                    });
                }
                // If self is a don't care, the reduce_map should be &[0].
                let num_exits = reduce_map.iter().max().unwrap() + 1;
                let mut reduce_map_max = num_exits;
                let mut layer_reduce_map = reduce_map;
                let mut new_connection_list = Vec::with_capacity(internal_node.connections.len());
                for connection_list in internal_node.connections.iter().rev() {
                    let mut new_connection_hashes = vec![];
                    let mut new_connections = vec![];
                    let new_reduce_map = connection_list
                        .iter()
                        .map(|connection| {
                            // first appearance of a value.
                            let mut inverse_lookup = vec![None; reduce_map_max];
                            let mut num_outs = 0;
                            let mut new_return_map = vec![];
                            let reduce_map_outer = connection
                                .return_map
                                .map_array
                                .iter()
                                .map(|x| {
                                    let ans = layer_reduce_map[*x];
                                    inverse_lookup[ans].get_or_insert_with(|| {
                                        num_outs += 1;
                                        new_return_map.push(ans);
                                        num_outs - 1
                                    });
                                    ans
                                })
                                .collect::<Vec<_>>();
                            let reduce_map_inner = reduce_map_outer
                                .iter()
                                .map(|x| inverse_lookup[*x].unwrap())
                                .collect::<Vec<_>>();
                            let new_entry =
                                connection.entry_point.reduce(reduce_map_inner, context);
                            // hash should exist, since the new entry is freshly created and added to the context;
                            let new_connection = ConnectionT {
                                entry_point: new_entry,
                                return_map: ReturnMapT::new(new_return_map),
                            };
                            let mut hasher = std::hash::DefaultHasher::new();
                            new_connection.hash(&mut hasher);
                            let hash = hasher.finish();
                            new_connection_hashes
                                .iter()
                                .position(|x| *x == hash)
                                .unwrap_or_else(|| {
                                    new_connection_hashes.push(hash);
                                    new_connections.push(new_connection);
                                    new_connection_hashes.len() - 1
                                })
                        })
                        .collect();
                    reduce_map_max = new_connection_hashes.len();
                    layer_reduce_map = new_reduce_map;
                    new_connection_list.push(new_connections);
                }
                new_connection_list.reverse();
                context.borrow_mut().add_gcflobdd_node(Self {
                    num_exits,
                    hash_cache: RefCell::new(None),
                    node: GcflobddNodeType::Internal(InternalNode {
                        connections: new_connection_list,
                        grammar: internal_node.grammar,
                    }),
                    grammar: self.grammar,
                })
            }
            GcflobddNodeType::Bdd(bdd_node) => todo!(),
        };
        context
            .borrow_mut()
            .set_reduction_cache(self, cache_reduce_map, ans.clone());
        ans
    }
}

impl<'grammar> InternalNode<'grammar> {
    fn find_one_path_to(
        &self,
        value: usize,
        layer_idx: usize,
        connection_idx: usize,
    ) -> Option<Vec<Option<bool>>> {
        let connection_list = &self.connections[layer_idx];
        let connection = &connection_list[connection_idx];
        if layer_idx == self.connections.len() - 1 {
            return connection
                .return_map
                .inverse_lookup(&value)
                .map(|inner_value| connection.entry_point.find_one_path_to(inner_value));
        }
        for inner_target in 0..connection.entry_point.get_num_exits() {
            let next_connection_index = connection.return_map.lookup(inner_target);
            if let Some(path) = self.find_one_path_to(value, layer_idx + 1, next_connection_index) {
                let path_to_next_connection = connection.entry_point.find_one_path_to(inner_target);
                return Some([path_to_next_connection, path].concat());
            }
        }
        None
    }
}
