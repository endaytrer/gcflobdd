use crate::{
    gcflobdd::{
        bdd::Bdd,
        connection::{Connection, ConnectionPair, ConnectionT},
        context::Context,
        return_map::inverse_lookup,
    },
    grammar::{BddNodeType, GrammarNode, GrammarNodeType, InternalGrammarNodeType},
    utils::hash_cache::Rch,
};
use std::{
    cell::RefCell,
    collections::HashMap,
    hash::{Hash, Hasher},
    rc::Rc,
};

pub(super) struct GcflobddNode<'grammar> {
    num_exits: usize,
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
        self.node.hash(state);
        Rc::as_ptr(self.grammar).hash(state);
    }
}

#[derive(Debug, Hash)]
pub(super) enum GcflobddNodeType<'grammar> {
    DontCare,
    Fork,
    Internal(InternalNode<'grammar>),
    Bdd(BddNode<'grammar>),
}

pub(super) struct InternalNode<'grammar> {
    pub(super) connections: Vec<Vec<Connection<'grammar>>>,
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
            grammar,
            node: GcflobddNodeType::DontCare,
        }
    }
    pub fn fork(grammar: &'grammar Rc<GrammarNode>) -> Self {
        Self {
            num_exits: 2,
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
    ) -> Rch<Self> {
        let ans = Self {
            num_exits: 2,
            grammar,
            node: match &grammar.node {
                GrammarNodeType::Internal(grammar_nodes) => {
                    let mut connections = vec![];
                    let mut position: Option<usize> = Some(i);
                    for gn in grammar_nodes {
                        if let Some(p) = &mut position {
                            if *p >= gn.num_vars {
                                *p -= gn.num_vars;
                                connections.push(vec![Connection::new_sequential(
                                    Self::mk_no_distinction(gn, context),
                                    context,
                                )]);
                            } else {
                                connections.push(vec![Connection::new_sequential(
                                    Self::mk_distinction(*p, gn, context),
                                    context,
                                )]);
                                position.take();
                            }
                        } else {
                            let false_branch = Connection::new(
                                Self::mk_no_distinction(gn, context),
                                vec![0],
                                context,
                            );
                            let true_branch = Connection::new(
                                Self::mk_no_distinction(gn, context),
                                vec![1],
                                context,
                            );
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
    ) -> Rch<Self> {
        let ans = Self {
            num_exits: 1,
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
        lhs: &Rch<Self>,
        rhs: &Rch<Self>,
        context: &RefCell<Context<'grammar>>,
    ) -> ConnectionPair<'grammar> {
        // should be the same grammar
        debug_assert_eq!(lhs.grammar, rhs.grammar);
        if let Some(t) = context.borrow_mut().get_pair_product_cache(lhs, rhs) {
            return t;
        }
        let ans = match (&lhs.node, &rhs.node) {
            (GcflobddNodeType::Internal(lhs_node), GcflobddNodeType::Internal(rhs_node)) => {
                if lhs.num_exits == 1 {
                    if rhs.num_exits == 1 {
                        ConnectionT {
                            entry_point: Self::mk_no_distinction(lhs.grammar, context),
                            return_map: vec![(0, 0)],
                        }
                    } else {
                        ConnectionT {
                            entry_point: rhs.clone(),
                            return_map: (0..rhs.num_exits).map(|i| (0, i)).collect(),
                        }
                    }
                } else if rhs.num_exits == 1 {
                    ConnectionT {
                        entry_point: lhs.clone(),
                        return_map: (0..lhs.num_exits).map(|i| (i, 0)).collect(),
                    }
                } else {
                    let mut connection_pair_list = vec![(0usize, 0usize)];
                    let mut product_connections = vec![];

                    // Process layer 0 specifically to skip O(N) allocation and lookup overheads
                    if !lhs_node.connections.is_empty() {
                        let lhs_connection = &lhs_node.connections[0][0];
                        let rhs_connection = &rhs_node.connections[0][0];
                        let ConnectionT {
                            entry_point,
                            return_map: new_inner_pairs,
                        } = Self::pair_product(
                            &lhs_connection.entry_point,
                            &rhs_connection.entry_point,
                            context,
                        );

                        let mut new_connection_pair_list = vec![];
                        let size_first = if 0 == lhs_node.connections.len() - 1 {
                            lhs.num_exits
                        } else {
                            lhs_node.connections[1].len()
                        };
                        let size_second = if 0 == rhs_node.connections.len() - 1 {
                            rhs.num_exits
                        } else {
                            rhs_node.connections[1].len()
                        };

                        let mut temp_vector = vec![usize::MAX; size_first * size_second];
                        let mut new_outer_pairs = vec![];

                        for (inner_j, inner_k) in new_inner_pairs {
                            let outer_j = lhs_connection.return_map[inner_j];
                            let outer_k = rhs_connection.return_map[inner_k];
                            let index = outer_j * size_second + outer_k;

                            if temp_vector[index] == usize::MAX {
                                new_connection_pair_list.push((outer_j, outer_k));
                                new_outer_pairs.push(new_connection_pair_list.len() - 1);
                                temp_vector[index] = new_connection_pair_list.len() - 1;
                            } else {
                                new_outer_pairs.push(temp_vector[index]);
                            }
                        }

                        let return_map = context.borrow_mut().add_return_map(new_outer_pairs);
                        product_connections.push(vec![Connection {
                            entry_point,
                            return_map,
                        }]);
                        connection_pair_list = new_connection_pair_list;
                    }

                    for i in 1..lhs_node.connections.len() {
                        let lhs_connection_list = &lhs_node.connections[i];
                        let rhs_connection_list = &rhs_node.connections[i];
                        let mut new_connection_pair_list = vec![];

                        let size_first = if i == lhs_node.connections.len() - 1 {
                            lhs.num_exits
                        } else {
                            lhs_node.connections[i + 1].len()
                        };
                        let size_second = if i == rhs_node.connections.len() - 1 {
                            rhs.num_exits
                        } else {
                            rhs_node.connections[i + 1].len()
                        };
                        let mut temp_vector = vec![usize::MAX; size_first * size_second];

                        let new_connections = connection_pair_list
                            .into_iter()
                            .map(|(j, k)| {
                                let lhs_connection = &lhs_connection_list[j];
                                let rhs_connection = &rhs_connection_list[k];
                                let ConnectionT {
                                    entry_point,
                                    return_map: new_inner_pairs,
                                } = Self::pair_product(
                                    &lhs_connection.entry_point,
                                    &rhs_connection.entry_point,
                                    context,
                                );

                                let mut new_outer_pairs = vec![];
                                for (inner_j, inner_k) in new_inner_pairs {
                                    let outer_j = lhs_connection.return_map[inner_j];
                                    let outer_k = rhs_connection.return_map[inner_k];
                                    let index = outer_j * size_second + outer_k;

                                    if temp_vector[index] == usize::MAX {
                                        new_connection_pair_list.push((outer_j, outer_k));
                                        new_outer_pairs.push(new_connection_pair_list.len() - 1);
                                        temp_vector[index] = new_connection_pair_list.len() - 1;
                                    } else {
                                        new_outer_pairs.push(temp_vector[index]);
                                    }
                                }

                                let return_map =
                                    context.borrow_mut().add_return_map(new_outer_pairs);
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
                            grammar: lhs.grammar,
                            node: GcflobddNodeType::Internal(InternalNode {
                                connections: product_connections,
                                grammar: lhs_node.grammar,
                            }),
                        }),
                        return_map: connection_pair_list,
                    }
                }
            }
            (GcflobddNodeType::Bdd(_), GcflobddNodeType::Bdd(_)) => todo!(),
            (GcflobddNodeType::DontCare, GcflobddNodeType::DontCare) => ConnectionT {
                entry_point: lhs.clone(),
                return_map: vec![(0, 0)],
            },
            (GcflobddNodeType::Fork, GcflobddNodeType::DontCare) => ConnectionT {
                entry_point: lhs.clone(),
                return_map: vec![(0, 0), (1, 0)],
            },
            (GcflobddNodeType::DontCare, GcflobddNodeType::Fork) => ConnectionT {
                entry_point: rhs.clone(),
                return_map: vec![(0, 0), (0, 1)],
            },
            (GcflobddNodeType::Fork, GcflobddNodeType::Fork) => ConnectionT {
                entry_point: lhs.clone(),
                return_map: vec![(0, 0), (1, 1)],
            },
            _ => unreachable!("Invalid configuration for grammar"),
        };
        context
            .borrow_mut()
            .set_pair_product_cache(lhs, rhs, ans.clone());
        ans
    }
    pub fn reduce(
        this: &Rch<Self>,
        reduce_map: Vec<usize>,
        num_exits: usize,
        context: &RefCell<Context<'grammar>>,
    ) -> Rch<Self> {
        if num_exits == 1 {
            return Self::mk_no_distinction(this.grammar, context);
        }
        // is identity map. This is guaranteed by the generation process.
        if num_exits == reduce_map.len() {
            return this.clone();
        }

        if let Some(t) = context.borrow_mut().get_reduction_cache(this, &reduce_map) {
            return t;
        }
        let cache_reduce_map = reduce_map.clone();
        let ans = match &this.node {
            GcflobddNodeType::DontCare => {
                debug_assert!(reduce_map == [0]);
                context.borrow_mut().get_gcflobdd_node(this).unwrap()
            }
            GcflobddNodeType::Fork => {
                if reduce_map == [0, 0] {
                    context
                        .borrow_mut()
                        .add_gcflobdd_node(Self::dont_care(this.grammar))
                } else {
                    debug_assert!(reduce_map == [0, 1]);
                    context.borrow_mut().get_gcflobdd_node(this).unwrap()
                }
            }
            GcflobddNodeType::Internal(internal_node) => {
                // If this is a don't care, the reduce_map should be &[0].
                // The early return `if num_exits == 1` handles the `reduce_map.iter().all(|x| *x == 0)` case.
                let mut reduce_map_max = num_exits;
                let mut layer_reduce_map = reduce_map.clone();
                let mut new_connection_list =
                    Vec::with_capacity(internal_node.connections.len());
                for connection_list in internal_node.connections.iter().rev() {
                    let mut new_connection_hashes = HashMap::new();
                    let mut new_connections = vec![];

                    let is_identity_layer = layer_reduce_map.iter().enumerate().all(|(i, &x)| i == x);

                    let new_reduce_map = connection_list
                        .iter()
                        .map(|connection| {
                            if is_identity_layer {
                                let new_entry = GcflobddNode::reduce(
                                    &connection.entry_point,
                                    connection.return_map.iter().enumerate().map(|(i, _)| i).collect(),
                                    connection.return_map.len(),
                                    context,
                                );
                                
                                let new_connection = ConnectionT {
                                    entry_point: new_entry,
                                    return_map: context.borrow_mut().add_return_map(connection.return_map.iter().copied().collect()),
                                };
                                let mut hasher = std::hash::DefaultHasher::new();
                                new_connection.hash(&mut hasher);
                                let hash = hasher.finish();
                                
                                *new_connection_hashes.entry(hash).or_insert_with(|| {
                                    new_connections.push(new_connection);
                                    new_connections.len() - 1
                                })
                            } else {
                                // first appearance of a value.
                                let mut inverse_lookup = vec![None; reduce_map_max];
                                let mut num_outs = 0;
                                let mut new_return_map = vec![];
                                let reduce_map_outer = connection
                                    .return_map
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
                                let new_entry = GcflobddNode::reduce(
                                    &connection.entry_point,
                                    reduce_map_inner,
                                    new_return_map.len(),
                                    context,
                                );
                                // hash should exist, since the new entry is freshly created and added to the context;
                                let new_connection = ConnectionT {
                                    entry_point: new_entry,
                                    return_map: context.borrow_mut().add_return_map(new_return_map),
                                };
                                let mut hasher = std::hash::DefaultHasher::new();
                                new_connection.hash(&mut hasher);
                                let hash = hasher.finish();
                                
                                *new_connection_hashes.entry(hash).or_insert_with(|| {
                                    new_connections.push(new_connection);
                                    new_connections.len() - 1
                                })
                            }
                        })
                        .collect();
                    reduce_map_max = new_connection_hashes.len();
                    layer_reduce_map = new_reduce_map;
                    new_connection_list.push(new_connections);
                }
                new_connection_list.reverse();
                context.borrow_mut().add_gcflobdd_node(Self {
                    num_exits,
                    node: GcflobddNodeType::Internal(InternalNode {
                        connections: new_connection_list,
                        grammar: internal_node.grammar,
                    }),
                    grammar: this.grammar,
                })
            }
            GcflobddNodeType::Bdd(_bdd_node) => todo!(),
        };
        context
            .borrow_mut()
            .set_reduction_cache(this, &cache_reduce_map, ans.clone());
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
            return inverse_lookup(&connection.return_map, &value)
                .map(|inner_value| connection.entry_point.find_one_path_to(inner_value));
        }
        for inner_target in 0..connection.entry_point.get_num_exits() {
            let next_connection_index = connection.return_map[inner_target];
            if let Some(path) = self.find_one_path_to(value, layer_idx + 1, next_connection_index) {
                let path_to_next_connection = connection.entry_point.find_one_path_to(inner_target);
                return Some([path_to_next_connection, path].concat());
            }
        }
        None
    }
}
