use crate::{
    gcflobdd::{
        bdd::Bdd,
        connection::{Connection, ConnectionLayer, ConnectionPair, ConnectionT},
        context::Context,
        return_map::{ExitVec, ReturnMap, ReturnMapT, inverse_lookup},
    },
    grammar::{GrammarNode, GrammarNodeType},
    utils::hash_cache::Rch,
};
#[cfg(feature = "fx-hash")]
use rustc_hash::{FxHashMap as HashMap, FxHasher as DefaultHasher};
use std::{
    cell::RefCell,
    hash::{Hash, Hasher},
    mem::MaybeUninit,
    rc::Rc,
};
#[cfg(not(feature = "fx-hash"))]
use std::{collections::HashMap, hash::DefaultHasher};
use smallvec::smallvec;

pub struct GcflobddNode<'grammar> {
    pub(super) num_exits: usize,
    pub(super) grammar: &'grammar Rc<GrammarNode>,
    pub(super) node: GcflobddNodeType<'grammar>,
}
impl std::fmt::Debug for GcflobddNode<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcflobddNode")
            .field("num_vars", &self.grammar.num_vars)
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

/// A reduce map _R_ should be:
/// 0. Non-empty
/// 1. R[0] = 0,
/// 2. For all i > 0, R[i] \in {R[0], R[1], ..., R[i - 1], max{R[0], R[1], ..., R[i - 1]}}
pub(super) struct ReduceMap(ExitVec);

impl From<ExitVec> for ReduceMap {
    #[inline]
    fn from(map: ExitVec) -> Self {
        // debug_assert_eq!(map[0], 0);
        // debug_assert!(map.iter().enumerate().skip(1).all(|(i, x)| {
        //     map[0..i].contains(x) || *x == *map[0..i].iter().max().unwrap()
        // }));
        Self(map)
    }
}

impl AsRef<ExitVec> for ReduceMap {
    fn as_ref(&self) -> &ExitVec {
        &self.0
    }
}
impl std::borrow::Borrow<ExitVec> for ReduceMap {
    fn borrow(&self) -> &ExitVec {
        &self.0
    }
}
impl std::ops::Deref for ReduceMap {
    type Target = ExitVec;
    fn deref(&self) -> &ExitVec {
        &self.0
    }
}
impl std::ops::Index<usize> for ReduceMap {
    type Output = usize;
    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
pub(super) enum GcflobddNodeType<'grammar> {
    DontCare,
    Fork,
    Internal(InternalNode<'grammar>),
    Bdd(Bdd),
}

#[derive(PartialEq, Eq)]
pub(super) struct InternalNode<'grammar> {
    pub(super) connections: Vec<ConnectionLayer<'grammar>>,
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

/// Add two values that live in the base-2 logarithmic domain, i.e. given
/// `a = log2(x)` and `b = log2(y)` return `log2(x + y)` without ever
/// materialising `x` or `y` (which may overflow `f64`). `f64::NEG_INFINITY`
/// represents `log2(0)`.
#[inline]
pub(crate) fn log2_add(a: f64, b: f64) -> f64 {
    if a == f64::NEG_INFINITY {
        return b;
    }
    if b == f64::NEG_INFINITY {
        return a;
    }
    let (hi, lo) = if a >= b { (a, b) } else { (b, a) };
    hi + (1.0 + (lo - hi).exp2()).log2()
}

impl<'grammar> GcflobddNode<'grammar> {
    pub fn get_num_exits(&self) -> usize {
        self.num_exits
    }

    /// For each of this node's exits, return `log2` of the number of
    /// assignments (over the node's `grammar.num_vars` variables) that are
    /// routed to that exit. The returned vector has length `num_exits` and its
    /// `log2`-domain entries sum (via [`log2_add`]) to `grammar.num_vars`.
    ///
    /// The GCFLOBDD node tables form a shared DAG, so results are memoized by
    /// node pointer to avoid an exponential blow-up.
    pub(super) fn log2_exit_counts(
        node: &Rch<Self>,
        memo: &mut HashMap<usize, Vec<f64>>,
    ) -> Vec<f64> {
        let key = Rc::as_ptr(node) as usize;
        if let Some(v) = memo.get(&key) {
            return v.clone();
        }
        let res = match &node.node {
            // A don't-care node has a single exit that every assignment reaches.
            GcflobddNodeType::DontCare => vec![node.grammar.num_vars as f64],
            // A fork spans exactly one variable: exit 0 for false, exit 1 for true.
            GcflobddNodeType::Fork => vec![0.0, 0.0],
            GcflobddNodeType::Bdd(bdd) => {
                bdd.log2_exit_counts(node.grammar.num_vars, node.num_exits)
            }
            GcflobddNodeType::Internal(internal_node) => {
                // Propagate a distribution over connection indices across the
                // matched layers. `dist[c]` is log2 of the number of
                // assignments (to the variables consumed so far) that select
                // connection `c` in the current layer.
                let mut dist = vec![f64::NEG_INFINITY; internal_node.connections[0].len()];
                dist[0] = 0.0;
                let num_layers = internal_node.connections.len();
                for (i, connection_list) in internal_node.connections.iter().enumerate() {
                    let next_size = if i == num_layers - 1 {
                        node.num_exits
                    } else {
                        internal_node.connections[i + 1].len()
                    };
                    let mut next = vec![f64::NEG_INFINITY; next_size];
                    for (c, &weight) in dist.iter().enumerate() {
                        if weight == f64::NEG_INFINITY {
                            continue;
                        }
                        let connection = &connection_list[c];
                        let sub = Self::log2_exit_counts(&connection.entry_point, memo);
                        for (inner, &sub_count) in sub.iter().enumerate() {
                            let target = connection.return_map[inner];
                            next[target] = log2_add(next[target], weight + sub_count);
                        }
                    }
                    dist = next;
                }
                dist
            }
        };
        memo.insert(key, res.clone());
        res
    }

    /// Evaluate the (opaque) exit index this node routes `assignment` to.
    /// `assignment` must have length `grammar.num_vars`. Used as a testing
    /// oracle and by callers that want a concrete point value.
    pub(super) fn evaluate(&self, assignment: &[bool]) -> usize {
        match &self.node {
            GcflobddNodeType::DontCare => 0,
            GcflobddNodeType::Fork => assignment[0] as usize,
            GcflobddNodeType::Bdd(bdd) => bdd.evaluate(assignment),
            GcflobddNodeType::Internal(internal_node) => {
                let GrammarNodeType::Internal(grammar_children) = &self.grammar.node else {
                    unreachable!("Internal node must have an Internal grammar")
                };
                let mut connection_idx = 0;
                let mut offset = 0;
                for (i, connection_list) in internal_node.connections.iter().enumerate() {
                    let child = &grammar_children[i];
                    let sub = &assignment[offset..offset + child.num_vars];
                    offset += child.num_vars;
                    let connection = &connection_list[connection_idx];
                    let inner = connection.entry_point.evaluate(sub);
                    connection_idx = connection.return_map[inner];
                }
                connection_idx
            }
        }
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
                    let mut connections: Vec<ConnectionLayer> = vec![];
                    let mut position: Option<usize> = Some(i);
                    for gn in grammar_nodes {
                        if let Some(p) = &mut position {
                            if *p >= gn.num_vars {
                                *p -= gn.num_vars;
                                connections.push(smallvec![Connection::new_sequential(
                                    Self::mk_no_distinction(gn, context),
                                    context,
                                )]);
                            } else {
                                connections.push(smallvec![Connection::new_sequential(
                                    Self::mk_distinction(*p, gn, context),
                                    context,
                                )]);
                                position.take();
                            }
                        } else {
                            let false_branch = Connection::new(
                                Self::mk_no_distinction(gn, context),
                                smallvec![0],
                                context,
                            );
                            let true_branch = Connection::new(
                                Self::mk_no_distinction(gn, context),
                                smallvec![1],
                                context,
                            );
                            connections.push(smallvec![false_branch, true_branch])
                        }
                    }
                    GcflobddNodeType::Internal(InternalNode { connections })
                }
                GrammarNodeType::Bdd(_) => GcflobddNodeType::Bdd(Bdd::mk_projection(i, context)),
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
            node: GcflobddNodeType::DontCare,
        };
        context.borrow_mut().add_gcflobdd_node(ans)
    }
    /// Build the canonical node for an explicitly tabulated function.
    ///
    /// `table` is indexed by assignment with variable 0 as the *most*
    /// significant bit -- i.e. `table[i]` holds the value of the assignment
    /// that gives variable `v` the bit `(i >> (num_vars - 1 - v)) & 1` -- and
    /// must have length `2^grammar.num_vars`. That ordering is the traversal
    /// order of the node, so distinct values are met in exactly the canonical
    /// exit order and the returned node needs no further reduction.
    ///
    /// Returns the interned node together with its exit values.
    ///
    /// Cost is `O(2^num_vars)`: this constructs small, densely given functions
    /// (matrices in tests, hand-written operators); it is not a scalable way to
    /// build a diagram.
    pub(super) fn from_table<T: Clone + PartialEq>(
        grammar: &'grammar Rc<GrammarNode>,
        table: &[T],
        context: &RefCell<Context<'grammar>>,
    ) -> (Rch<Self>, ReturnMapT<T>) {
        debug_assert_eq!(table.len(), 1usize << grammar.num_vars);
        match &grammar.node {
            GrammarNodeType::Terminal => {
                if table[0] == table[1] {
                    (
                        Self::mk_no_distinction(grammar, context),
                        smallvec![table[0].clone()],
                    )
                } else {
                    (
                        Self::mk_distinction(0, grammar, context),
                        smallvec![table[0].clone(), table[1].clone()],
                    )
                }
            }
            GrammarNodeType::Bdd(_) => {
                unimplemented!("from_table does not support BDD groupings")
            }
            GrammarNodeType::Internal(grammar_children) => {
                // `classes[c]` is the residual table reached by every assignment
                // that selects connection `c` of the layer being built. Because
                // the table is indexed big-endian, a residual table is always a
                // contiguous slice of `table`.
                let mut classes: Vec<&[T]> = vec![table];
                let mut connection_layers = Vec::with_capacity(grammar_children.len());
                let mut remaining = grammar.num_vars;

                for child in grammar_children {
                    remaining -= child.num_vars;
                    let sub_len = 1usize << remaining;
                    let mut next_classes: Vec<&[T]> = Vec::new();
                    let mut connections = ConnectionLayer::with_capacity(classes.len());

                    for class in classes {
                        // The child node maps its own assignments to indices
                        // into `next_classes`, so its exit values *are* the
                        // connection's return map.
                        let child_table = (0..(1usize << child.num_vars))
                            .map(|hi| {
                                let sub = &class[hi * sub_len..(hi + 1) * sub_len];
                                match next_classes.iter().position(|c| *c == sub) {
                                    Some(i) => i,
                                    None => {
                                        next_classes.push(sub);
                                        next_classes.len() - 1
                                    }
                                }
                            })
                            .collect::<Vec<_>>();
                        let (entry_point, return_map) =
                            Self::from_table(child, &child_table, context);
                        connections.push(Connection::new(entry_point, return_map, context));
                    }
                    connection_layers.push(connections);
                    classes = next_classes;
                }

                let values: ReturnMapT<T> = classes.iter().map(|c| c[0].clone()).collect();
                if values.len() == 1 {
                    return (Self::mk_no_distinction(grammar, context), values);
                }
                let node = context.borrow_mut().add_gcflobdd_node(Self {
                    num_exits: values.len(),
                    grammar,
                    node: GcflobddNodeType::Internal(InternalNode {
                        connections: connection_layers,
                    }),
                });
                (node, values)
            }
        }
    }

    /// Count the distinct nodes reachable from `node`, and the connections
    /// leaving them.
    ///
    /// This measures one diagram, not the context's interning tables, so it is
    /// the figure to compare against another implementation's node/edge counts.
    /// Nodes reachable from `node`, and the edges leaving them, counted the way
    /// the reference C++ `CFLOBDDInternalNode::CountNodesAndEdges` counts them:
    /// **two** edges per connection, plus the entries of every *distinct*
    /// return map. Sizes reported by this crate are directly comparable with
    /// the reference's because of it.
    pub(super) fn count_nodes_and_edges(
        node: &Rch<Self>,
        seen: &mut HashMap<usize, ()>,
        seen_maps: &mut HashMap<usize, ()>,
        nodes: &mut usize,
        edges: &mut usize,
    ) {
        if seen.insert(Rc::as_ptr(node) as usize, ()).is_some() {
            return;
        }
        *nodes += 1;
        if let GcflobddNodeType::Internal(internal) = &node.node {
            let connections: usize = internal.connections.iter().map(|l| l.len()).sum();
            *edges += 2 * connections;
            for layer in &internal.connections {
                for connection in layer {
                    // The reference interns return maps and counts each distinct
                    // one once; ours are interned too, so dedup by handle.
                    let key = Rc::as_ptr(&connection.return_map) as usize;
                    if seen_maps.insert(key, ()).is_none() {
                        *edges += connection.return_map.len();
                    }
                    Self::count_nodes_and_edges(
                        &connection.entry_point,
                        seen,
                        seen_maps,
                        nodes,
                        edges,
                    );
                }
            }
        }
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
            GcflobddNodeType::Bdd(bdd) => {
                bdd.find_one_path_to(value, self.grammar.num_vars).unwrap()
            }
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
        if let Some(t) = context.borrow().get_pair_product_cache(lhs, rhs) {
            return t;
        }
        let ans = match (&lhs.node, &rhs.node) {
            (GcflobddNodeType::DontCare, GcflobddNodeType::DontCare) => ConnectionPair {
                entry_point: Self::mk_no_distinction(lhs.grammar, context),
                return_map: smallvec![(0, 0)],
            },
            (GcflobddNodeType::DontCare, _) => ConnectionPair {
                entry_point: rhs.clone(),
                return_map: (0..rhs.num_exits).map(|i| (0, i)).collect(),
            },
            (_, GcflobddNodeType::DontCare) => ConnectionPair {
                entry_point: lhs.clone(),
                return_map: (0..lhs.num_exits).map(|i| (i, 0)).collect(),
            },
            (GcflobddNodeType::Internal(lhs_node), GcflobddNodeType::Internal(rhs_node)) => {
                let mut connection_pair_list = smallvec![(0usize, 0usize)];
                let mut product_connections = Vec::with_capacity(lhs_node.connections.len());
                let mut exit_lookup = Vec::new();
                for i in 0..lhs_node.connections.len() {
                    let lhs_connection_list = &lhs_node.connections[i];
                    let rhs_connection_list = &rhs_node.connections[i];

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

                    let mut new_connection_pair_list =
                        ReturnMapT::with_capacity(size_first * size_second);
                    exit_lookup.clear();
                    exit_lookup.resize(size_first * size_second, usize::MAX);

                    let new_connections: ConnectionLayer = connection_pair_list
                        .into_iter()
                        .map(|(j, k)| {
                            let lhs_connection: &Connection<'grammar> =
                                &lhs_connection_list[j];
                            let rhs_connection: &Connection<'grammar> =
                                &rhs_connection_list[k];
                            let ConnectionPair {
                                entry_point,
                                return_map: new_inner_pairs,
                            } = Self::pair_product(
                                &lhs_connection.entry_point,
                                &rhs_connection.entry_point,
                                context,
                            );
                            let mut new_outer_pairs =
                                ReturnMap::with_capacity(new_inner_pairs.len());
                            for (inner_j, inner_k) in new_inner_pairs {
                                let outer_j = lhs_connection.return_map[inner_j];
                                let outer_k = rhs_connection.return_map[inner_k];
                                let index = outer_j * size_second + outer_k;

                                if exit_lookup[index] == usize::MAX {
                                    new_connection_pair_list.push((outer_j, outer_k));
                                    new_outer_pairs.push(new_connection_pair_list.len() - 1);
                                    exit_lookup[index] = new_connection_pair_list.len() - 1;
                                } else {
                                    new_outer_pairs.push(exit_lookup[index]);
                                }
                            }
                            Connection {
                                entry_point,
                                return_map: context.borrow_mut().add_return_map(new_outer_pairs),
                            }
                        })
                        .collect();
                    product_connections.push(new_connections);
                    connection_pair_list = new_connection_pair_list;
                }
                ConnectionPair {
                    entry_point: context.borrow_mut().add_gcflobdd_node(Self {
                        num_exits: connection_pair_list.len(),
                        grammar: lhs.grammar,
                        node: GcflobddNodeType::Internal(InternalNode {
                            connections: product_connections,
                        }),
                    }),
                    return_map: connection_pair_list,
                }
            }
            (GcflobddNodeType::Bdd(lhs_bdd), GcflobddNodeType::Bdd(rhs_bdd)) => {
                let product = lhs_bdd.pair_product(rhs_bdd, lhs.num_exits, rhs.num_exits, context);
                ConnectionPair {
                    entry_point: context.borrow_mut().add_gcflobdd_node(Self {
                        num_exits: product.return_map.len(),
                        grammar: lhs.grammar,
                        node: GcflobddNodeType::Bdd(Bdd(product.entry_point)),
                    }),
                    return_map: product.return_map,
                }
            }
            (GcflobddNodeType::Fork, GcflobddNodeType::Fork) => ConnectionPair {
                entry_point: lhs.clone(),
                return_map: smallvec![(0, 0), (1, 1)],
            },
            _ => unreachable!("Invalid configuration for grammar"),
        };
        context
            .borrow_mut()
            .set_pair_product_cache(lhs, rhs, ans.clone());
        ans
    }

    pub fn pair_map(
        lhs: &Rch<Self>,
        rhs: &Rch<Self>,
        reduce_matrix: &Rch<ExitVec>, // should be a ReduceMap if either lhs / rhs is a dont care
        num_exits: usize,                // it's only used in reduce of don't care and normal nodes
        context: &RefCell<Context<'grammar>>,
    ) -> Connection<'grammar> {
        if num_exits == 1 {
            return Connection {
                entry_point: Self::mk_no_distinction(lhs.grammar, context),
                return_map: context.borrow_mut().add_return_map(smallvec![reduce_matrix[0]]),
            };
        }
        if let Some(t) = context.borrow().get_pair_map_cache(lhs, rhs, reduce_matrix) {
            return t;
        }

        let ans = match (&lhs.node, &rhs.node) {
            (GcflobddNodeType::DontCare, GcflobddNodeType::DontCare) => {
                debug_assert_eq!(reduce_matrix.len(), 1);
                Connection {
                    entry_point: Self::mk_no_distinction(lhs.grammar, context),
                    return_map: context.borrow_mut().add_return_map(smallvec![reduce_matrix[0]]),
                }
            }
            (GcflobddNodeType::DontCare, _) => {
                debug_assert_eq!(reduce_matrix.len(), rhs.num_exits);
                Connection::new_sequential(
                    Self::reduce(
                        rhs,
                        reduce_matrix.as_ref().as_ref().clone().into(),
                        num_exits,
                        context,
                    ),
                    context,
                )
            }
            (_, GcflobddNodeType::DontCare) => {
                debug_assert_eq!(reduce_matrix.len(), lhs.num_exits);
                Connection::new_sequential(
                    Self::reduce(
                        lhs,
                        reduce_matrix.as_ref().as_ref().clone().into(),
                        num_exits,
                        context,
                    ),
                    context,
                )
            }
            (GcflobddNodeType::Fork, GcflobddNodeType::Fork) => {
                debug_assert_eq!(reduce_matrix.len(), 4);
                if reduce_matrix[0] == reduce_matrix[3] {
                    Connection {
                        entry_point: Self::mk_no_distinction(lhs.grammar, context),
                        return_map: context.borrow_mut().add_return_map(smallvec![reduce_matrix[0]]),
                    }
                } else {
                    Connection {
                        entry_point: lhs.clone(),
                        return_map: context
                            .borrow_mut()
                            .add_return_map(smallvec![reduce_matrix[0], reduce_matrix[3]]),
                    }
                }
            }

            (GcflobddNodeType::Internal(lhs_node), GcflobddNodeType::Internal(rhs_node)) => {
                let mut connection_pair_list = smallvec![(0usize, 0usize)];
                let mut product_connections = Vec::with_capacity(lhs_node.connections.len());
                for i in 0..lhs_node.connections.len() - 1 {
                    let lhs_connection_list = &lhs_node.connections[i];
                    let rhs_connection_list = &rhs_node.connections[i];

                    let size_first = lhs_node.connections[i + 1].len();
                    let size_second = rhs_node.connections[i + 1].len();

                    let mut new_connection_pair_list =
                        ReturnMapT::with_capacity(size_first * size_second);
                    let mut exit_lookup = vec![usize::MAX; size_first * size_second];

                    let new_connections: ConnectionLayer = connection_pair_list
                        .into_iter()
                        .map(|(j, k)| {
                            let lhs_connection: &Connection<'grammar> =
                                &lhs_connection_list[j];
                            let rhs_connection: &Connection<'grammar> =
                                &rhs_connection_list[k];
                            let ConnectionPair {
                                entry_point,
                                return_map: new_inner_pairs,
                            } = Self::pair_product(
                                &lhs_connection.entry_point,
                                &rhs_connection.entry_point,
                                context,
                            );
                            let mut new_outer_pairs =
                                ReturnMap::with_capacity(new_inner_pairs.len());
                            for (inner_j, inner_k) in new_inner_pairs {
                                let outer_j = lhs_connection.return_map[inner_j];
                                let outer_k = rhs_connection.return_map[inner_k];
                                let index = outer_j * size_second + outer_k;

                                if exit_lookup[index] == usize::MAX {
                                    new_connection_pair_list.push((outer_j, outer_k));
                                    new_outer_pairs.push(new_connection_pair_list.len() - 1);
                                    exit_lookup[index] = new_connection_pair_list.len() - 1;
                                } else {
                                    new_outer_pairs.push(exit_lookup[index]);
                                }
                            }
                            Connection {
                                entry_point,
                                return_map: context.borrow_mut().add_return_map(new_outer_pairs),
                            }
                        })
                        .collect();
                    product_connections.push(new_connections);
                    connection_pair_list = new_connection_pair_list;
                }
                let lhs_connection_list = &lhs_node.connections[lhs_node.connections.len() - 1];
                let rhs_connection_list = &rhs_node.connections[rhs_node.connections.len() - 1];

                #[cfg(feature = "fx-hash")]
                let mut new_connection_hashes = HashMap::default();
                #[cfg(not(feature = "fx-hash"))]
                let mut new_connection_hashes = HashMap::new();

                let mut new_connections = ConnectionLayer::with_capacity(connection_pair_list.len());

                let mut exit_lookup = vec![usize::MAX; num_exits];
                let mut return_map = ReturnMap::with_capacity(num_exits);

                // 2. recursively call pair_map for connection layer n - 1
                let mut layer_reduce_map = connection_pair_list
                    .into_iter()
                    .map(|(j, k)| {
                        let lhs_connection = &lhs_connection_list[j];
                        let rhs_connection = &rhs_connection_list[k];
                        let mut connection_exit_lookup = vec![usize::MAX; num_exits];

                        let mut inner_value_map = Vec::with_capacity(num_exits);
                        let mut inner_reduce_map = ExitVec::with_capacity(
                            rhs_connection.return_map.len() * lhs_connection.return_map.len(),
                        );
                        for rt_k in rhs_connection.return_map.iter() {
                            for rt_j in lhs_connection.return_map.iter() {
                                let ans = reduce_matrix[*rt_k * lhs.num_exits + *rt_j];
                                if connection_exit_lookup[ans] == usize::MAX {
                                    connection_exit_lookup[ans] = inner_value_map.len();
                                    inner_value_map.push(ans);
                                }
                                inner_reduce_map.push(connection_exit_lookup[ans]);
                            }
                        }

                        let inner_reduce_map =
                            context.borrow_mut().add_reduce_matrix(inner_reduce_map);

                        let Connection {
                            entry_point,
                            return_map: inner_return_map,
                        } = Self::pair_map(
                            &lhs_connection.entry_point,
                            &rhs_connection.entry_point,
                            &inner_reduce_map,
                            inner_value_map.len(),
                            context,
                        );
                        let mut mapped_return_map =
                            ReturnMap::with_capacity(inner_return_map.len());
                        for rt in inner_return_map.iter() {
                            let ans = inner_value_map[*rt];
                            if exit_lookup[ans] == usize::MAX {
                                exit_lookup[ans] = return_map.len();
                                return_map.push(ans);
                            }
                            mapped_return_map.push(exit_lookup[ans]);
                        }
                        let new_connection = Connection {
                            entry_point,
                            return_map: context.borrow_mut().add_return_map(mapped_return_map),
                        };

                        let mut hasher = DefaultHasher::default();
                        new_connection.hash(&mut hasher);
                        let hash = hasher.finish();

                        *new_connection_hashes.entry(hash).or_insert_with(|| {
                            new_connections.push(new_connection);
                            new_connections.len() - 1
                        })
                    })
                    .collect::<Vec<_>>();
                let mut reduce_map_max = new_connection_hashes.len();

                if return_map.len() == 1 {
                    Connection {
                        entry_point: Self::mk_no_distinction(lhs.grammar, context),
                        return_map: context.borrow_mut().add_return_map(return_map),
                    }
                } else {
                    let mut new_connection_list: Vec<MaybeUninit<ConnectionLayer<'grammar>>> =
                        Vec::with_capacity(lhs_node.connections.len());
                    unsafe {
                        new_connection_list.set_len(lhs_node.connections.len());
                    }
                    new_connection_list[lhs_node.connections.len() - 1].write(new_connections);

                    // 3. call reduce for connection 0..n-1 (backwards)
                    for (idx, connection_list) in product_connections.iter().enumerate().rev() {
                        if reduce_map_max == layer_reduce_map.len() {
                            for (i, new_connection) in
                                new_connection_list.iter_mut().enumerate().take(idx + 1)
                            {
                                new_connection.write(product_connections[i].clone());
                            }
                            break;
                        }
                        #[cfg(feature = "fx-hash")]
                        let mut new_connection_hashes = HashMap::default();
                        #[cfg(not(feature = "fx-hash"))]
                        let mut new_connection_hashes = HashMap::new();

                        let mut new_connections =
                            ConnectionLayer::with_capacity(connection_list.len());

                        let new_reduce_map = connection_list
                            .iter()
                            .map(|connection| {
                                let mut inverse_lookup = vec![usize::MAX; reduce_map_max];
                                let mut num_outs = 0;
                                let mut new_return_map =
                                    ReturnMap::with_capacity(connection.return_map.len());
                                let reduce_map_outer = connection
                                    .return_map
                                    .iter()
                                    .map(|x| {
                                        let ans = layer_reduce_map[*x];
                                        if inverse_lookup[ans] == usize::MAX {
                                            num_outs += 1;
                                            new_return_map.push(ans);
                                            inverse_lookup[ans] = num_outs - 1;
                                        }
                                        ans
                                    })
                                    .collect::<Vec<_>>();
                                let reduce_map_inner = reduce_map_outer
                                    .iter()
                                    .map(|x| inverse_lookup[*x])
                                    .collect::<ExitVec>();
                                let new_entry = GcflobddNode::reduce(
                                    &connection.entry_point,
                                    reduce_map_inner.into(),
                                    new_return_map.len(),
                                    context,
                                );
                                let new_connection = ConnectionT {
                                    entry_point: new_entry,
                                    return_map: context.borrow_mut().add_return_map(new_return_map),
                                };
                                let mut hasher = DefaultHasher::default();
                                new_connection.hash(&mut hasher);
                                let hash = hasher.finish();

                                *new_connection_hashes.entry(hash).or_insert_with(|| {
                                    new_connections.push(new_connection);
                                    new_connections.len() - 1
                                })
                            })
                            .collect();
                        reduce_map_max = new_connection_hashes.len();
                        layer_reduce_map = new_reduce_map;
                        new_connection_list[idx].write(new_connections);
                    }

                    let new_connection_list = unsafe {
                        std::mem::transmute::<
                            Vec<MaybeUninit<ConnectionLayer>>,
                            Vec<ConnectionLayer>,
                        >(new_connection_list)
                    };

                    // Not every value the reduce matrix can produce is
                    // necessarily reachable, so the node's exits are the ones
                    // `return_map` actually collected, not `num_exits`.
                    let entry_point = context.borrow_mut().add_gcflobdd_node(Self {
                        num_exits: return_map.len(),
                        grammar: lhs.grammar,
                        node: GcflobddNodeType::Internal(InternalNode {
                            connections: new_connection_list,
                        }),
                    });
                    let return_map = context.borrow_mut().add_return_map(return_map);

                    Connection {
                        entry_point,
                        return_map,
                    }
                }
            }
            (GcflobddNodeType::Bdd(lhs_bdd), GcflobddNodeType::Bdd(rhs_bdd)) => {
                let product = lhs_bdd.pair_map(
                    rhs_bdd,
                    reduce_matrix,
                    lhs.num_exits,
                    rhs.num_exits,
                    context,
                );
                if product.return_map.len() == 1 {
                    Connection {
                        entry_point: Self::mk_no_distinction(lhs.grammar, context),
                        return_map: product.return_map,
                    }
                } else {
                    Connection {
                        entry_point: context.borrow_mut().add_gcflobdd_node(Self {
                            num_exits: product.return_map.len(),
                            grammar: lhs.grammar,
                            node: GcflobddNodeType::Bdd(Bdd(product.entry_point)),
                        }),
                        return_map: product.return_map,
                    }
                }
            }
            _ => unreachable!("Invalid configuration for grammar"),
        };
        context
            .borrow_mut()
            .set_pair_map_cache(lhs, rhs, reduce_matrix, ans.clone());
        ans
    }
    pub fn reduce(
        this: &Rch<Self>,
        reduce_map: ReduceMap,
        num_exits: usize,
        context: &RefCell<Context<'grammar>>,
    ) -> Rch<Self> {
        if num_exits == 1 {
            return Self::mk_no_distinction(this.grammar, context);
        }
        // is identity map. This is guaranteed by the generation process.
        if num_exits == reduce_map.len() {
            debug_assert!(reduce_map.iter().enumerate().all(|(i, x)| *x == i));
            return this.clone();
        }

        if let Some(t) = context.borrow().get_reduction_cache(this, &reduce_map) {
            return t;
        }
        let cache_reduce_map = reduce_map.clone();
        let ans = match &this.node {
            GcflobddNodeType::DontCare => context.borrow_mut().get_gcflobdd_node(this).unwrap(),
            GcflobddNodeType::Fork => context.borrow_mut().get_gcflobdd_node(this).unwrap(),
            GcflobddNodeType::Internal(internal_node) => {
                // If this is a don't care, the reduce_map should be &[0].
                // The early return `if num_exits == 1` handles the `reduce_map.iter().all(|x| *x == 0)` case.
                let mut reduce_map_max = num_exits;
                let mut layer_reduce_map = reduce_map.clone();
                let mut new_connection_list: Vec<MaybeUninit<ConnectionLayer<'grammar>>> =
                    Vec::with_capacity(internal_node.connections.len());
                unsafe {
                    new_connection_list.set_len(internal_node.connections.len());
                }

                for (idx, connection_list) in internal_node.connections.iter().enumerate().rev() {
                    if reduce_map_max == layer_reduce_map.len() {
                        // if layer is identity, upper layers should be identity too.
                        // calling clone instead of memcpy to correctly update ref count.
                        for (i, new_connection) in
                            new_connection_list.iter_mut().enumerate().take(idx + 1)
                        {
                            new_connection.write(internal_node.connections[i].clone());
                        }
                        break;
                    }
                    #[cfg(feature = "fx-hash")]
                    let mut new_connection_hashes = HashMap::default();
                    #[cfg(not(feature = "fx-hash"))]
                    let mut new_connection_hashes = HashMap::new();

                    let mut new_connections =
                        ConnectionLayer::with_capacity(connection_list.len());

                    let new_reduce_map = connection_list
                        .iter()
                        .map(|connection| {
                            // first appearance of a value.
                            let mut inverse_lookup = vec![usize::MAX; reduce_map_max];
                            let mut num_outs = 0;
                            let mut new_return_map =
                                ReturnMap::with_capacity(connection.return_map.len());
                            let reduce_map_outer = connection
                                .return_map
                                .iter()
                                .map(|x| {
                                    let ans = layer_reduce_map[*x];
                                    if inverse_lookup[ans] == usize::MAX {
                                        num_outs += 1;
                                        new_return_map.push(ans);
                                        inverse_lookup[ans] = num_outs - 1;
                                    }
                                    ans
                                })
                                .collect::<Vec<_>>();
                            let reduce_map_inner = reduce_map_outer
                                .iter()
                                .map(|x| inverse_lookup[*x])
                                .collect::<ExitVec>();
                            let new_entry = GcflobddNode::reduce(
                                &connection.entry_point,
                                reduce_map_inner.into(),
                                new_return_map.len(),
                                context,
                            );
                            // hash should exist, since the new entry is freshly created and added to the context;
                            let new_connection = ConnectionT {
                                entry_point: new_entry,
                                return_map: context.borrow_mut().add_return_map(new_return_map),
                            };
                            let mut hasher = DefaultHasher::default();
                            new_connection.hash(&mut hasher);
                            let hash = hasher.finish();

                            *new_connection_hashes.entry(hash).or_insert_with(|| {
                                new_connections.push(new_connection);
                                new_connections.len() - 1
                            })
                        })
                        .collect();
                    reduce_map_max = new_connection_hashes.len();
                    layer_reduce_map = new_reduce_map;
                    new_connection_list[idx].write(new_connections);
                }
                // safe because every entry has been initialized;
                let new_connection_list = unsafe {
                    std::mem::transmute::<
                        Vec<MaybeUninit<ConnectionLayer>>,
                        Vec<ConnectionLayer>,
                    >(new_connection_list)
                };
                context.borrow_mut().add_gcflobdd_node(Self {
                    num_exits,
                    node: GcflobddNodeType::Internal(InternalNode {
                        connections: new_connection_list,
                    }),
                    grammar: this.grammar,
                })
            }
            GcflobddNodeType::Bdd(bdd_node) => {
                let node = GcflobddNodeType::Bdd(bdd_node.reduce(&reduce_map, num_exits, context));
                context.borrow_mut().add_gcflobdd_node(Self {
                    num_exits,
                    grammar: this.grammar,
                    node,
                })
            }
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

impl<'grammar> PartialEq for GcflobddNode<'grammar> {
    fn eq(&self, other: &Self) -> bool {
        self.num_exits == other.num_exits
            && Rc::ptr_eq(self.grammar, other.grammar)
            && self.node == other.node
    }
}
impl<'grammar> Eq for GcflobddNode<'grammar> {}
