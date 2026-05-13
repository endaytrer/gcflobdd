use std::{cell::RefCell, hash::Hash, rc::Rc};

use crate::{
    gcflobdd::{context::Context, node::GcflobddNode},
    grammar::GrammarNode,
    utils::hash_cache::Rch,
};

#[cfg(feature = "fx-hash")]
use rustc_hash::FxHashMap as HashMap;
#[cfg(not(feature = "fx-hash"))]
use std::collections::HashMap;

#[derive(Debug, Clone, Hash)]
pub enum ReturnMapT<'grammar, T: Hash> {
    NonTerminal(Vec<Rch<ConnectionT<'grammar, T>>>),
    Terminal(Vec<T>),
}

impl<T: PartialEq + Hash> PartialEq for ReturnMapT<'_, T> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            // guarenteed to be the same if they are the same pointer, since they are deduplicated
            (ReturnMapT::NonTerminal(a), ReturnMapT::NonTerminal(b)) => {
                a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| Rc::ptr_eq(x, y))
            }
            (ReturnMapT::Terminal(a), ReturnMapT::Terminal(b)) => a == b,
            _ => false,
        }
    }
}
impl<T: Eq + Hash> Eq for ReturnMapT<'_, T> {}

type ReturnMap<'grammar> = ReturnMapT<'grammar, usize>;

#[derive(Clone)]
pub(super) struct ConnectionT<'grammar, T: Hash> {
    pub entry_point: Rch<GcflobddNode<'grammar>>,
    pub return_map: ReturnMapT<'grammar, T>,
}

impl<'grammar, T: Hash> Hash for ConnectionT<'grammar, T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.entry_point.hash(state);
        self.return_map.hash(state);
    }
}

impl<'grammar, T: PartialEq + Hash> PartialEq for ConnectionT<'grammar, T> {
    fn eq(&self, other: &Self) -> bool {
        Rc::as_ptr(&self.entry_point) == Rc::as_ptr(&other.entry_point)
            && self.return_map == other.return_map
    }
}
impl<'grammar, T: Eq + Hash> Eq for ConnectionT<'grammar, T> {}

impl<'grammar, T: std::fmt::Debug + Hash> std::fmt::Debug for ConnectionT<'grammar, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionT")
            .field("entry_point", &Rc::as_ptr(&self.entry_point))
            .field("return_map", &self.return_map)
            .finish()
    }
}

pub(crate) type Connection<'grammar> = ConnectionT<'grammar, usize>;

impl<'grammar> Connection<'grammar> {
    pub fn mk_no_distinction(
        value: usize,
        grammar: &'grammar [Rc<GrammarNode>],
        context: &RefCell<Context<'grammar>>,
    ) -> Rch<Self> {
        let ans = if grammar.len() == 1 {
            Self {
                entry_point: GcflobddNode::mk_no_distinction(&grammar[0], context),
                return_map: ReturnMap::Terminal(vec![value]),
            }
        } else {
            Self {
                entry_point: GcflobddNode::mk_no_distinction(&grammar[0], context),
                return_map: ReturnMap::NonTerminal(vec![Self::mk_no_distinction(
                    value,
                    &grammar[1..],
                    context,
                )]),
            }
        };
        context.borrow_mut().add_connection(ans)
    }

    pub fn mk_distinction(
        i: usize,
        grammar: &'grammar [Rc<GrammarNode>],
        context: &RefCell<Context<'grammar>>,
    ) -> Rch<Self> {
        let ans = if grammar.len() == 1 {
            Self {
                entry_point: GcflobddNode::mk_distinction(i, &grammar[0], context),
                return_map: ReturnMap::Terminal(vec![0, 1]),
            }
        } else if i < grammar[0].num_vars {
            Self {
                entry_point: GcflobddNode::mk_distinction(i, &grammar[0], context),
                return_map: ReturnMap::NonTerminal(vec![
                    Self::mk_no_distinction(0, &grammar[1..], context),
                    Self::mk_no_distinction(1, &grammar[1..], context),
                ]),
            }
        } else {
            Self {
                entry_point: GcflobddNode::mk_no_distinction(&grammar[0], context),
                return_map: ReturnMap::NonTerminal(vec![Self::mk_distinction(
                    i - grammar[0].num_vars,
                    &grammar[1..],
                    context,
                )]),
            }
        };
        context.borrow_mut().add_connection(ans)
    }

    pub fn pair_product(
        lhs: &Rch<Self>,
        rhs: &Rch<Self>,
        rhs_num_exits: usize,
        exit_lookup: &mut [usize],
        outer_return_map: &mut Vec<(usize, usize)>,
        context: &RefCell<Context<'grammar>>,
    ) -> Rch<Self> {
        let (inner_product, inner_pairs) =
            GcflobddNode::pair_product(&lhs.entry_point, &rhs.entry_point, context);
        let ans = match (&lhs.return_map, &rhs.return_map) {
            (ReturnMap::NonTerminal(lhs_next), ReturnMap::NonTerminal(rhs_next)) => Connection {
                entry_point: inner_product,
                return_map: ReturnMap::NonTerminal(
                    inner_pairs
                        .into_iter()
                        .map(|(i, j)| {
                            Self::pair_product(
                                &lhs_next[i],
                                &rhs_next[j],
                                rhs_num_exits,
                                exit_lookup,
                                outer_return_map,
                                context,
                            )
                        })
                        .collect(),
                ),
            },
            (ReturnMap::Terminal(lhs_terminal), ReturnMap::Terminal(rhs_terminal)) => {
                let inner_return_map = inner_pairs
                    .into_iter()
                    .map(|(i, j)| {
                        let outer_i = lhs_terminal[i];
                        let outer_j = rhs_terminal[j];
                        let index = outer_i * rhs_num_exits + outer_j;
                        if exit_lookup[index] == usize::MAX {
                            exit_lookup[index] = outer_return_map.len();
                            outer_return_map.push((outer_i, outer_j));
                        }
                        exit_lookup[index]
                    })
                    .collect();
                Connection {
                    entry_point: inner_product,
                    return_map: ReturnMap::Terminal(inner_return_map),
                }
            }
            _ => unreachable!(),
        };
        context.borrow_mut().add_connection(ans)
    }
    pub fn reduce(
        this: &Rch<Self>,
        reduce_map: &[usize],
        num_exits: usize,
        context: &RefCell<Context<'grammar>>,
    ) -> Rch<Self> {
        let ans = match &this.return_map {
            ReturnMap::NonTerminal(next_connections) => {
                #[cfg(feature = "fx-hash")]
                let mut value_hash_map = HashMap::default();
                #[cfg(not(feature = "fx-hash"))]
                let mut value_hash_map = HashMap::new();
                let mut inner_return_map = Vec::with_capacity(next_connections.len());

                let inner_reduce_map = next_connections
                    .iter()
                    .map(|node| {
                        let reduced = Self::reduce(node, reduce_map, num_exits, context);
                        *value_hash_map.entry(reduced.clone()).or_insert_with(|| {
                            inner_return_map.push(reduced.clone());
                            inner_return_map.len() - 1
                        })
                    })
                    .collect::<Vec<_>>();
                let reduced_entry_point = GcflobddNode::reduce(
                    &this.entry_point,
                    &inner_reduce_map,
                    inner_return_map.len(),
                    context,
                );
                Connection {
                    entry_point: reduced_entry_point,
                    return_map: ReturnMap::NonTerminal(inner_return_map),
                }
            }
            ReturnMap::Terminal(terminal) => {
                // the lookup table for reduce map
                let mut inner_lookup = vec![usize::MAX; num_exits];
                let mut inner_return_map = Vec::with_capacity(terminal.len());
                let inner_reduce_map = terminal
                    .iter()
                    .map(|t| {
                        let reduced_index = reduce_map[*t];
                        if inner_lookup[reduced_index] == usize::MAX {
                            inner_lookup[reduced_index] = inner_return_map.len();
                            inner_return_map.push(reduced_index);
                        }
                        inner_lookup[reduced_index]
                    })
                    .collect::<Vec<_>>();
                let reduced_entry_point = GcflobddNode::reduce(
                    &this.entry_point,
                    &inner_reduce_map,
                    inner_return_map.len(),
                    context,
                );
                Connection {
                    entry_point: reduced_entry_point,
                    return_map: ReturnMap::Terminal(inner_return_map),
                }
            }
        };
        context.borrow_mut().add_connection(ans)
    }
    /// size of op_matrix: lhs_num_exits * rhs_num_exits
    /// num_exits: should be the largest value in op_matrix + 1
    #[allow(clippy::too_many_arguments)]
    pub fn pair_map(
        lhs: &Rch<Self>,
        rhs: &Rch<Self>,
        op_matrix: &Rch<Vec<usize>>,
        num_exits: usize,
        rhs_num_exits: usize,
        exit_lookup: &mut [usize],
        outer_return_map: &mut Vec<usize>,
        context: &RefCell<Context<'grammar>>,
    ) -> Rch<Self> {
        let ans = match (&lhs.return_map, &rhs.return_map) {
            (ReturnMap::NonTerminal(lhs_next), ReturnMap::NonTerminal(rhs_next)) => {
                let (inner_product, inner_pairs) =
                    GcflobddNode::pair_product(&lhs.entry_point, &rhs.entry_point, context);

                #[cfg(feature = "fx-hash")]
                let mut value_hash_map = HashMap::default();
                #[cfg(not(feature = "fx-hash"))]
                let mut value_hash_map = HashMap::new();
                let mut inner_return_map = Vec::with_capacity(inner_pairs.len());
                let inner_reduce_map = inner_pairs
                    .into_iter()
                    .map(|(i, j)| {
                        let conn = Self::pair_map(
                            &lhs_next[i],
                            &rhs_next[j],
                            op_matrix,
                            num_exits,
                            rhs_num_exits,
                            exit_lookup,
                            outer_return_map,
                            context,
                        );
                        *value_hash_map.entry(conn.clone()).or_insert_with(|| {
                            inner_return_map.push(conn.clone());
                            inner_return_map.len() - 1
                        })
                    })
                    .collect::<Vec<_>>();
                let reduced_entry_point = GcflobddNode::reduce(
                    &inner_product,
                    &inner_reduce_map,
                    inner_return_map.len(),
                    context,
                );
                Connection {
                    entry_point: reduced_entry_point,
                    return_map: ReturnMap::NonTerminal(inner_return_map),
                }
            }
            (ReturnMap::Terminal(lhs_terminal), ReturnMap::Terminal(rhs_terminal)) => {
                let mut inner_lookup = vec![usize::MAX; num_exits];
                let mut inner_value_map = Vec::with_capacity(num_exits);
                // the op-matrix should always have the lowest value 0 and the highest value num_exits - 1
                let mut inner_op_matrix =
                    Vec::with_capacity(lhs_terminal.len() & rhs_terminal.len());
                for &j in rhs_terminal {
                    for &i in lhs_terminal {
                        let op_index = op_matrix[j * rhs_num_exits + i];
                        if inner_lookup[op_index] == usize::MAX {
                            inner_lookup[op_index] = inner_value_map.len();
                            inner_value_map.push(op_index);
                        }
                        inner_op_matrix.push(inner_lookup[op_index]);
                    }
                }
                let inner_op_matrix = &context.borrow_mut().add_op_matrix(inner_op_matrix);
                let (mapped_entry_point, inner_return_map) = GcflobddNode::pair_map(
                    &lhs.entry_point,
                    &rhs.entry_point,
                    inner_op_matrix,
                    inner_value_map.len(),
                    context,
                );
                let mapped_return_map = inner_return_map
                    .into_iter()
                    .map(|rt| {
                        let ans = inner_value_map[rt];
                        if exit_lookup[ans] == usize::MAX {
                            exit_lookup[ans] = outer_return_map.len();
                            outer_return_map.push(ans);
                        }
                        exit_lookup[ans]
                    })
                    .collect::<Vec<_>>();
                Connection {
                    entry_point: mapped_entry_point,
                    return_map: ReturnMap::Terminal(mapped_return_map),
                }
            }
            _ => unreachable!(),
        };
        context.borrow_mut().add_connection(ans)
    }
}

impl<'grammar, T: Eq + Hash> ConnectionT<'grammar, T> {
    pub fn find_one_path_to(&self, value: &T) -> Option<Vec<Option<bool>>> {
        match &self.return_map {
            ReturnMapT::Terminal(return_map) => {
                let idx = return_map.iter().position(|x| *x == *value)?;
                Some(self.entry_point.find_one_path_to(idx))
            }
            ReturnMapT::NonTerminal(return_map) => {
                for (i, conn) in return_map.iter().enumerate() {
                    if let Some(path) = conn.find_one_path_to(value) {
                        let path_to_next_connection = self.entry_point.find_one_path_to(i);
                        return Some([path_to_next_connection, path].concat());
                    }
                }
                None
            }
        }
    }
}
