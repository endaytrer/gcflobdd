use crate::{
    gcflobdd::{bdd::Bdd, connection::Connection, context::Context},
    grammar::{GrammarNode, GrammarNodeType},
    utils::hash_cache::Rch,
};
use std::{
    cell::RefCell,
    hash::{Hash, Hasher},
    rc::Rc,
};
#[cfg(not(feature = "fx-hash"))]
use std::{collections::HashMap, hash::DefaultHasher};

pub struct GcflobddNode<'grammar> {
    num_exits: usize,
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

#[derive(Debug, Hash, PartialEq, Eq)]
pub(super) enum GcflobddNodeType<'grammar> {
    DontCare,
    Fork,
    Internal(InternalNode<'grammar>),
    Bdd(Bdd),
}

#[derive(PartialEq, Eq)]
pub(super) struct InternalNode<'grammar>(pub(super) Rch<Connection<'grammar>>);

impl std::fmt::Debug for InternalNode<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("InternalNode").field(&self.0).finish()
    }
}

impl Hash for InternalNode<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

pub(super) type PairProductResult<'grammar> = (Rch<GcflobddNode<'grammar>>, Vec<(usize, usize)>);
pub(super) type PairMapResult<'grammar> = (Rch<GcflobddNode<'grammar>>, Vec<usize>);
impl<'grammar> GcflobddNode<'grammar> {
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
                GrammarNodeType::Internal(grammar_nodes) => GcflobddNodeType::Internal(
                    InternalNode(Connection::mk_distinction(i, grammar_nodes, context)),
                ),
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
    pub fn find_one_path_to(&self, value: usize) -> Vec<Option<bool>> {
        if self.num_exits == 1 {
            debug_assert_eq!(value, 0);
            return vec![None; self.grammar.num_vars];
        }
        match &self.node {
            GcflobddNodeType::Internal(internal_node) => {
                internal_node.0.find_one_path_to(&value).unwrap()
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

    // pub fn mk_balanced_hadamard_voc12(
    //     level: usize,
    //     grammar: &'grammar Rc<GrammarNode>,
    //     context: &RefCell<Context<'grammar>>,
    // ) -> Rch<Self> {
    //     if level == 2 {
    //         return Self::mk_balanced_hadamard_voc12_2(grammar, context);
    //     }

    //     let GrammarNodeType::Internal(grammar_nodes) = &grammar.node else {
    //         unreachable!("mk_hardamard_voc12 should have two groupings")
    //     };
    //     let [a, b] = &grammar_nodes[..] else {
    //         unreachable!("mk_hardamard_voc12 should have two groupings")
    //     };
    //     let conn_a = Self::mk_balanced_hadamard_voc12(level - 1, a, context);
    //     let conn_b = Self::mk_balanced_hadamard_voc12(level - 1, b, context);
    //     let node_type = GcflobddNodeType::Internal(InternalNode {
    //         connections: vec![
    //             vec![Connection::new_sequential(conn_a, context)],
    //             vec![
    //                 Connection::new_sequential(conn_b.clone(), context),
    //                 Connection::new(conn_b, vec![1, 0], context),
    //             ],
    //         ],
    //     });
    //     context.borrow_mut().add_gcflobdd_node(Self {
    //         num_exits: 2,
    //         grammar,
    //         node: node_type,
    //     })
    // }

    // pub fn mk_balanced_hadamard_voc13(
    //     level: usize,
    //     grammar: &'grammar Rc<GrammarNode>,
    //     context: &RefCell<Context<'grammar>>,
    // ) -> Rch<Self> {
    //     if level == 2 {
    //         return Self::mk_hadamard_voc13_2(grammar, context);
    //     }

    //     let GrammarNodeType::Internal(grammar_nodes) = &grammar.node else {
    //         unreachable!("mk_hardamard_voc13 should have two groupings")
    //     };
    //     let [a, b] = &grammar_nodes[..] else {
    //         unreachable!("mk_hardamard_voc13 should have two groupings")
    //     };
    //     let conn_a = Self::mk_balanced_hadamard_voc13(level - 1, a, context);
    //     let conn_b = Self::mk_balanced_hadamard_voc13(level - 1, b, context);
    //     let node_type = GcflobddNodeType::Internal(InternalNode {
    //         connections: vec![
    //             vec![Connection::new_sequential(conn_a, context)],
    //             vec![
    //                 Connection::new_sequential(conn_b.clone(), context),
    //                 Connection::new(conn_b, vec![1, 0], context),
    //             ],
    //         ],
    //     });
    //     context.borrow_mut().add_gcflobdd_node(Self {
    //         num_exits: 2,
    //         grammar,
    //         node: node_type,
    //     })
    // }
    // fn mk_balanced_hadamard_voc12_2(
    //     grammar: &'grammar Rc<GrammarNode>,
    //     context: &RefCell<Context<'grammar>>,
    // ) -> Rch<Self> {
    //     let GrammarNodeType::Internal(grammar_nodes) = &grammar.node else {
    //         unreachable!("mk_hardamard_voc12_2 should have 4 variables with two groupings")
    //     };
    //     let [a, b] = &grammar_nodes[..] else {
    //         unreachable!("mk_hardamard_voc12_2 should have 4 variables with two groupings")
    //     };
    //     let a_conn = Self::mk_balanced_hadamard_2(a, context);
    //     let b_conn = Self::mk_no_distinction(b, context);
    //     let node_type = GcflobddNodeType::Internal(InternalNode {
    //         connections: vec![
    //             vec![Connection::new_sequential(a_conn, context)],
    //             vec![
    //                 Connection::new_sequential(b_conn.clone(), context),
    //                 Connection::new_sequential(b_conn, context),
    //             ],
    //         ],
    //     });
    //     context.borrow_mut().add_gcflobdd_node(Self {
    //         num_exits: 2,
    //         grammar,
    //         node: node_type,
    //     })
    // }

    // fn mk_hadamard_voc13_2(
    //     grammar: &'grammar Rc<GrammarNode>,
    //     context: &RefCell<Context<'grammar>>,
    // ) -> Rch<Self> {
    //     let GrammarNodeType::Internal(grammar_nodes) = &grammar.node else {
    //         unreachable!("mk_hardamard_voc12_2 should have 4 variables with two groupings")
    //     };
    //     let [a, b] = &grammar_nodes[..] else {
    //         unreachable!("mk_hardamard_voc12_2 should have 4 variables with two groupings")
    //     };
    //     let a_conn = Self::mk_distinction(0, a, context);
    //     let b0_conn = Self::mk_no_distinction(b, context);
    //     let b1_conn = Self::mk_distinction(0, b, context);
    //     let node_type = GcflobddNodeType::Internal(InternalNode {
    //         connections: vec![
    //             vec![Connection::new_sequential(a_conn, context)],
    //             vec![
    //                 Connection::new_sequential(b0_conn, context),
    //                 Connection::new_sequential(b1_conn, context),
    //             ],
    //         ],
    //     });
    //     context.borrow_mut().add_gcflobdd_node(Self {
    //         num_exits: 3,
    //         grammar,
    //         node: node_type,
    //     })
    // }

    // fn mk_balanced_hadamard_2(
    //     grammar: &'grammar Rc<GrammarNode>,
    //     context: &RefCell<Context<'grammar>>,
    // ) -> Rch<Self> {
    //     let node_type = match &grammar.node {
    //         GrammarNodeType::Internal(grammar_nodes) => {
    //             let [a, b] = &grammar_nodes[..] else {
    //                 unreachable!("mk_hadamard_2 should have two variables")
    //             };
    //             debug_assert!(
    //                 matches!(a.node, GrammarNodeType::Terminal),
    //                 "mk_hadamard_2 should have two variables"
    //             );
    //             debug_assert!(
    //                 matches!(b.node, GrammarNodeType::Terminal),
    //                 "mk_hadamard_2 should have two variables"
    //             );
    //             let a_conn = Self::mk_distinction(0, a, context);
    //             let b0_conn = Self::mk_no_distinction(b, context);
    //             let b1_conn = Self::mk_distinction(0, b, context);
    //             GcflobddNodeType::Internal(InternalNode {
    //                 connections: vec![
    //                     vec![Connection::new_sequential(a_conn, context)],
    //                     vec![
    //                         Connection::new_sequential(b0_conn, context),
    //                         Connection::new_sequential(b1_conn, context),
    //                     ],
    //                 ],
    //             })
    //         }
    //         GrammarNodeType::Bdd(2) => GcflobddNodeType::Bdd(Bdd::mk_hadamard_2(context)),
    //         _ => unreachable!("mk_hadamard_2 should have two variables"),
    //     };
    //     context.borrow_mut().add_gcflobdd_node(Self {
    //         num_exits: 2,
    //         grammar,
    //         node: node_type,
    //     })
    // }

    pub fn pair_product(
        lhs: &Rch<Self>,
        rhs: &Rch<Self>,
        context: &RefCell<Context<'grammar>>,
    ) -> PairProductResult<'grammar> {
        // should be the same grammar
        debug_assert_eq!(lhs.grammar, rhs.grammar);
        if let Some(t) = context.borrow().get_pair_product_cache(lhs, rhs) {
            return t;
        }
        if Rc::ptr_eq(lhs, rhs) {
            let ans = (lhs.clone(), (0..lhs.num_exits).map(|i| (i, i)).collect());
            context
                .borrow_mut()
                .set_pair_product_cache(lhs, rhs, ans.clone());
            return ans;
        }
        let ans = match (&lhs.node, &rhs.node) {
            (GcflobddNodeType::DontCare, GcflobddNodeType::DontCare) => {
                (Self::mk_no_distinction(lhs.grammar, context), vec![(0, 0)])
            }
            (GcflobddNodeType::DontCare, _) => {
                (rhs.clone(), (0..rhs.num_exits).map(|i| (0, i)).collect())
            }
            (_, GcflobddNodeType::DontCare) => {
                (lhs.clone(), (0..lhs.num_exits).map(|i| (i, 0)).collect())
            }
            (GcflobddNodeType::Internal(lhs_node), GcflobddNodeType::Internal(rhs_node)) => {
                let mut exit_lookup = vec![usize::MAX; lhs.num_exits * rhs.num_exits];
                let mut outer_return_map = Vec::with_capacity(lhs.num_exits * rhs.num_exits);
                let connection = Connection::pair_product(
                    &lhs_node.0,
                    &rhs_node.0,
                    rhs.num_exits,
                    &mut exit_lookup,
                    &mut outer_return_map,
                    context,
                );

                if outer_return_map.len() == 1 {
                    (
                        Self::mk_no_distinction(lhs.grammar, context),
                        outer_return_map,
                    )
                } else {
                    (
                        context.borrow_mut().add_gcflobdd_node(Self {
                            grammar: lhs.grammar,
                            num_exits: outer_return_map.len(),
                            node: GcflobddNodeType::Internal(InternalNode(connection)),
                        }),
                        outer_return_map,
                    )
                }
            }
            (GcflobddNodeType::Bdd(lhs_bdd), GcflobddNodeType::Bdd(rhs_bdd)) => {
                let product = lhs_bdd.pair_product(rhs_bdd, lhs.num_exits, rhs.num_exits, context);
                (
                    context.borrow_mut().add_gcflobdd_node(Self {
                        num_exits: product.return_map.len(),
                        grammar: lhs.grammar,
                        node: GcflobddNodeType::Bdd(Bdd(product.entry_point)),
                    }),
                    product.return_map,
                )
            }
            (GcflobddNodeType::Fork, GcflobddNodeType::Fork) => (lhs.clone(), vec![(0, 0), (1, 1)]),
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
        op_matrix: &Rch<Vec<usize>>, // should be a ReduceMap if either lhs / rhs is a dont care
        num_exits: usize,            // it's only used in reduce of don't care and normal nodes
        context: &RefCell<Context<'grammar>>,
    ) -> PairMapResult<'grammar> {
        if num_exits == 1 {
            return (
                Self::mk_no_distinction(lhs.grammar, context),
                vec![op_matrix[0]],
            );
        }
        if let Some(t) = context.borrow().get_pair_map_cache(lhs, rhs, op_matrix) {
            return t;
        }

        let ans = match (&lhs.node, &rhs.node) {
            (GcflobddNodeType::DontCare, GcflobddNodeType::DontCare) => {
                debug_assert_eq!(op_matrix.len(), 1);
                (
                    Self::mk_no_distinction(lhs.grammar, context),
                    op_matrix.as_ref().as_ref().clone(),
                )
            }
            (GcflobddNodeType::DontCare, _) => {
                debug_assert_eq!(op_matrix.len(), rhs.num_exits);
                (
                    Self::reduce(rhs, op_matrix, num_exits, context),
                    (0..num_exits).collect(),
                )
            }
            (_, GcflobddNodeType::DontCare) => {
                debug_assert_eq!(op_matrix.len(), lhs.num_exits);
                (
                    Self::reduce(lhs, op_matrix, num_exits, context),
                    (0..num_exits).collect(),
                )
            }
            (GcflobddNodeType::Fork, GcflobddNodeType::Fork) => {
                debug_assert_eq!(op_matrix.len(), 4);
                if op_matrix[0] == op_matrix[3] {
                    (
                        Self::mk_no_distinction(lhs.grammar, context),
                        vec![op_matrix[0]],
                    )
                } else {
                    (lhs.clone(), vec![op_matrix[0], op_matrix[3]])
                }
            }

            (GcflobddNodeType::Internal(lhs_node), GcflobddNodeType::Internal(rhs_node)) => {
                let mut outer_return_map = Vec::with_capacity(lhs.num_exits * rhs.num_exits);
                let mut exit_lookup = vec![usize::MAX; lhs.num_exits * rhs.num_exits];
                let connection = Connection::pair_map(
                    &lhs_node.0,
                    &rhs_node.0,
                    op_matrix,
                    num_exits,
                    rhs.num_exits,
                    &mut exit_lookup,
                    &mut outer_return_map,
                    context,
                );
                if outer_return_map.len() == 1 {
                    (
                        Self::mk_no_distinction(lhs.grammar, context),
                        outer_return_map,
                    )
                } else {
                    let node = Self {
                        grammar: lhs.grammar,
                        num_exits: outer_return_map.len(),
                        node: GcflobddNodeType::Internal(InternalNode(connection)),
                    };
                    (
                        context.borrow_mut().add_gcflobdd_node(node),
                        outer_return_map,
                    )
                }
            }
            (GcflobddNodeType::Bdd(lhs_bdd), GcflobddNodeType::Bdd(rhs_bdd)) => {
                let product =
                    lhs_bdd.pair_map(rhs_bdd, op_matrix, lhs.num_exits, rhs.num_exits, context);
                if product.return_map.len() == 1 {
                    (
                        Self::mk_no_distinction(lhs.grammar, context),
                        product.return_map,
                    )
                } else {
                    (
                        context.borrow_mut().add_gcflobdd_node(Self {
                            num_exits: product.return_map.len(),
                            grammar: lhs.grammar,
                            node: GcflobddNodeType::Bdd(Bdd(product.entry_point)),
                        }),
                        product.return_map,
                    )
                }
            }
            _ => unreachable!("Invalid configuration for grammar"),
        };
        context
            .borrow_mut()
            .set_pair_map_cache(lhs, rhs, op_matrix, ans.clone());
        ans
    }
    pub fn reduce(
        this: &Rch<Self>,
        reduce_map: &[usize],
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

        if let Some(t) = context.borrow().get_reduction_cache(this, reduce_map) {
            return t;
        }
        let ans = match &this.node {
            GcflobddNodeType::DontCare => unreachable!(
                "should not reduce a don't care node, it should have been handled at the beginning of the function"
            ),
            // can only be two exits, the possibility of only having one exit is handled at the beginning of the function
            GcflobddNodeType::Fork => context.borrow_mut().get_gcflobdd_node(this).unwrap(),
            GcflobddNodeType::Internal(internal_node) => {
                let node = GcflobddNodeType::Internal(InternalNode(Connection::reduce(
                    &internal_node.0,
                    reduce_map,
                    num_exits,
                    context,
                )));
                context.borrow_mut().add_gcflobdd_node(Self {
                    num_exits,
                    grammar: this.grammar,
                    node,
                })
            }
            GcflobddNodeType::Bdd(bdd_node) => {
                let node = GcflobddNodeType::Bdd(bdd_node.reduce(reduce_map, num_exits, context));
                context.borrow_mut().add_gcflobdd_node(Self {
                    num_exits,
                    grammar: this.grammar,
                    node,
                })
            }
        };
        context
            .borrow_mut()
            .set_reduction_cache(this, reduce_map, ans.clone());
        ans
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
