use std::{cell::RefCell, rc::Rc};

use crate::{
    gcflobdd::{
        bdd::connection::{BddConnection, BddConnectionPair},
        context::Context,
        node::log2_add,
    },
    utils::hash_cache::Rch,
};

#[cfg(feature = "fx-hash")]
use rustc_hash::FxHashMap as HashMap;

#[cfg(not(feature = "fx-hash"))]
use std::collections::HashMap;
use crate::gcflobdd::return_map::{ExitVec, ReturnMapT};

#[derive(Debug, Hash, PartialEq, Eq)]
pub enum BddNode {
    Internal(BddInternalNode),
    Terminal(usize),
}

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct BddInternalNode {
    var_id: usize,
    pub(crate) zero_branch: Rch<BddNode>,
    pub(crate) one_branch: Rch<BddNode>,
}
impl BddNode {
    pub fn mk_distinction(i: usize, context: &RefCell<Context<'_>>) -> Rch<Self> {
        let zero_branch = Self::mk_terminal(0, context);
        let one_branch = Self::mk_terminal(1, context);

        context
            .borrow_mut()
            .add_bdd_node(Self::Internal(BddInternalNode {
                var_id: i,
                zero_branch,
                one_branch,
            }))
    }
    #[cfg(test)]
    pub(super) fn mk_inverse_distinction(i: usize, context: &RefCell<Context<'_>>) -> Rch<Self> {
        let zero_branch = Self::mk_terminal(1, context);
        let one_branch = Self::mk_terminal(0, context);
        context
            .borrow_mut()
            .add_bdd_node(Self::Internal(BddInternalNode {
                var_id: i,
                zero_branch,
                one_branch,
            }))
    }

    pub fn mk_hadamard_2(context: &RefCell<Context<'_>>) -> Rch<Self> {
        let zero_branch = Self::mk_terminal(0, context);
        let one_branch = Self::mk_terminal(1, context);
        let node_1 = context
            .borrow_mut()
            .add_bdd_node(Self::Internal(BddInternalNode {
                var_id: 1,
                zero_branch: zero_branch.clone(),
                one_branch,
            }));
        context
            .borrow_mut()
            .add_bdd_node(Self::Internal(BddInternalNode {
                var_id: 0,
                zero_branch,
                one_branch: node_1,
            }))
    }

    fn mk_terminal(i: usize, context: &RefCell<Context<'_>>) -> Rch<Self> {
        context.borrow_mut().add_bdd_node(Self::Terminal(i))
    }

    /// The lowest variable index this (sub-)BDD may branch on, or `bdd_size`
    /// when it is a terminal (no variables left).
    fn first_var(node: &Rch<Self>, bdd_size: usize) -> usize {
        match node.as_ref().as_ref() {
            Self::Terminal(_) => bdd_size,
            Self::Internal(inner) => inner.var_id,
        }
    }

    /// `log2` of the number of assignments (over variables `[first_var..bdd_size)`)
    /// routed to each exit of this sub-BDD. Memoized by node pointer.
    fn log2_counts_below(
        node: &Rch<Self>,
        bdd_size: usize,
        num_exits: usize,
        memo: &mut HashMap<usize, Vec<f64>>,
    ) -> Vec<f64> {
        let key = Rc::as_ptr(node) as usize;
        if let Some(v) = memo.get(&key) {
            return v.clone();
        }
        let res = match node.as_ref().as_ref() {
            Self::Terminal(t) => {
                let mut v = vec![f64::NEG_INFINITY; num_exits];
                v[*t] = 0.0;
                v
            }
            Self::Internal(inner) => {
                let zero = Self::log2_counts_below(&inner.zero_branch, bdd_size, num_exits, memo);
                let one = Self::log2_counts_below(&inner.one_branch, bdd_size, num_exits, memo);
                // Don't-care variables skipped between this node and each child.
                let skip_zero =
                    (Self::first_var(&inner.zero_branch, bdd_size) - (inner.var_id + 1)) as f64;
                let skip_one =
                    (Self::first_var(&inner.one_branch, bdd_size) - (inner.var_id + 1)) as f64;
                (0..num_exits)
                    .map(|e| log2_add(skip_zero + zero[e], skip_one + one[e]))
                    .collect()
            }
        };
        memo.insert(key, res.clone());
        res
    }

    /// `log2` of the number of assignments (over all `bdd_size` variables)
    /// routed to each exit of this BDD leaf.
    pub(super) fn log2_exit_counts(
        node: &Rch<Self>,
        bdd_size: usize,
        num_exits: usize,
    ) -> Vec<f64> {
        let mut memo = HashMap::default();
        let leading = Self::first_var(node, bdd_size) as f64;
        let below = Self::log2_counts_below(node, bdd_size, num_exits, &mut memo);
        below.into_iter().map(|c| leading + c).collect()
    }

    /// Evaluate the exit index this BDD leaf routes `assignment` to.
    /// `assignment` is indexed by the leaf-local variable id.
    pub(super) fn evaluate(&self, assignment: &[bool]) -> usize {
        match self {
            Self::Terminal(t) => *t,
            Self::Internal(inner) => {
                if assignment[inner.var_id] {
                    inner.one_branch.evaluate(assignment)
                } else {
                    inner.zero_branch.evaluate(assignment)
                }
            }
        }
    }

    pub fn find_one_path_to(
        &self,
        index: usize,
        next_var_id: usize,
        bdd_size: usize,
    ) -> Option<Vec<Option<bool>>> {
        match self {
            Self::Terminal(i) => (*i == index).then(|| vec![None; bdd_size - next_var_id]),
            Self::Internal(BddInternalNode {
                var_id,
                zero_branch,
                one_branch,
            }) => {
                let num_nones = var_id - next_var_id;
                let mut prefix = vec![None; num_nones];
                if let Some(path) = zero_branch.find_one_path_to(index, var_id + 1, bdd_size) {
                    prefix.push(Some(false));
                    Some([prefix, path].concat())
                } else if let Some(path) = one_branch.find_one_path_to(index, var_id + 1, bdd_size)
                {
                    prefix.push(Some(true));
                    Some([prefix, path].concat())
                } else {
                    None
                }
            }
        }
    }

    pub(super) fn pair_product(
        lhs: &Rch<Self>,
        rhs: &Rch<Self>,
        lhs_num_exits: usize,
        rhs_num_exits: usize,
        context: &RefCell<Context<'_>>,
    ) -> BddConnectionPair {
        if let Some(t) = context.borrow().get_bdd_pair_product_cache(lhs, rhs) {
            return t;
        }

        let mut leaf_map = HashMap::default();
        let mut return_map = ReturnMapT::with_capacity(lhs_num_exits * rhs_num_exits);
        let mut pair_cache = HashMap::default();
        let entry_point = Self::pair_product_recursive(
            lhs,
            rhs,
            context,
            &mut leaf_map,
            &mut return_map,
            &mut pair_cache,
        );
        let ans = BddConnectionPair {
            entry_point,
            return_map,
        };

        context
            .borrow_mut()
            .set_bdd_pair_product_cache(lhs, rhs, ans.clone());
        ans
    }

    fn pair_product_recursive(
        lhs: &Rch<Self>,
        rhs: &Rch<Self>,
        context: &RefCell<Context<'_>>,
        leaf_map: &mut HashMap<(usize, usize), usize>,
        return_map: &mut ReturnMapT<(usize, usize)>,
        pair_cache: &mut HashMap<(u64, u64), Rch<Self>>,
    ) -> Rch<Self> {
        let hash1 = lhs.hash_code();
        let hash2 = rhs.hash_code();
        if let Some(cached) = pair_cache.get(&(hash1, hash2)) {
            return cached.clone();
        }

        if let (BddNode::Terminal(lhs_t), BddNode::Terminal(rhs_t)) =
            (lhs.as_ref().as_ref(), rhs.as_ref().as_ref())
        {
            let pair = (*lhs_t, *rhs_t);
            let idx = *leaf_map.entry(pair).or_insert_with(|| {
                let new_idx = return_map.len();
                return_map.push(pair);
                new_idx
            });
            let ans = context.borrow_mut().add_bdd_node(Self::Terminal(idx));
            pair_cache.insert((hash1, hash2), ans.clone());
            return ans;
        }

        let (var_id, zero_l, one_l, zero_r, one_r) =
            match (lhs.as_ref().as_ref(), rhs.as_ref().as_ref()) {
                (BddNode::Internal(l), BddNode::Internal(r)) => {
                    if l.var_id == r.var_id {
                        (
                            l.var_id,
                            &l.zero_branch,
                            &l.one_branch,
                            &r.zero_branch,
                            &r.one_branch,
                        )
                    } else if l.var_id < r.var_id {
                        (l.var_id, &l.zero_branch, &l.one_branch, rhs, rhs)
                    } else {
                        (r.var_id, lhs, lhs, &r.zero_branch, &r.one_branch)
                    }
                }
                (BddNode::Internal(l), BddNode::Terminal(_)) => {
                    (l.var_id, &l.zero_branch, &l.one_branch, rhs, rhs)
                }
                (BddNode::Terminal(_), BddNode::Internal(r)) => {
                    (r.var_id, lhs, lhs, &r.zero_branch, &r.one_branch)
                }
                _ => unreachable!(),
            };

        let zero_branch =
            Self::pair_product_recursive(zero_l, zero_r, context, leaf_map, return_map, pair_cache);
        let one_branch =
            Self::pair_product_recursive(one_l, one_r, context, leaf_map, return_map, pair_cache);

        let ans = if Rc::as_ptr(&zero_branch) == Rc::as_ptr(&one_branch) {
            zero_branch
        } else {
            context
                .borrow_mut()
                .add_bdd_node(Self::Internal(BddInternalNode {
                    var_id,
                    zero_branch,
                    one_branch,
                }))
        };
        pair_cache.insert((hash1, hash2), ans.clone());
        ans
    }

    pub fn reduce(
        this: &Rch<Self>,
        reduce_map: &[usize],
        num_exits: usize,
        context: &RefCell<Context<'_>>,
    ) -> Rch<Self> {
        if num_exits == reduce_map.len() {
            return this.clone();
        }

        if let Some(t) = context.borrow().get_bdd_reduction_cache(this, reduce_map) {
            return t;
        }
        let ans = match this.as_ref().as_ref() {
            Self::Terminal(i) => context
                .borrow_mut()
                .add_bdd_node(BddNode::Terminal(reduce_map[*i])),
            Self::Internal(BddInternalNode {
                var_id,
                zero_branch,
                one_branch,
            }) => {
                let zero_branch = Self::reduce(zero_branch, reduce_map, num_exits, context);
                let one_branch = Self::reduce(one_branch, reduce_map, num_exits, context);
                if Rc::as_ptr(&zero_branch) == Rc::as_ptr(&one_branch) {
                    zero_branch
                } else {
                    context
                        .borrow_mut()
                        .add_bdd_node(BddNode::Internal(BddInternalNode {
                            var_id: *var_id,
                            zero_branch,
                            one_branch,
                        }))
                }
            }
        };
        context
            .borrow_mut()
            .set_bdd_reduction_cache(this, reduce_map, ans.clone());
        ans
    }
    pub fn pair_map(
        lhs: &Rch<Self>,
        rhs: &Rch<Self>,
        reduce_matrix: &Rch<ExitVec>,
        lhs_num_exits: usize,
        rhs_num_exits: usize,
        context: &RefCell<Context<'_>>,
    ) -> BddConnection {
        if let Some(t) = context
            .borrow()
            .get_bdd_pair_map_cache(lhs, rhs, reduce_matrix)
        {
            return t;
        }
        let mut leaf_map = HashMap::default();
        let mut return_map = ReturnMapT::with_capacity(lhs_num_exits * rhs_num_exits);
        let mut cache = HashMap::default();

        let entry_point = Self::pair_map_recursive(
            lhs,
            rhs,
            reduce_matrix,
            lhs_num_exits,
            context,
            &mut leaf_map,
            &mut return_map,
            &mut cache,
        );

        let ans = BddConnection {
            entry_point,
            return_map: context.borrow_mut().add_return_map(return_map),
        };
        context
            .borrow_mut()
            .set_bdd_pair_map_cache(lhs, rhs, reduce_matrix, ans.clone());
        ans
    }

    #[allow(clippy::too_many_arguments)]
    pub fn pair_map_recursive(
        lhs: &Rch<Self>,
        rhs: &Rch<Self>,
        reduce_map: &[usize],
        lhs_num_exits: usize,
        context: &RefCell<Context<'_>>,
        leaf_map: &mut HashMap<usize, usize>,
        return_map: &mut ReturnMapT<usize>,
        pair_cache: &mut HashMap<(u64, u64), Rch<Self>>,
    ) -> Rch<Self> {
        let hash1 = lhs.hash_code();
        let hash2 = rhs.hash_code();
        if let Some(cached) = pair_cache.get(&(hash1, hash2)) {
            return cached.clone();
        }

        if let (BddNode::Terminal(lhs_t), BddNode::Terminal(rhs_t)) =
            (lhs.as_ref().as_ref(), rhs.as_ref().as_ref())
        {
            let return_idx = reduce_map[*rhs_t * lhs_num_exits + *lhs_t];
            let idx = *leaf_map.entry(return_idx).or_insert_with(|| {
                return_map.push(return_idx);
                return_map.len() - 1
            });
            let ans = context.borrow_mut().add_bdd_node(Self::Terminal(idx));
            pair_cache.insert((hash1, hash2), ans.clone());
            return ans;
        }

        let (var_id, zero_l, one_l, zero_r, one_r) =
            match (lhs.as_ref().as_ref(), rhs.as_ref().as_ref()) {
                (BddNode::Internal(l), BddNode::Internal(r)) => {
                    if l.var_id == r.var_id {
                        (
                            l.var_id,
                            &l.zero_branch,
                            &l.one_branch,
                            &r.zero_branch,
                            &r.one_branch,
                        )
                    } else if l.var_id < r.var_id {
                        (l.var_id, &l.zero_branch, &l.one_branch, rhs, rhs)
                    } else {
                        (r.var_id, lhs, lhs, &r.zero_branch, &r.one_branch)
                    }
                }
                (BddNode::Internal(l), BddNode::Terminal(_)) => {
                    (l.var_id, &l.zero_branch, &l.one_branch, rhs, rhs)
                }
                (BddNode::Terminal(_), BddNode::Internal(r)) => {
                    (r.var_id, lhs, lhs, &r.zero_branch, &r.one_branch)
                }
                _ => unreachable!(),
            };

        let zero_branch = Self::pair_map_recursive(
            zero_l,
            zero_r,
            reduce_map,
            lhs_num_exits,
            context,
            leaf_map,
            return_map,
            pair_cache,
        );
        let one_branch = Self::pair_map_recursive(
            one_l,
            one_r,
            reduce_map,
            lhs_num_exits,
            context,
            leaf_map,
            return_map,
            pair_cache,
        );

        let ans = if Rc::as_ptr(&zero_branch) == Rc::as_ptr(&one_branch) {
            zero_branch
        } else {
            context
                .borrow_mut()
                .add_bdd_node(Self::Internal(BddInternalNode {
                    var_id,
                    zero_branch,
                    one_branch,
                }))
        };
        pair_cache.insert((hash1, hash2), ans.clone());
        ans
    }
}
