use std::{cell::RefCell, rc::Rc};

use crate::{
    gcflobdd::{
        bdd::connection::{BddConnection, BddConnectionPair},
        context::Context,
    },
    utils::hash_cache::Rch,
};

#[derive(Debug, Hash)]
pub enum BddNode {
    Internal(BddInternalNode),
    Terminal(usize),
}

#[derive(Debug, Hash)]
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

    fn mk_terminal(i: usize, context: &RefCell<Context<'_>>) -> Rch<Self> {
        context.borrow_mut().add_bdd_node(Self::Terminal(i))
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
        context: &RefCell<Context<'_>>,
    ) -> BddConnectionPair {
        if let Some(t) = context.borrow().get_bdd_pair_product_cache(lhs, rhs) {
            return t;
        }

        let mut leaf_map = std::collections::HashMap::new();
        let mut return_map = vec![];
        let mut pair_cache = std::collections::HashMap::new();
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
        leaf_map: &mut std::collections::HashMap<(usize, usize), usize>,
        return_map: &mut Vec<(usize, usize)>,
        pair_cache: &mut std::collections::HashMap<(u64, u64), Rch<Self>>,
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
        reduce_map: &[usize],
        lhs_num_exits: usize,
        context: &RefCell<Context<'_>>,
    ) -> BddConnection {
        let mut leaf_map = std::collections::HashMap::new();
        let mut return_map = vec![];
        let mut cache = std::collections::HashMap::new();

        let entry_point = Self::pair_map_recursive(
            lhs,
            rhs,
            reduce_map,
            lhs_num_exits,
            context,
            &mut leaf_map,
            &mut return_map,
            &mut cache,
        );

        BddConnection {
            entry_point,
            return_map: context.borrow_mut().add_return_map(return_map),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn pair_map_recursive(
        lhs: &Rch<Self>,
        rhs: &Rch<Self>,
        reduce_map: &[usize],
        lhs_num_exits: usize,
        context: &RefCell<Context<'_>>,
        leaf_map: &mut std::collections::HashMap<usize, usize>,
        return_map: &mut Vec<usize>,
        pair_cache: &mut std::collections::HashMap<(u64, u64), Rch<Self>>,
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
