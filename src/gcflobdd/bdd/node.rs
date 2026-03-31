use std::cell::RefCell;

use crate::{
    gcflobdd::{bdd::connection::BddConnectionPair, context::Context},
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

        if let (BddNode::Terminal(lhs_t), BddNode::Terminal(rhs_t)) =
            (lhs.as_ref().as_ref(), rhs.as_ref().as_ref())
        {
            let ans = BddConnectionPair {
                entry_point: context.borrow_mut().add_bdd_node(Self::Terminal(0)),
                return_map: vec![(*lhs_t, *rhs_t)],
            };
            context
                .borrow_mut()
                .set_bdd_pair_product_cache(lhs, rhs, ans.clone());
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

        let zero_branch = Self::pair_product(zero_l, zero_r, context);
        let one_branch = Self::pair_product(one_l, one_r, context);

        let mut return_map = zero_branch.return_map.clone();
        use std::collections::HashMap;
        let mut inverse_lookup: HashMap<(usize, usize), usize> = HashMap::new();
        for (i, p) in return_map.iter().enumerate() {
            inverse_lookup.insert(*p, i);
        }

        let mut one_reduce_map = Vec::with_capacity(one_branch.return_map.len());
        for p in &one_branch.return_map {
            if let Some(&idx) = inverse_lookup.get(p) {
                one_reduce_map.push(idx);
            } else {
                let idx = return_map.len();
                inverse_lookup.insert(*p, idx);
                return_map.push(*p);
                one_reduce_map.push(idx);
            }
        }

        let zero_entry = zero_branch.entry_point;
        let one_entry = Self::reduce(&one_branch.entry_point, &one_reduce_map, context);

        let ans_entry = if zero_entry.hash_code() == one_entry.hash_code() {
            zero_entry
        } else {
            context
                .borrow_mut()
                .add_bdd_node(Self::Internal(BddInternalNode {
                    var_id,
                    zero_branch: zero_entry,
                    one_branch: one_entry,
                }))
        };

        let ans = BddConnectionPair {
            entry_point: ans_entry,
            return_map,
        };
        context
            .borrow_mut()
            .set_bdd_pair_product_cache(lhs, rhs, ans.clone());
        ans
    }

    pub fn reduce(
        this: &Rch<Self>,
        reduce_map: &[usize],
        context: &RefCell<Context<'_>>,
    ) -> Rch<Self> {
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
                let zero_branch = Self::reduce(zero_branch, reduce_map, context);
                let one_branch = Self::reduce(one_branch, reduce_map, context);
                if zero_branch.hash_code() == one_branch.hash_code() {
                    return zero_branch;
                }
                context
                    .borrow_mut()
                    .add_bdd_node(BddNode::Internal(BddInternalNode {
                        var_id: *var_id,
                        zero_branch,
                        one_branch,
                    }))
            }
        };
        context
            .borrow_mut()
            .set_bdd_reduction_cache(this, reduce_map, ans.clone());
        ans
    }
}
