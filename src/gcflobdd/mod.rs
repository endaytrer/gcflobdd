mod bdd;
#[cfg(feature = "complex")]
pub mod complex;
mod connection;
pub mod context;
mod node;
mod return_map;
#[cfg(test)]
mod tests;

use std::cell::RefCell;
use std::ops::Not;
use std::rc::Rc;

use crate::gcflobdd::connection::ConnectionPair;
use crate::gcflobdd::context::{BoolOperation, Context, IntOperation};
use crate::gcflobdd::node::GcflobddNode;
use crate::gcflobdd::return_map::{complement, inverse_lookup};
use crate::grammar::Grammar;
use connection::ConnectionT;
use return_map::ReturnMapT;

#[derive(Clone)]
pub struct GcflobddT<'grammar, T> {
    connection: ConnectionT<'grammar, ReturnMapT<T>>,
    grammar: &'grammar Grammar,
}
impl<'grammar, T: std::fmt::Debug> std::fmt::Debug for GcflobddT<'grammar, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("GcflobddT").field(&self.connection).finish()
    }
}

pub type Gcflobdd<'grammar> = GcflobddT<'grammar, bool>;

macro_rules! _define_op {
    ($name:ident, $pair_func:ident, $get_op_cache:ident, $set_op_cache:ident, $op_type: ident, $val_type:ident, $op_code:ident, $lhs:ident, $rhs:ident, $op:expr) => {
        pub fn $name(&self, rhs: &Self, context: &'grammar RefCell<Context<'grammar>>) -> Self {
            if let Some(ans) = context
                .borrow()
                .$get_op_cache::<{ $op_type::$op_code as usize }>(self.clone(), rhs.clone())
            {
                return ans;
            }
            let ans = self.$pair_func(
                rhs,
                |&$lhs: &$val_type, &$rhs: &$val_type| -> $val_type { $op },
                context,
            );
            context
                .borrow_mut()
                .$set_op_cache::<{ $op_type::$op_code as usize }>(
                    self.clone(),
                    rhs.clone(),
                    ans.clone(),
                );
            ans
        }
    };
}
macro_rules! define_bool_op {
    ($name:ident, $op_code:ident, $lhs:ident, $rhs:ident, $op:expr) => {
        #[cfg(not(feature = "separate_reduce_map"))]
        _define_op!(
            $name,
            mk_op_pair_map,
            get_op_cache,
            set_op_cache,
            BoolOperation,
            bool,
            $op_code,
            $lhs,
            $rhs,
            $op
        );
        #[cfg(feature = "separate_reduce_map")]
        _define_op!(
            $name,
            mk_op,
            get_op_cache,
            set_op_cache,
            BoolOperation,
            bool,
            $op_code,
            $lhs,
            $rhs,
            $op
        );
    };
}
macro_rules! define_int_op {
    ($name:ident, $op_code:ident, $lhs:ident, $rhs:ident, $op:expr) => {
        #[cfg(not(feature = "separate_reduce_map"))]
        _define_op!(
            $name,
            mk_op_pair_map,
            get_int_op_cache,
            set_int_op_cache,
            IntOperation,
            i32,
            $op_code,
            $lhs,
            $rhs,
            $op
        );
        #[cfg(feature = "separate_reduce_map")]
        _define_op!(
            $name,
            mk_op,
            get_int_op_cache,
            set_int_op_cache,
            IntOperation,
            i32,
            $op_code,
            $lhs,
            $rhs,
            $op
        );
    };
}

impl<'grammar> Gcflobdd<'grammar> {
    fn new(
        connection: ConnectionT<'grammar, ReturnMapT<bool>>,
        grammar: &'grammar Grammar,
    ) -> Self {
        Self {
            connection,
            grammar,
        }
    }

    pub fn mk_projection(
        i: usize,
        grammar: &'grammar Grammar,
        context: &RefCell<Context<'grammar>>,
    ) -> Self {
        let node = GcflobddNode::mk_distinction(i, &grammar.root, context);
        Self::new(
            ConnectionT {
                entry_point: node,
                return_map: vec![false, true],
            },
            grammar,
        )
    }
    pub fn mk_true(grammar: &'grammar Grammar, context: &RefCell<Context<'grammar>>) -> Self {
        let node = GcflobddNode::mk_no_distinction(&grammar.root, context);
        Self::new(
            ConnectionT {
                entry_point: node,
                return_map: vec![true],
            },
            grammar,
        )
    }
    pub fn mk_false(grammar: &'grammar Grammar, context: &RefCell<Context<'grammar>>) -> Self {
        let node = GcflobddNode::mk_no_distinction(&grammar.root, context);
        Self::new(
            ConnectionT {
                entry_point: node,
                return_map: vec![false],
            },
            grammar,
        )
    }
    pub fn mk_not(&self) -> Self {
        let mut connection = self.connection.clone();
        connection.return_map = complement(&connection.return_map);
        Self {
            connection,
            grammar: self.grammar,
        }
    }

    define_bool_op!(mk_and, And, a, b, a && b);
    define_bool_op!(mk_or, Or, a, b, a || b);
    define_bool_op!(mk_xor, Xor, a, b, a ^ b);
    define_bool_op!(mk_nand, Nand, a, b, !(a && b));
    define_bool_op!(mk_nor, Nor, a, b, !(a || b));
    define_bool_op!(mk_xnor, Xnor, a, b, !(a ^ b));
    define_bool_op!(mk_implies, Implies, a, b, !a || b);

    pub fn find_one_satisfiable_assignment(&self) -> Option<Vec<Option<bool>>> {
        self.find_one_path_to(&true)
    }
}

impl Not for Gcflobdd<'_> {
    type Output = Self;
    #[inline]
    fn not(self) -> Self {
        self.mk_not()
    }
}

pub type GcflobddInt<'grammar> = GcflobddT<'grammar, i32>;

impl<'grammar> GcflobddInt<'grammar> {
    pub fn mk_hadamard_voc12(
        level: usize,
        grammar: &'grammar Grammar,
        context: &RefCell<Context<'grammar>>,
    ) -> Self {
        Self {
            connection: ConnectionT {
                entry_point: GcflobddNode::mk_balanced_hadamard_voc12(
                    level,
                    &grammar.root,
                    context,
                ),
                return_map: vec![1, -1],
            },
            grammar,
        }
    }
    pub fn mk_hadamard_voc13(
        level: usize,
        grammar: &'grammar Grammar,
        context: &RefCell<Context<'grammar>>,
    ) -> Self {
        Self {
            connection: ConnectionT {
                entry_point: GcflobddNode::mk_balanced_hadamard_voc13(
                    level,
                    &grammar.root,
                    context,
                ),
                return_map: vec![1, -1],
            },
            grammar,
        }
    }

    define_int_op!(mk_add, Add, a, b, a + b);
    define_int_op!(mk_sub, Sub, a, b, a - b);
    define_int_op!(mk_mul, Mul, a, b, a * b);
}

impl<'grammar, T: Eq> GcflobddT<'grammar, T> {
    pub fn find_one_path_to(&self, value: &T) -> Option<Vec<Option<bool>>> {
        let index = inverse_lookup(&self.connection.return_map, value)?;
        Some(self.connection.entry_point.find_one_path_to(index))
    }
}
impl<'grammar, T: Copy> GcflobddT<'grammar, T> {
    pub fn pair_product(
        &self,
        rhs: &Self,
        context: &'grammar RefCell<Context<'grammar>>,
    ) -> GcflobddT<'grammar, (T, T)> {
        let ConnectionPair {
            entry_point,
            return_map,
        } = GcflobddNode::pair_product(
            &self.connection.entry_point,
            &rhs.connection.entry_point,
            context,
        );
        let mapped_return_map = return_map
            .into_iter()
            .map(|(i, j)| (self.connection.return_map[i], rhs.connection.return_map[j]))
            .collect();

        GcflobddT {
            connection: ConnectionT {
                entry_point,
                return_map: mapped_return_map,
            },
            grammar: self.grammar,
        }
    }
}

impl<'grammar, T> GcflobddT<'grammar, T> {
    pub fn map<V: Eq>(
        &self,
        f: impl Fn(&T) -> V,
        context: &RefCell<Context<'grammar>>,
    ) -> GcflobddT<'grammar, V> {
        let mut new_return_handle = vec![];
        let mapping_array = self
            .connection
            .return_map
            .iter()
            .map(|t| {
                let v = f(t);
                new_return_handle
                    .iter()
                    .position(|x| *x == v)
                    .unwrap_or_else(|| {
                        new_return_handle.push(v);
                        new_return_handle.len() - 1
                    })
            })
            .collect::<Vec<_>>();
        let num_exits = new_return_handle.len();
        let entry_point = GcflobddNode::reduce(
            &self.connection.entry_point,
            mapping_array.into(),
            num_exits,
            context,
        );

        GcflobddT {
            connection: ConnectionT {
                entry_point,
                return_map: new_return_handle,
            },
            grammar: self.grammar,
        }
    }
}
impl<'grammar, T: Copy + Eq> GcflobddT<'grammar, T> {
    pub fn mk_op(
        &self,
        rhs: &Self,
        op: impl Fn(&T, &T) -> T,
        context: &'grammar RefCell<Context<'grammar>>,
    ) -> Self {
        self.pair_product(rhs, context)
            .map(|(a, b)| op(a, b), context)
    }

    pub fn mk_op_pair_map(
        &self,
        rhs: &Self,
        op: impl Fn(&T, &T) -> T,
        context: &'grammar RefCell<Context<'grammar>>,
    ) -> Self {
        let mut new_return_handle = vec![];
        let lhs_num_exits = self.connection.entry_point.get_num_exits();
        let rhs_num_exits = rhs.connection.entry_point.get_num_exits();

        let mut reduce_map = vec![0; lhs_num_exits * rhs_num_exits];

        for j in 0..lhs_num_exits {
            for k in 0..rhs_num_exits {
                let a = &self.connection.return_map[j];
                let b = &rhs.connection.return_map[k];
                let v = op(a, b);

                let idx = new_return_handle
                    .iter()
                    .position(|x| *x == v)
                    .unwrap_or_else(|| {
                        new_return_handle.push(v);
                        new_return_handle.len() - 1
                    });

                reduce_map[k * lhs_num_exits + j] = idx;
            }
        }

        let num_exits = new_return_handle.len();
        let reduce_matrix = context.borrow_mut().add_reduce_matrix(reduce_map);
        let ConnectionT {
            entry_point,
            return_map,
        } = GcflobddNode::pair_map(
            &self.connection.entry_point,
            &rhs.connection.entry_point,
            &reduce_matrix,
            num_exits,
            context,
        );
        let mapped_return_map = return_map.iter().map(|i| new_return_handle[*i]).collect();

        GcflobddT {
            connection: ConnectionT {
                entry_point,
                return_map: mapped_return_map,
            },
            grammar: self.grammar,
        }
    }
}

impl<T: PartialEq> PartialEq for GcflobddT<'_, T> {
    fn eq(&self, other: &Self) -> bool {
        Rc::as_ptr(&self.connection.entry_point) == Rc::as_ptr(&other.connection.entry_point)
            && self.connection.return_map == other.connection.return_map
    }
}
impl<T: Eq> Eq for GcflobddT<'_, T> {}

impl<T: std::hash::Hash> std::hash::Hash for GcflobddT<'_, T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.connection.hash(state);
    }
}
