mod bdd;
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
use crate::gcflobdd::context::{
    Context, OP_AND, OP_IMPLIES, OP_NAND, OP_NOR, OP_OR, OP_XNOR, OP_XOR,
};
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

macro_rules! _define_op_new {
    ($name:ident, $op_code:ident, $op:expr) => {
        pub fn $name(&self, rhs: &Self, context: &RefCell<Context<'grammar>>) -> Self {
            if let Some(ans) = context
                .borrow()
                .get_op_cache::<$op_code>(self.clone(), rhs.clone())
            {
                return ans;
            }
            let ans = self.mk_op_pair_map(rhs, $op, context);
            context
                .borrow_mut()
                .set_op_cache::<$op_code>(self.clone(), rhs.clone(), ans.clone());
            ans
        }
    };
}
macro_rules! _define_op_legacy {
    ($name:ident, $op_code:ident, $op:expr) => {
        pub fn $name(&self, rhs: &Self, context: &RefCell<Context<'grammar>>) -> Self {
            if let Some(ans) = context
                .borrow()
                .get_op_cache::<$op_code>(self.clone(), rhs.clone())
            {
                return ans;
            }
            let ans = self.mk_op(rhs, $op, context);
            context
                .borrow_mut()
                .set_op_cache::<$op_code>(self.clone(), rhs.clone(), ans.clone());
            ans
        }
    };
}
macro_rules! define_op_comparison {
    ($name: ident, $name_new:ident, $name_legacy:ident, $op_code:ident, $op:expr) => {
        #[cfg(not(feature = "separate_reduce_map"))]
        _define_op_new!($name, $op_code, $op);
        #[cfg(feature = "separate_reduce_map")]
        _define_op_legacy!($name, $op_code, $op);
        // pub fn $name(&self, rhs: &Self, context: &RefCell<Context<'grammar>>) -> Self {
        //     let a = self.$name_new(rhs, context);
        //     let b = self.$name_legacy(rhs, context);
        //     println!("{:?}", a);
        //     println!("{:?}", b);
        //     println!("{}", $op_code);
        //     if a != b {
        //         panic!("$name_new and $name_legacy do not agree on $op_code");
        //     }
        //     b
        // }
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

    define_op_comparison!(mk_and, mk_and_new, mk_and_legacy, OP_AND, |a: &bool,
                                                                      b: &bool|
     -> bool {
        *a && *b
    });
    define_op_comparison!(mk_or, mk_or_new, mk_or_legacy, OP_OR, |a: &bool,
                                                                  b: &bool|
     -> bool {
        *a || *b
    });
    define_op_comparison!(mk_xor, mk_xor_new, mk_xor_legacy, OP_XOR, |a: &bool,
                                                                      b: &bool|
     -> bool {
        *a ^ *b
    });
    define_op_comparison!(mk_nand, mk_nand_new, mk_nand_legacy, OP_NAND, |a: &bool,
                                                                          b: &bool|
     -> bool {
        !(*a && *b)
    });
    define_op_comparison!(mk_nor, mk_nor_new, mk_nor_legacy, OP_NOR, |a: &bool,
                                                                      b: &bool|
     -> bool {
        !(*a || *b)
    });
    define_op_comparison!(mk_xnor, mk_xnor_new, mk_xnor_legacy, OP_XNOR, |a: &bool,
                                                                          b: &bool|
     -> bool {
        !(*a ^ *b)
    });
    define_op_comparison!(
        mk_implies,
        mk_implies_new,
        mk_implies_legacy,
        OP_IMPLIES,
        |a: &bool, b: &bool| -> bool { !(*a) || *b }
    );

    pub fn find_one_satisfiable_assignment(&self) -> Option<Vec<Option<bool>>> {
        self.find_one_path_to(&true)
    }
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
        context: &RefCell<Context<'grammar>>,
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
        context: &RefCell<Context<'grammar>>,
    ) -> Self {
        self.pair_product(rhs, context)
            .map(|(a, b)| op(a, b), context)
    }

    pub fn mk_op_pair_map(
        &self,
        rhs: &Self,
        op: impl Fn(&T, &T) -> T,
        context: &RefCell<Context<'grammar>>,
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

impl Not for Gcflobdd<'_> {
    type Output = Self;
    #[inline]
    fn not(self) -> Self {
        self.mk_not()
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
