//! Recursive matrix multiplication.
//!
//! # Matrices as diagrams
//!
//! A diagram over `2m` variables denotes a `2^m x 2^m` matrix in *interleaved*
//! order: variable `2k` is bit `k` of the row index and variable `2k+1` is bit
//! `k` of the column index, bit `0` most significant. Splitting the variables
//! in two therefore splits both indices in two: the first grouping of a node
//! picks the cell `(Rhi, Chi)` of a grid of blocks, and the second picks the
//! position `(Rlo, Clo)` inside the chosen block.
//!
//! The grammar must be *binary* -- every rule has two symbols on its right,
//! each covering an even number of variables, with `S -> a a` at the leaves.
//! The balanced grammar `S_k -> S_(k-1) S_(k-1)`, ..., `S_0 -> a a` (a
//! non-general CFLOBDD) is the canonical example.
//!
//! # How the product is computed
//!
//! Substituting the decomposition above into `P = M1 * M2` and splitting the
//! summation index the same way gives
//!
//! ```text
//!     Pblock(Rhi, Chi) = sum over Khi of  B1[A1(Rhi, Khi)] * B2[A2(Khi, Chi)]
//! ```
//!
//! which has the shape of a matrix product of the two A-diagrams, except that
//! "multiplying" two block ids yields the *pair* of ids and "adding" collects
//! pairs. Multiplying the two A-diagrams recursively in that deferred semiring
//! (`MatMulMap`) therefore produces, in a single recursive call, the plan of
//! which block pairs to multiply -- for every grid cell at once, grouped into
//! as many classes as there are distinct plans. That collapse, not the grid
//! size, is what the cost depends on.
//!
//! Real numbers only appear in [`GcflobddT::mk_matmul`], which substitutes them
//! into the finished symbolic diagram in one pass. So the node recursion and
//! its cache are keyed on structure alone and are shared by every value type.

mod map;
pub(in crate::gcflobdd) mod node;
#[cfg(test)]
mod tests;

use std::cell::RefCell;
use std::rc::Rc;

use crate::gcflobdd::GcflobddT;
use crate::gcflobdd::connection::{Connection, ConnectionT};
use crate::gcflobdd::context::Context;
use crate::gcflobdd::matmul::node::{matmul_node, split};
use crate::gcflobdd::node::{GcflobddNode, GcflobddNodeType, InternalNode};
use crate::grammar::{Grammar, GrammarNode, GrammarNodeType};
use crate::utils::hash_cache::Rch;

/// The values a matrix built out of [`GcflobddT`] can hold.
///
/// Only [`Clone`] and [`PartialEq`] are required of the type itself, so `f64`
/// and `rug::Complex` -- neither of which is [`Eq`] -- can be used.
///
/// `zero_like` takes a sample because a value type may carry context that a
/// constant cannot know (`rug::Complex` carries its precision).
pub trait MatMulValue: Clone + PartialEq {
    fn zero_like(sample: &Self) -> Self;
    fn add(&self, rhs: &Self) -> Self;
    fn mul(&self, rhs: &Self) -> Self;
    /// `coeff * self`, where `coeff` counts how many times a product occurs.
    fn scale(&self, coeff: i64) -> Self;
}

macro_rules! integer_matmul_value {
    ($t:ty) => {
        impl MatMulValue for $t {
            #[inline]
            fn zero_like(_sample: &Self) -> Self {
                0
            }
            #[inline]
            fn add(&self, rhs: &Self) -> Self {
                self + rhs
            }
            #[inline]
            fn mul(&self, rhs: &Self) -> Self {
                self * rhs
            }
            #[inline]
            fn scale(&self, coeff: i64) -> Self {
                <$t>::try_from(coeff).expect("matmul: coefficient does not fit the value type")
                    * self
            }
        }
    };
}

integer_matmul_value!(i32);
integer_matmul_value!(i64);

impl MatMulValue for f64 {
    #[inline]
    fn zero_like(_sample: &Self) -> Self {
        0.0
    }
    #[inline]
    fn add(&self, rhs: &Self) -> Self {
        self + rhs
    }
    #[inline]
    fn mul(&self, rhs: &Self) -> Self {
        self * rhs
    }
    #[inline]
    fn scale(&self, coeff: i64) -> Self {
        self * coeff as f64
    }
}

#[cfg(feature = "complex")]
impl MatMulValue for rug::Complex {
    fn zero_like(sample: &Self) -> Self {
        rug::Complex::with_val(sample.prec(), (0, 0))
    }
    fn add(&self, rhs: &Self) -> Self {
        rug::Complex::with_val(self.prec(), self + rhs)
    }
    fn mul(&self, rhs: &Self) -> Self {
        rug::Complex::with_val(self.prec(), self * rhs)
    }
    fn scale(&self, coeff: i64) -> Self {
        rug::Complex::with_val(self.prec(), self * coeff)
    }
}

/// Check that `grammar` describes a square matrix in interleaved order: binary
/// groupings all the way down, an even number of variables under each, and
/// `S -> a a` at the leaves.
fn check_matrix_grammar(grammar: &Rc<GrammarNode>) {
    match &grammar.node {
        GrammarNodeType::Internal(children) => {
            let [g1, g2] = &children[..] else {
                panic!(
                    "matmul requires binary groupings, found a rule with {} symbols on the right",
                    children.len()
                )
            };
            let terminals = matches!(g1.node, GrammarNodeType::Terminal) as usize
                + matches!(g2.node, GrammarNodeType::Terminal) as usize;
            if terminals == 2 {
                // `S -> a a`: the 2x2 base case.
                return;
            }
            assert_eq!(
                terminals, 0,
                "matmul requires a grouping to hold either two terminals or two non-terminals"
            );
            assert!(
                g1.num_vars % 2 == 0 && g2.num_vars % 2 == 0,
                "matmul requires each grouping to cover an even number of variables, \
                 found {} and {}",
                g1.num_vars,
                g2.num_vars
            );
            check_matrix_grammar(g1);
            check_matrix_grammar(g2);
        }
        GrammarNodeType::Terminal => panic!("matmul requires at least two variables"),
        GrammarNodeType::Bdd(_) => panic!("matmul does not support BDD groupings"),
    }
}

/// The node of the identity matrix over `grammar`: exit 0 is the diagonal, exit
/// 1 everything else.
fn identity_node<'grammar>(
    grammar: &'grammar Rc<GrammarNode>,
    context: &RefCell<Context<'grammar>>,
) -> Rch<GcflobddNode<'grammar>> {
    let (g1, g2) = split(grammar);
    let (a_connection, b_connections) = if matches!(g1.node, GrammarNodeType::Terminal) {
        // [[diag, off], [off, diag]]
        (
            Connection::new(
                GcflobddNode::mk_distinction(0, g1, context),
                vec![0, 1],
                context,
            ),
            vec![
                Connection::new(
                    GcflobddNode::mk_distinction(0, g2, context),
                    vec![0, 1],
                    context,
                ),
                Connection::new(
                    GcflobddNode::mk_distinction(0, g2, context),
                    vec![1, 0],
                    context,
                ),
            ],
        )
    } else {
        // Diagonal cells hold an identity block, every other cell is all-off.
        (
            Connection::new(identity_node(g1, context), vec![0, 1], context),
            vec![
                Connection::new(identity_node(g2, context), vec![0, 1], context),
                Connection::new(
                    GcflobddNode::mk_no_distinction(g2, context),
                    vec![1],
                    context,
                ),
            ],
        )
    };
    context.borrow_mut().add_gcflobdd_node(GcflobddNode {
        num_exits: 2,
        grammar,
        node: GcflobddNodeType::Internal(InternalNode {
            connections: vec![vec![a_connection], b_connections],
        }),
    })
}

impl<'grammar, T: Clone + PartialEq> GcflobddT<'grammar, T> {
    /// Build the matrix whose entry `(row, col)` is `rows[row][col]`.
    ///
    /// Cost is `O(N^2)` in the matrix dimension -- a constructor for small,
    /// dense matrices.
    pub fn from_matrix(
        rows: &[Vec<T>],
        grammar: &'grammar Grammar,
        context: &RefCell<Context<'grammar>>,
    ) -> Self {
        let bits = Self::dimension_bits(grammar);
        let n = 1usize << bits;
        assert_eq!(rows.len(), n, "expected a {n}x{n} matrix");
        let mut table = Vec::with_capacity(n * n);
        for index in 0..(n * n) {
            let (row, col) = deinterleave(index, bits);
            assert_eq!(rows[row].len(), n, "expected a {n}x{n} matrix");
            table.push(rows[row][col].clone());
        }
        Self::from_table(&table, grammar, context)
    }

    /// The identity matrix, with `one` on the diagonal and `zero` elsewhere.
    ///
    /// Built directly, so its size is logarithmic in the matrix dimension.
    pub fn mk_identity(
        one: T,
        zero: T,
        grammar: &'grammar Grammar,
        context: &RefCell<Context<'grammar>>,
    ) -> Self {
        check_matrix_grammar(&grammar.root);
        assert!(one != zero, "the identity needs two distinct values");
        Self {
            connection: ConnectionT {
                entry_point: identity_node(&grammar.root, context),
                return_map: vec![one, zero],
            },
            grammar,
        }
    }

    /// `log2` of the dimension of the matrix this grammar describes.
    fn dimension_bits(grammar: &Grammar) -> usize {
        assert_eq!(
            grammar.num_vars() % 2,
            0,
            "a matrix needs an even number of variables"
        );
        grammar.num_vars() / 2
    }
}

impl<'grammar, T: Clone> GcflobddT<'grammar, T> {
    /// The entry at `(row, col)`, in the interleaved order described above.
    pub fn entry(&self, row: usize, col: usize) -> T {
        let bits = self.grammar.num_vars() / 2;
        debug_assert!(
            bits >= usize::BITS as usize || (row < (1usize << bits) && col < (1usize << bits))
        );
        // A dimension can exceed what `usize` indexes, so bits above the width
        // of the index are simply zero.
        let bit = |value: usize, position: usize| {
            position < usize::BITS as usize && (value >> position) & 1 == 1
        };
        let mut assignment = Vec::with_capacity(2 * bits);
        for k in 0..bits {
            assignment.push(bit(row, bits - 1 - k));
            assignment.push(bit(col, bits - 1 - k));
        }
        self.evaluate(&assignment)
    }
}

impl<'grammar, T: MatMulValue> GcflobddT<'grammar, T> {
    /// The matrix product `self * rhs`.
    ///
    /// Both operands must be over the same grammar, which must describe a
    /// square matrix (see the module documentation); anything else panics.
    pub fn mk_matmul(&self, rhs: &Self, context: &RefCell<Context<'grammar>>) -> Self {
        assert!(
            Rc::ptr_eq(&self.grammar.root, &rhs.grammar.root),
            "matmul operands must share a grammar"
        );
        check_matrix_grammar(&self.grammar.root);

        // Which exit of each operand is known to be zero, if any. It only ever
        // prunes work: an operand whose whole subtree is that exit contributes
        // nothing and is never descended into.
        let zero = T::zero_like(&self.connection.return_map[0]);
        let z1 = self.connection.return_map.iter().position(|v| *v == zero);
        let z2 = rhs.connection.return_map.iter().position(|v| *v == zero);

        let product = matmul_node(
            &self.connection.entry_point,
            &rhs.connection.entry_point,
            z1,
            z2,
            context,
        );

        // Substitute the real values, once, into the symbolic result.
        let mut values: Vec<T> = Vec::with_capacity(product.return_map.len());
        let mut reduce_map = Vec::with_capacity(product.return_map.len());
        for combination in product.return_map.iter() {
            let mut value = T::zero_like(&self.connection.return_map[0]);
            for (i, j, coeff) in combination.iter() {
                value = value.add(
                    &self.connection.return_map[i]
                        .mul(&rhs.connection.return_map[j])
                        .scale(coeff),
                );
            }
            reduce_map.push(match values.iter().position(|v| *v == value) {
                Some(index) => index,
                None => {
                    values.push(value);
                    values.len() - 1
                }
            });
        }

        let num_exits = values.len();
        Self {
            connection: ConnectionT {
                entry_point: GcflobddNode::reduce(
                    &product.entry_point,
                    reduce_map.into(),
                    num_exits,
                    context,
                ),
                return_map: values,
            },
            grammar: self.grammar,
        }
    }
}

/// Split an interleaved table index back into its row and column.
fn deinterleave(index: usize, bits: usize) -> (usize, usize) {
    let mut row = 0;
    let mut col = 0;
    for k in 0..bits {
        let shift = 2 * (bits - 1 - k);
        row = (row << 1) | ((index >> (shift + 1)) & 1);
        col = (col << 1) | ((index >> shift) & 1);
    }
    (row, col)
}
