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
//!
//! # Vectors
//!
//! A vector of length `2^m` is a diagram over `m` variables, index big-endian,
//! so it lives over the matrix grammar's [halved](Grammar::halved) counterpart
//! -- the same tree with every `S -> a a` leaf collapsed to one terminal.
//!
//! [`GcflobddT::mk_matvec`] is its own recursion rather than a matrix product
//! against a padded operand, so the vector keeps half as many variables all the
//! way down. Dropping the column coordinate from the recurrence above leaves
//!
//! ```text
//!     yblock(Rhi) = sum over Chi of  B1[ A1(Rhi, Chi) ] * Bv[ Av(Chi) ]
//! ```
//!
//! which is again a matrix-vector product -- of the grid `A1` by the vector
//! `Av` -- so the same deferred semiring applies, and one recursive call still
//! produces the plan for every `Rhi` at once.
//!
//! # Kronecker product
//!
//! [`GcflobddT::mk_kron`] needs no recursion at all. Interleaving makes
//! `A (x) B` the *concatenation* of the two variable blocks: the result's
//! leading row and column bits are `A`'s, the trailing ones `B`'s, so its first
//! `2p` variables are exactly `A`'s own layout and the rest exactly `B`'s.
//!
//! ```text
//!     (A (x) B)[(rA,rB)][(cA,cB)] = A[rA][cA] * B[rB][cB]
//! ```
//!
//! The result is therefore one two-layer node over
//! [`Grammar::concat`](crate::grammar::Grammar::concat) that runs each operand
//! on its own block -- built in one connection per exit of `A`, with both
//! operands shared rather than copied, however large they are. The same holds
//! for the tensor product of two vectors.

pub mod coefficient;
mod map;
pub(in crate::gcflobdd) mod node;
#[cfg(test)]
mod tests;

use std::cell::RefCell;
use std::rc::Rc;

use crate::gcflobdd::GcflobddT;
use crate::gcflobdd::connection::{Connection, ConnectionPair, ConnectionT};
use crate::gcflobdd::context::Context;
pub use crate::gcflobdd::matmul::coefficient::Coefficient;
use crate::gcflobdd::matmul::node::{Valued, kron_node, matmul_node, matvec_node, split};
use crate::gcflobdd::node::{GcflobddNode, GcflobddNodeType, InternalNode};
use crate::grammar::{Grammar, GrammarNode, GrammarNodeType};
use crate::utils::hash_cache::Rch;
use crate::utils::{HashMap, HashSet, new_hash_map, new_hash_set};

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
    /// See [`Coefficient`] for how wide that count can get.
    fn scale(&self, coeff: &Coefficient) -> Self;

    /// A hashable stand-in for this value, if one exists.
    ///
    /// Every product collapses exits that carry equal values, and searching
    /// the value list linearly makes that quadratic -- which bites exactly
    /// when a diagram has many exits, as a Fourier-transformed state does.
    /// A key turns the search into a hash lookup.
    ///
    /// **Contract**: two values must have equal keys if and only if they are
    /// `==`. Returning `None` (the default) is always safe and falls back to
    /// the linear scan; a *wrong* key silently merges distinct values.
    fn dedup_key(&self) -> Option<u128> {
        None
    }
}

/// The bit pattern of an `f64`, normalised so that it agrees with `==`.
///
/// `-0.0 == 0.0` but their bit patterns differ, and no NaN is `==` anything,
/// so both have to be kept away from the hashed path.
#[inline]
fn float_key(value: f64) -> Option<u128> {
    if value.is_nan() {
        None
    } else if value == 0.0 {
        Some(0)
    } else {
        Some(value.to_bits() as u128)
    }
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
            fn scale(&self, coeff: &Coefficient) -> Self {
                let coeff = coeff
                    .to_i128()
                    .and_then(|c| <$t>::try_from(c).ok())
                    .expect("matmul: coefficient does not fit the value type");
                coeff * self
            }
            #[inline]
            fn dedup_key(&self) -> Option<u128> {
                Some(*self as i128 as u128)
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
    fn scale(&self, coeff: &Coefficient) -> Self {
        self * coeff.to_f64()
    }
    #[inline]
    fn dedup_key(&self) -> Option<u128> {
        float_key(*self)
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
    fn scale(&self, coeff: &Coefficient) -> Self {
        rug::Complex::with_val(self.prec(), self * coeff.to_rug())
    }
}

/// The two groupings of a matrix rule, checked. Panics unless the rule is
/// binary; reports whether its two symbols are terminals (`S -> a a`, the 2x2
/// base case).
fn matrix_rule(grammar: &Rc<GrammarNode>) -> (&Rc<GrammarNode>, &Rc<GrammarNode>, bool) {
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
            assert!(
                terminals == 0 || terminals == 2,
                "matmul requires a grouping to hold either two terminals or two non-terminals"
            );
            (g1, g2, terminals == 2)
        }
        GrammarNodeType::Terminal => panic!("matmul requires at least two variables"),
        GrammarNodeType::Bdd(_) => panic!("matmul does not support BDD groupings"),
    }
}

/// Check that `grammar` describes a square matrix in interleaved order: binary
/// groupings all the way down, an even number of variables under each, and
/// `S -> a a` at the leaves.
///
/// Grammars are DAGs -- a balanced one names the same symbol twice -- so
/// visited nodes are recorded; walking them as trees would cost `2^depth`.
fn check_matrix_grammar(grammar: &Rc<GrammarNode>) {
    fn walk(grammar: &Rc<GrammarNode>, seen: &mut HashSet<usize>) {
        if !seen.insert(Rc::as_ptr(grammar) as usize) {
            return;
        }
        let (g1, g2, leaves) = matrix_rule(grammar);
        if leaves {
            return;
        }
        assert!(
            g1.num_vars % 2 == 0 && g2.num_vars % 2 == 0,
            "matmul requires each grouping to cover an even number of variables, \
             found {} and {}",
            g1.num_vars,
            g2.num_vars
        );
        walk(g1, seen);
        walk(g2, seen);
    }
    walk(grammar, &mut new_hash_set());
}

/// Check that `vector` is the [halved](Grammar::halved) counterpart of the
/// matrix grammar `matrix`: the same tree, with each `S -> a a` leaf collapsed
/// to one terminal, so it addresses exactly the row half of the variables.
fn check_matvec_grammars(matrix: &Rc<GrammarNode>, vector: &Rc<GrammarNode>) {
    fn walk(
        matrix: &Rc<GrammarNode>,
        vector: &Rc<GrammarNode>,
        seen: &mut HashSet<(usize, usize)>,
    ) {
        if !seen.insert((Rc::as_ptr(matrix) as usize, Rc::as_ptr(vector) as usize)) {
            return;
        }
        let (g1, g2, leaves) = matrix_rule(matrix);
        if leaves {
            assert!(
                matches!(vector.node, GrammarNodeType::Terminal),
                "matvec: a `S -> a a` matrix grouping must face a single terminal, \
                 found one covering {} variables",
                vector.num_vars
            );
            return;
        }
        let GrammarNodeType::Internal(children) = &vector.node else {
            panic!("matvec: the vector grammar must mirror the matrix grammar's groupings")
        };
        let [h1, h2] = &children[..] else {
            panic!("matvec: the vector grammar must mirror the matrix grammar's groupings")
        };
        assert!(
            g1.num_vars == 2 * h1.num_vars && g2.num_vars == 2 * h2.num_vars,
            "matvec: each vector grouping must cover half its matrix grouping, \
             found {}/{} against {}/{}",
            h1.num_vars,
            h2.num_vars,
            g1.num_vars,
            g2.num_vars
        );
        walk(g1, h1, seen);
        walk(g2, h2, seen);
    }
    walk(matrix, vector, &mut new_hash_set());
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

    /// Build the vector whose component `i` is `entries[i]`.
    ///
    /// `grammar` is a vector grammar -- typically
    /// [`matrix_grammar.halved()`](Grammar::halved) -- so it covers `log2` of
    /// the length in variables, index big-endian. Cost is `O(N)`.
    pub fn from_vector(
        entries: &[T],
        grammar: &'grammar Grammar,
        context: &RefCell<Context<'grammar>>,
    ) -> Self {
        assert_eq!(
            entries.len(),
            1usize << grammar.num_vars(),
            "expected a vector of length {}",
            1usize << grammar.num_vars()
        );
        // A vector's table is its components: variable 0 is the leading index
        // bit, which is exactly `from_table`'s convention.
        Self::from_table(entries, grammar, context)
    }

    /// The basis vector `e_index`: `one` at `index`, `zero` everywhere else.
    ///
    /// Built directly, so its size is logarithmic in the length.
    pub fn mk_basis_vector(
        index: usize,
        one: T,
        zero: T,
        grammar: &'grammar Grammar,
        context: &RefCell<Context<'grammar>>,
    ) -> Self {
        assert!(one != zero, "a basis vector needs two distinct values");
        let bits = grammar.num_vars();
        assert!(
            bits >= usize::BITS as usize || index < (1usize << bits),
            "index {index} is outside the vector"
        );
        let (entry_point, match_exit) = basis_node(&grammar.root, index, context);
        Self {
            connection: ConnectionT {
                entry_point,
                return_map: if match_exit == 0 {
                    vec![one, zero]
                } else {
                    vec![zero, one]
                },
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
        let mut assignment = Vec::with_capacity(2 * bits);
        for k in 0..bits {
            assignment.push(bit(row, bits - 1 - k));
            assignment.push(bit(col, bits - 1 - k));
        }
        self.evaluate(&assignment)
    }

    /// The component at `index`, for a diagram over a vector grammar.
    pub fn component(&self, index: usize) -> T {
        let bits = self.grammar.num_vars();
        debug_assert!(bits >= usize::BITS as usize || index < (1usize << bits));
        let assignment = (0..bits)
            .map(|k| bit(index, bits - 1 - k))
            .collect::<Vec<_>>();
        self.evaluate(&assignment)
    }
}

/// Substitute real values into a finished symbolic diagram: each exit's
/// combination becomes `sum coeff * lhs[i] * rhs[j]`, exits that end up equal
/// collapse, and the result is a diagram over `grammar`.
///
/// This is the only place a product touches a number, and it runs once per
/// call for both [`GcflobddT::mk_matmul`] and [`GcflobddT::mk_matvec`].
fn substitute<'grammar, T: MatMulValue>(
    product: Valued<'grammar>,
    lhs: &[T],
    rhs: &[T],
    grammar: &'grammar Grammar,
    context: &RefCell<Context<'grammar>>,
) -> GcflobddT<'grammar, T> {
    let values = product.return_map.iter().map(|combination| {
        let mut value = T::zero_like(&lhs[0]);
        for (i, j, coeff) in combination.iter() {
            value = value.add(&lhs[i].mul(&rhs[j]).scale(coeff));
        }
        value
    });
    collapse(&product.entry_point, values, grammar, context)
}

/// Interns exit values, giving each distinct one an index.
///
/// Scans while the list is short and switches to hashing once it is not: the
/// scan wins for the handful of values a boolean or structured operand
/// produces, and the hash saves the quadratic blow-up on a diagram with
/// thousands of exits, such as a Fourier-transformed state. Values whose type
/// cannot produce a [`MatMulValue::dedup_key`] simply stay on the scan.
struct ValueSet<T> {
    values: Vec<T>,
    keys: HashMap<u128, usize>,
    hashed: bool,
    unkeyable: bool,
}

/// Where hashing starts to pay for itself, measured on the dense matrix
/// multiply (few exits) against the QFT (many).
const HASH_THRESHOLD: usize = 16;

impl<T> Default for ValueSet<T> {
    fn default() -> Self {
        Self {
            values: Vec::new(),
            keys: new_hash_map(),
            hashed: false,
            unkeyable: false,
        }
    }
}

impl<T: MatMulValue> ValueSet<T> {
    fn intern(&mut self, value: T) -> usize {
        if !self.hashed && !self.unkeyable && self.values.len() >= HASH_THRESHOLD {
            match self
                .values
                .iter()
                .map(T::dedup_key)
                .collect::<Option<Vec<_>>>()
            {
                Some(keys) => {
                    self.keys = keys.into_iter().zip(0..).collect();
                    self.hashed = true;
                }
                // One value without a key means the map could never answer
                // correctly; stop trying.
                None => self.unkeyable = true,
            }
        }
        if self.hashed {
            if let Some(key) = value.dedup_key() {
                let next = self.values.len();
                let values = &mut self.values;
                return *self.keys.entry(key).or_insert_with(|| {
                    values.push(value);
                    next
                });
            }
            // Mixed keyable and not: fall back for good, which stays correct
            // because both paths search the same list.
            self.hashed = false;
            self.unkeyable = true;
        }
        match self.values.iter().position(|v| *v == value) {
            Some(index) => index,
            None => {
                self.values.push(value);
                self.values.len() - 1
            }
        }
    }
}

/// Attach one freshly computed value to each exit of `entry_point`, collapsing
/// the exits that ended up equal.
///
/// This is [`GcflobddT::map`] with `PartialEq` in place of `Eq`, which is what
/// lets `f64` and `rug::Complex` through.
fn collapse<'grammar, T: MatMulValue>(
    entry_point: &Rch<GcflobddNode<'grammar>>,
    exit_values: impl Iterator<Item = T>,
    grammar: &'grammar Grammar,
    context: &RefCell<Context<'grammar>>,
) -> GcflobddT<'grammar, T> {
    let mut interner = ValueSet::default();
    let reduce_map: Vec<usize> = exit_values.map(|value| interner.intern(value)).collect();
    let values = interner.values;

    let num_exits = values.len();
    GcflobddT {
        connection: ConnectionT {
            entry_point: GcflobddNode::reduce(entry_point, reduce_map.into(), num_exits, context),
            return_map: values,
        },
        grammar,
    }
}

impl<'grammar, T: MatMulValue> GcflobddT<'grammar, T> {
    /// Which exit is known to hold zero, if any. It only ever prunes work: an
    /// operand whose whole subtree is that exit contributes nothing and is
    /// never descended into.
    fn zero_exit(&self, zero: &T) -> Option<usize> {
        self.connection.return_map.iter().position(|v| v == zero)
    }

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

        let zero = T::zero_like(&self.connection.return_map[0]);
        let product = matmul_node(
            &self.connection.entry_point,
            &rhs.connection.entry_point,
            self.zero_exit(&zero),
            rhs.zero_exit(&zero),
            context,
        );
        substitute(
            product,
            &self.connection.return_map,
            &rhs.connection.return_map,
            self.grammar,
            context,
        )
    }

    /// The entrywise sum `self + rhs`, over the grammar both share.
    ///
    /// Needed to build anything that is not a product operator -- a controlled
    /// gate is `|0><0| (x) I + |1><1| (x) U`, two Kronecker towers added.
    /// [`mk_op_pair_map`](Self::mk_op_pair_map) does the same job but demands
    /// `Copy + Eq`, which rules out `f64` and `rug::Complex`; the name differs
    /// from `GcflobddInt`'s cached `mk_add` for the same reason.
    pub fn mk_matadd(&self, rhs: &Self, context: &RefCell<Context<'grammar>>) -> Self {
        assert!(
            Rc::ptr_eq(&self.grammar.root, &rhs.grammar.root),
            "addition operands must share a grammar"
        );
        let ConnectionPair {
            entry_point,
            return_map,
        } = GcflobddNode::pair_product(
            &self.connection.entry_point,
            &rhs.connection.entry_point,
            context,
        );
        let values = return_map
            .iter()
            .map(|(i, j)| self.connection.return_map[*i].add(&rhs.connection.return_map[*j]));
        collapse(&entry_point, values, self.grammar, context)
    }

    /// `scalar * self`, which touches only the value map unless the scaling
    /// makes two exits coincide.
    pub fn mk_scale(&self, scalar: &T, context: &RefCell<Context<'grammar>>) -> Self {
        let values = self.connection.return_map.iter().map(|v| v.mul(scalar));
        collapse(&self.connection.entry_point, values, self.grammar, context)
    }

    /// The Kronecker product `self (x) rhs`, over `grammar` -- which must be
    /// [`self.grammar.concat(rhs.grammar)`](Grammar::concat).
    ///
    /// In the interleaved order this is concatenation of the two variable
    /// blocks, so it costs one connection per exit of `self` however large the
    /// operands are, and the operands' diagrams are shared rather than copied.
    ///
    /// The same construction is the tensor product of two vectors: `self` runs
    /// on the leading index bits and `rhs` on the trailing ones either way.
    /// Concatenating two matrix grammars gives a matrix grammar, so results
    /// feed straight back into [`mk_matmul`](Self::mk_matmul) and
    /// [`mk_matvec`](Self::mk_matvec).
    pub fn mk_kron(
        &self,
        rhs: &Self,
        grammar: &'grammar Grammar,
        context: &RefCell<Context<'grammar>>,
    ) -> Self {
        let GrammarNodeType::Internal(children) = &grammar.root.node else {
            panic!("kron needs the concatenation of the two operands' grammars")
        };
        assert!(
            matches!(&children[..], [g1, g2]
                if Rc::ptr_eq(g1, &self.grammar.root) && Rc::ptr_eq(g2, &rhs.grammar.root)),
            "kron needs the concatenation of the two operands' grammars, in that order"
        );

        let product = kron_node(
            &self.connection.entry_point,
            &rhs.connection.entry_point,
            &grammar.root,
            context,
        );
        substitute(
            product,
            &self.connection.return_map,
            &rhs.connection.return_map,
            grammar,
            context,
        )
    }

    /// The matrix-vector product `self * vector`.
    ///
    /// `vector` must be over the [halved](Grammar::halved) counterpart of this
    /// matrix's grammar, and the result is a vector over that same grammar.
    /// This is a recursion of its own rather than a matrix product with a
    /// padded operand: the vector keeps half as many variables throughout.
    pub fn mk_matvec(&self, vector: &Self, context: &RefCell<Context<'grammar>>) -> Self {
        check_matvec_grammars(&self.grammar.root, &vector.grammar.root);

        let zero = T::zero_like(&self.connection.return_map[0]);
        let product = matvec_node(
            &self.connection.entry_point,
            &vector.connection.entry_point,
            self.zero_exit(&zero),
            vector.zero_exit(&zero),
            context,
        );
        substitute(
            product,
            &self.connection.return_map,
            &vector.connection.return_map,
            vector.grammar,
            context,
        )
    }
}

/// Bit `position` of `value`, counted from the least significant.
///
/// A dimension can exceed what a `usize` indexes, so positions past its width
/// read as zero rather than overflowing the shift.
#[inline]
fn bit(value: usize, position: usize) -> bool {
    position < usize::BITS as usize && (value >> position) & 1 == 1
}

/// The leading and trailing parts of an index whose last `low_bits` bits belong
/// to the second grouping.
#[inline]
fn split_index(index: usize, low_bits: usize) -> (usize, usize) {
    if low_bits >= usize::BITS as usize {
        (0, index)
    } else {
        (index >> low_bits, index & ((1usize << low_bits) - 1))
    }
}

/// The node of the basis vector `e_index` over `grammar`, and the exit that
/// `index` itself reaches; the other exit is every other index.
///
/// Which of the two is the match is forced by canonical numbering rather than
/// chosen: exit 0 is whatever the *first* assignment reaches, so it is the
/// match only when the leading index bit is 0.
fn basis_node<'grammar>(
    grammar: &'grammar Rc<GrammarNode>,
    index: usize,
    context: &RefCell<Context<'grammar>>,
) -> (Rch<GcflobddNode<'grammar>>, usize) {
    let children = match &grammar.node {
        GrammarNodeType::Terminal => {
            // One variable: a fork, whose exits are the two index values.
            return (GcflobddNode::mk_distinction(0, grammar, context), index & 1);
        }
        GrammarNodeType::Internal(children) if children.len() == 2 => children,
        _ => panic!("a basis vector needs a binary vector grammar"),
    };
    let (h1, h2) = (&children[0], &children[1]);
    let (high, low) = split_index(index, h2.num_vars);
    let (a_node, a_match) = basis_node(h1, high, context);
    let (b_node, b_match) = basis_node(h2, low, context);

    let (b_connections, match_exit) = if a_match == 0 {
        // The matching half is reached first, so the low node's exits are
        // registered first and its numbering carries over unchanged.
        (
            vec![
                Connection::new(b_node, vec![0, 1], context),
                Connection::new(
                    GcflobddNode::mk_no_distinction(h2, context),
                    vec![1 - b_match],
                    context,
                ),
            ],
            b_match,
        )
    } else {
        // Everything under the first exit mismatches, so exit 0 is the
        // mismatch and the match becomes exit 1.
        (
            vec![
                Connection::new(
                    GcflobddNode::mk_no_distinction(h2, context),
                    vec![0],
                    context,
                ),
                Connection::new(
                    b_node,
                    if b_match == 0 { vec![1, 0] } else { vec![0, 1] },
                    context,
                ),
            ],
            1,
        )
    };
    // Built before the node is interned: `add_gcflobdd_node` holds the context
    // borrow, and `Connection::new` needs it too.
    let a_connection = Connection::new(a_node, vec![0, 1], context);
    let node = context.borrow_mut().add_gcflobdd_node(GcflobddNode {
        num_exits: 2,
        grammar,
        node: GcflobddNodeType::Internal(InternalNode {
            connections: vec![vec![a_connection], b_connections],
        }),
    });
    (node, match_exit)
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
