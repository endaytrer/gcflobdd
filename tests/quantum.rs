//! Quantum-algorithm benchmarks, shaped to be driven by the same harness as
//! the reference C++ CFLOBDD (`trishullab/cflobdd`).
//!
//! The CLI and the summary line mirror that implementation's
//! `./cflobdd <test> <p> [seed]`, so `run_quantum.sh` can drive either binary
//! and produce comparable CSV. Qubit count is `n = 2^p`, as there.
//!
//! Where the two implementations differ, they differ deliberately and the
//! difference is called out in BENCHMARKS.md: the circuits here are the
//! textbook ones, and a state vector stays a vector (`mk_matvec`) rather than
//! being padded into a matrix.

use gcflobdd::gcflobdd::GcflobddT;
use gcflobdd::gcflobdd::context::Context;
use gcflobdd::gcflobdd::matmul::{Coefficient, MatMulValue};
use gcflobdd::grammar::Grammar;
use std::cell::RefCell;
use std::collections::HashMap;
use std::time::Instant;

// ---------------------------------------------------------------------------
// Amplitudes
// ---------------------------------------------------------------------------

/// What the algorithms need of an amplitude type beyond [`MatMulValue`].
///
/// `zero`/`one` are functions rather than constants, and the type need not be
/// `Copy`, so that an arbitrary-precision amplitude qualifies.
trait Amplitude: MatMulValue + std::fmt::Debug {
    fn zero() -> Self;
    fn one() -> Self;
    fn of(x: f64) -> Self;
    /// `2^exponent`, separate from [`of`](Self::of) because these algorithms'
    /// natural constants -- `2^n`, `2/2^n`, `1/sqrt(2^n)` -- run well past what
    /// an `f64` literal can carry. The exponent is always an integer: the
    /// algorithms that would need `2^(-n/2)` require an even `n`.
    fn power_of_two(exponent: i32) -> Self;
    /// `e^(i * theta)`.
    fn phase(theta: f64) -> Self;
    fn magnitude(&self) -> f64;
}

impl Amplitude for f64 {
    fn zero() -> Self {
        0.0
    }
    fn one() -> Self {
        1.0
    }
    fn of(x: f64) -> Self {
        x
    }
    fn power_of_two(exponent: i32) -> Self {
        2f64.powi(exponent)
    }
    fn phase(theta: f64) -> Self {
        let value = theta.cos();
        assert!(
            theta.sin().abs() < 1e-12,
            "a real amplitude cannot hold the phase e^(i{theta})"
        );
        value
    }
    fn magnitude(&self) -> f64 {
        self.abs()
    }
}

/// Double-precision complex. The reference implementation uses 100-digit
/// `boost::multiprecision` complex here; see BENCHMARKS.md.
#[derive(Clone, Copy, Debug, PartialEq)]
struct C64 {
    re: f64,
    im: f64,
}

impl C64 {
    const fn new(re: f64, im: f64) -> Self {
        Self { re, im }
    }
}

impl MatMulValue for C64 {
    fn zero_like(_sample: &Self) -> Self {
        Self::new(0.0, 0.0)
    }
    fn add(&self, rhs: &Self) -> Self {
        Self::new(self.re + rhs.re, self.im + rhs.im)
    }
    fn mul(&self, rhs: &Self) -> Self {
        Self::new(
            self.re * rhs.re - self.im * rhs.im,
            self.re * rhs.im + self.im * rhs.re,
        )
    }
    fn scale(&self, coeff: &Coefficient) -> Self {
        let coeff = coeff.to_f64();
        Self::new(self.re * coeff, self.im * coeff)
    }
    fn dedup_key(&self) -> Option<u128> {
        // Both halves' bit patterns, with -0.0 and NaN handled as for f64.
        let part = |x: f64| -> Option<u64> {
            if x.is_nan() {
                None
            } else if x == 0.0 {
                Some(0)
            } else {
                Some(x.to_bits())
            }
        };
        Some(((part(self.re)? as u128) << 64) | part(self.im)? as u128)
    }
}

impl Amplitude for C64 {
    fn zero() -> Self {
        Self::new(0.0, 0.0)
    }
    fn one() -> Self {
        Self::new(1.0, 0.0)
    }
    fn of(x: f64) -> Self {
        Self::new(x, 0.0)
    }
    fn power_of_two(exponent: i32) -> Self {
        Self::new(2f64.powi(exponent), 0.0)
    }
    fn phase(theta: f64) -> Self {
        Self::new(theta.cos(), theta.sin())
    }
    fn magnitude(&self) -> f64 {
        self.re.hypot(self.im)
    }
}

/// An exact integer amplitude, of unbounded size.
///
/// With unnormalised Walsh gates every amplitude in Bernstein-Vazirani and
/// Deutsch-Jozsa is an integer, running up to `2^n` for `n` qubits -- past
/// `f64`'s range at about 1024 qubits, which is why the reference
/// implementation carries 100-digit floats. Holding them as integers instead
/// is both exact and unbounded: the answer at 65536 qubits is `2^65536` on the
/// nose, and the `2^(-(2n+1)/2)` normalisation is left symbolic.
#[derive(Clone, Debug, PartialEq)]
struct Int(rug::Integer);

impl Int {
    fn of_i64(value: i64) -> Self {
        Self(rug::Integer::from(value))
    }
}

impl MatMulValue for Int {
    fn zero_like(_sample: &Self) -> Self {
        Self::of_i64(0)
    }
    fn add(&self, rhs: &Self) -> Self {
        Self(rug::Integer::from(&self.0 + &rhs.0))
    }
    fn mul(&self, rhs: &Self) -> Self {
        Self(rug::Integer::from(&self.0 * &rhs.0))
    }
    fn scale(&self, coeff: &Coefficient) -> Self {
        Self(&self.0 * coeff.to_rug())
    }
    // No `dedup_key`: an arbitrary integer does not fit one, and these states
    // hold only a handful of distinct amplitudes, so the scan is fine.
}

impl Amplitude for Int {
    fn zero() -> Self {
        Self::of_i64(0)
    }
    fn one() -> Self {
        Self::of_i64(1)
    }
    fn of(x: f64) -> Self {
        debug_assert_eq!(x, x.trunc(), "an integer amplitude cannot hold {x}");
        Self::of_i64(x as i64)
    }
    fn power_of_two(exponent: i32) -> Self {
        let exponent =
            u32::try_from(exponent).expect("an integer amplitude cannot hold a negative power");
        Self(rug::Integer::from(1) << exponent)
    }
    fn phase(theta: f64) -> Self {
        let value = theta.cos();
        assert!(
            theta.sin().abs() < 1e-12 && value.abs() == 1.0,
            "an integer amplitude cannot hold the phase e^(i{theta})"
        );
        Self::of(value)
    }
    fn magnitude(&self) -> f64 {
        self.0.to_f64().abs()
    }
}

/// A real amplitude of arbitrary precision, at [`working_precision`] bits.
///
/// Only Grover needs this, and only when raising its operator to a power: that
/// operator rotates by `2 asin(2^(-n/2))` per iteration, so representing the
/// first squarings at all takes about `n/2` bits of mantissa. Past 108 qubits
/// `f64` rounds them to the identity and the search silently does nothing --
/// see BENCHMARKS.md. The reference implementation reaches for
/// `cpp_dec_float_100` throughout for the same reason.
#[derive(Clone, Debug, PartialEq)]
struct Real(rug::Float);

static PRECISION: std::sync::atomic::AtomicU32 =
    std::sync::atomic::AtomicU32::new(f64::MANTISSA_DIGITS);

fn working_precision() -> u32 {
    PRECISION.load(std::sync::atomic::Ordering::Relaxed)
}

fn set_working_precision(bits: u32) {
    PRECISION.store(bits, std::sync::atomic::Ordering::Relaxed);
}

impl Real {
    fn of_f64(value: f64) -> Self {
        Self(rug::Float::with_val(working_precision(), value))
    }
    /// The precision to carry a result of combining these two, which is the
    /// wider of the pair -- exactly as MPFR's own binary operators choose.
    fn joint_precision(&self, rhs: &Self) -> u32 {
        self.0.prec().max(rhs.0.prec())
    }
}

impl MatMulValue for Real {
    fn zero_like(sample: &Self) -> Self {
        Self(rug::Float::with_val(sample.0.prec(), 0))
    }
    fn add(&self, rhs: &Self) -> Self {
        Self(rug::Float::with_val(
            self.joint_precision(rhs),
            &self.0 + &rhs.0,
        ))
    }
    fn mul(&self, rhs: &Self) -> Self {
        Self(rug::Float::with_val(
            self.joint_precision(rhs),
            &self.0 * &rhs.0,
        ))
    }
    fn scale(&self, coeff: &Coefficient) -> Self {
        Self(rug::Float::with_val(
            self.0.prec(),
            &self.0 * coeff.to_rug(),
        ))
    }
    // No `dedup_key`: a wide float does not fit one, and these states hold only
    // a handful of distinct amplitudes, so the scan is fine.
}

impl Amplitude for Real {
    fn zero() -> Self {
        Self::of_f64(0.0)
    }
    fn one() -> Self {
        Self::of_f64(1.0)
    }
    fn of(x: f64) -> Self {
        Self::of_f64(x)
    }
    fn power_of_two(exponent: i32) -> Self {
        Self(rug::Float::with_val(working_precision(), 1) << exponent)
    }
    fn phase(theta: f64) -> Self {
        let value = theta.cos();
        assert!(
            theta.sin().abs() < 1e-12,
            "a real amplitude cannot hold the phase e^(i{theta})"
        );
        Self::of_f64(value)
    }
    fn magnitude(&self) -> f64 {
        self.0.clone().abs().to_f64()
    }
}

// ---------------------------------------------------------------------------
// Registers
// ---------------------------------------------------------------------------

/// The aligned-balanced grammar tree for a register, keyed by qubit count.
///
/// `matrix(c)` describes an operator on `c` qubits (`2c` variables) and
/// `vector(c)` a state of `c` qubits. A count divides into its ceiling half and
/// its floor half, down to one qubit's `S -> a a` -- the shape
/// `tests/n_queens.rs` calls aligned-balanced. A power of two divides evenly at
/// every level, so it reproduces the balanced family exactly; any other count
/// divides unevenly, which none of the matrix algebra minds. The deferred
/// semiring never looks at where a grouping splits its variables, only that the
/// split falls between two (row, column) pairs, and building the tree over
/// *qubits* guarantees that whatever the count.
///
/// Equal counts share one `Rc`, so a parent really is the concatenation of its
/// two children and [`GcflobddT::mk_kron`] accepts it.
struct Register {
    matrix: HashMap<usize, Grammar>,
    vector: HashMap<usize, Grammar>,
}

impl Register {
    fn new(qubits: usize) -> Self {
        assert!(qubits >= 1, "a register needs at least one qubit");
        let mut register = Self {
            matrix: HashMap::default(),
            vector: HashMap::default(),
        };
        register.build(qubits);
        // The one thing the matrix algebra asks of a grammar: every grouping
        // covers a whole number of (row, column) pairs. Building the tree over
        // qubits gives it for free at any count -- one qubit is two variables,
        // so k qubits are 2k -- but it is the precondition, so check it rather
        // than trust the construction.
        for (block, grammar) in &register.matrix {
            assert_eq!(
                grammar.num_vars(),
                2 * block,
                "a {block}-qubit grouping must cover {} variables",
                2 * block
            );
            assert_eq!(grammar.num_vars() % 2, 0, "grouping with odd variables");
        }
        register
    }

    fn build(&mut self, qubits: usize) -> (Grammar, Grammar) {
        if let (Some(matrix), Some(vector)) = (self.matrix.get(&qubits), self.vector.get(&qubits)) {
            return (matrix.clone(), vector.clone());
        }
        let (matrix, vector) = if qubits == 1 {
            let matrix = Grammar::new(&["S0 -> a a".to_string()]).unwrap();
            let vector = matrix.halved();
            (matrix, vector)
        } else {
            let high = qubits.div_ceil(2);
            let (matrix_high, vector_high) = self.build(high);
            let (matrix_low, vector_low) = self.build(qubits - high);
            (
                matrix_high.concat(&matrix_low),
                vector_high.concat(&vector_low),
            )
        };
        self.matrix.insert(qubits, matrix.clone());
        self.vector.insert(qubits, vector.clone());
        (matrix, vector)
    }

    fn matrix(&self, qubits: usize) -> &Grammar {
        self.matrix
            .get(&qubits)
            .unwrap_or_else(|| panic!("no {qubits}-qubit block in this register's tree"))
    }

    fn vector(&self, qubits: usize) -> &Grammar {
        self.vector
            .get(&qubits)
            .unwrap_or_else(|| panic!("no {qubits}-qubit block in this register's tree"))
    }

    /// Every block size in the tree, smallest first. There are `O(log n)` of
    /// them: each level of an aligned-balanced tree holds at most two adjacent
    /// sizes.
    fn block_sizes(&self) -> Vec<usize> {
        let mut sizes: Vec<usize> = self.matrix.keys().copied().collect();
        sizes.sort_unstable();
        sizes
    }

    /// The splits, largest first, as `parent=high+low`. Two different sizes at
    /// one level means the tree is genuinely uneven there, which is the whole
    /// point of a count that is not a power of two.
    fn shape(&self) -> String {
        let mut sizes = self.block_sizes();
        sizes.reverse();
        sizes
            .iter()
            .filter(|size| **size > 1)
            .map(|size| format!("{size}={}+{}", size.div_ceil(2), size - size.div_ceil(2)))
            .collect::<Vec<_>>()
            .join(" ")
    }
}

/// A 2x2 gate, row-major.
type Gate<T> = [T; 4];

fn rows<T: Clone>(gate: &Gate<T>) -> Vec<Vec<T>> {
    vec![
        vec![gate[0].clone(), gate[1].clone()],
        vec![gate[2].clone(), gate[3].clone()],
    ]
}

/// Unnormalised Hadamard; the `2^(-n/2)` is applied once, at the end.
fn walsh<T: Amplitude>() -> Gate<T> {
    [T::one(), T::one(), T::one(), T::of(-1.0)]
}
fn pauli_x<T: Amplitude>() -> Gate<T> {
    [T::zero(), T::one(), T::one(), T::zero()]
}
/// `|0><0|` or `|1><1|`.
fn projector<T: Amplitude>(bit: usize) -> Gate<T> {
    outer(bit, bit)
}
fn phase_gate<T: Amplitude>(theta: f64) -> Gate<T> {
    [T::one(), T::zero(), T::zero(), T::phase(theta)]
}
/// `|row><col|`.
fn outer<T: Amplitude>(row: usize, col: usize) -> Gate<T> {
    let mut gate = [T::zero(), T::zero(), T::zero(), T::zero()];
    gate[2 * row + col] = T::one();
    gate
}

/// A register's grammar levels plus the identity operator at each of them.
///
/// Caching the identities matters: `place` needs one for every subtree that
/// holds no gate, which is most of them, and rebuilding those towers dominated
/// the gate-heavy algorithms.
struct Ops<'g, T> {
    register: &'g Register,
    identity: HashMap<usize, GcflobddT<'g, T>>,
}

impl<'g, T: Amplitude> Ops<'g, T> {
    fn new(register: &'g Register, context: &RefCell<Context<'g>>) -> Self {
        let identity = register
            .matrix
            .iter()
            .map(|(qubits, grammar)| {
                (
                    *qubits,
                    GcflobddT::mk_identity(T::one(), T::zero(), grammar, context),
                )
            })
            .collect();
        Self { register, identity }
    }

    fn identity(&self, qubits: usize) -> &GcflobddT<'g, T> {
        self.identity
            .get(&qubits)
            .unwrap_or_else(|| panic!("no {qubits}-qubit identity in this register's tree"))
    }

    fn matrix(&self, qubits: usize) -> &'g Grammar {
        self.register.matrix(qubits)
    }
}

/// The operator that applies `gates` at their qubit positions and the identity
/// everywhere else, over the `qubits`-wide block starting at `offset`.
///
/// `gates` must be sorted by qubit and already restricted to that block; each
/// level splits it where the grammar splits rather than re-scanning it, so a
/// full n-qubit layer costs `O(n log n)` and not `O(n^2)`. Subtrees holding no
/// gate are the cached identity outright.
fn place<'g, T: Amplitude>(
    ops: &Ops<'g, T>,
    qubits: usize,
    offset: usize,
    gates: &[(usize, Gate<T>)],
    context: &RefCell<Context<'g>>,
) -> GcflobddT<'g, T> {
    debug_assert!(
        gates.windows(2).all(|w| w[0].0 <= w[1].0),
        "gates must be sorted"
    );
    let Some(first) = gates.first() else {
        return ops.identity(qubits).clone();
    };
    if qubits == 1 {
        debug_assert_eq!(gates.len(), 1);
        return GcflobddT::from_matrix(&rows(&first.1), ops.matrix(1), context);
    }
    let high = qubits.div_ceil(2);
    let mid = gates.partition_point(|(qubit, _)| *qubit < offset + high);
    let left = place(ops, high, offset, &gates[..mid], context);
    let right = place(ops, qubits - high, offset + high, &gates[mid..], context);
    left.mk_kron(&right, ops.matrix(qubits), context)
}

/// The same gate on every qubit of a `qubits`-wide block.
///
/// One Kronecker product per distinct block size in the register's tree, of
/// which there are `O(log n)`, rather than the `O(n log n)` that placing each
/// qubit's gate separately would cost. Over a power of two every level's two
/// halves coincide and this is exactly the doubling fold -- `log n` squarings
/// -- that it generalises. A full Hadamard layer is why it matters.
fn uniform<'g, T: Amplitude>(
    ops: &Ops<'g, T>,
    qubits: usize,
    gate: &Gate<T>,
    context: &RefCell<Context<'g>>,
) -> GcflobddT<'g, T> {
    let mut built: HashMap<usize, GcflobddT<'g, T>> = HashMap::default();
    for size in ops.register.block_sizes() {
        let operator = if size == 1 {
            GcflobddT::from_matrix(&rows(gate), ops.matrix(1), context)
        } else {
            let high = size.div_ceil(2);
            built[&high].mk_kron(&built[&(size - high)], ops.matrix(size), context)
        };
        built.insert(size, operator);
        if size == qubits {
            break;
        }
    }
    built
        .remove(&qubits)
        .unwrap_or_else(|| panic!("no {qubits}-qubit block in this register's tree"))
}

/// `|0><0|_c (x) I  +  |1><1|_c (x) U_t`, built directly.
///
/// [`GcflobddT::mk_controlled`] does the whole thing in one pass over the
/// grammar and caches it, which matters because these circuits place one
/// controlled gate per qubit: composing each one as that sum -- two `place`
/// towers and a matrix addition -- was the single largest cost in GHZ,
/// Bernstein-Vazirani and Deutsch-Jozsa alike.
/// [`controlled_composed`] is the sum, kept as the oracle it is checked against.
fn controlled<'g, T: Amplitude>(
    ops: &Ops<'g, T>,
    qubits: usize,
    control: usize,
    target: usize,
    gate: Gate<T>,
    context: &RefCell<Context<'g>>,
) -> GcflobddT<'g, T> {
    GcflobddT::mk_controlled(
        control,
        target,
        &gate,
        T::one(),
        T::zero(),
        ops.matrix(qubits),
        context,
    )
}

/// The definition, written out: two Kronecker towers and a matrix addition.
/// Only [`smoke`] calls it, to check that the direct construction agrees.
fn controlled_composed<'g, T: Amplitude>(
    ops: &Ops<'g, T>,
    qubits: usize,
    control: usize,
    target: usize,
    gate: Gate<T>,
    context: &RefCell<Context<'g>>,
) -> GcflobddT<'g, T> {
    let off = place(ops, qubits, 0, &[(control, projector::<T>(0))], context);
    let mut on_gates = [(control, projector::<T>(1)), (target, gate)];
    on_gates.sort_by_key(|(qubit, _)| *qubit);
    let on = place(ops, qubits, 0, &on_gates, context);
    off.mk_matadd(&on, context)
}

fn swap<'g, T: Amplitude>(
    ops: &Ops<'g, T>,
    qubits: usize,
    i: usize,
    j: usize,
    context: &RefCell<Context<'g>>,
) -> GcflobddT<'g, T> {
    let (i, j) = (i.min(j), i.max(j));
    let terms = [
        [(i, outer::<T>(0, 0)), (j, outer::<T>(0, 0))],
        [(i, outer::<T>(0, 1)), (j, outer::<T>(1, 0))],
        [(i, outer::<T>(1, 0)), (j, outer::<T>(0, 1))],
        [(i, outer::<T>(1, 1)), (j, outer::<T>(1, 1))],
    ];
    let mut sum = place(ops, qubits, 0, &terms[0], context);
    for term in &terms[1..] {
        sum = sum.mk_matadd(&place(ops, qubits, 0, term, context), context);
    }
    sum
}

// ---------------------------------------------------------------------------
// Reporting -- the line `run_quantum.sh` parses
// ---------------------------------------------------------------------------

fn report<T>(
    elapsed: std::time::Duration,
    state: &GcflobddT<'_, T>,
    context: &RefCell<Context<'_>>,
) {
    let (nodes, edges) = state.count_nodes_and_edges();
    // The same diagram under the reference's counting convention. `totalCount`
    // is NOT comparable with the reference's `totalCount` -- it counts one edge
    // per connection and no return-map entries, where the reference counts two
    // and all of them -- so `cflobddConvTotal` is what to line up against it.
    let (cf_nodes, cf_edges) = state.count_cflobdd_convention();
    // `Duration:` in whole milliseconds is what the reference harness prints and
    // parses; `durationUs` is added because most of these runs finish inside one
    // millisecond, which that field cannot show.
    println!(
        "Duration: {} nodeCount: {nodes} edgeCount: {edges} totalCount: {} \
         durationUs: {} contextNodes: {} cflobddConvTotal: {}",
        elapsed.as_millis(),
        nodes + edges,
        elapsed.as_micros(),
        context.borrow().node_count(),
        cf_nodes + cf_edges,
    );
}

/// Deterministic xorshift, so a seed means the same thing on every run.
fn prng(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

fn secret_bits(n: usize, seed: u64) -> Vec<bool> {
    let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    (0..n).map(|_| prng(&mut state) % 2 == 1).collect()
}

fn close(actual: f64, expected: f64) -> bool {
    (actual - expected).abs() <= 1e-9 * expected.abs().max(1.0)
}

// ---------------------------------------------------------------------------
// GHZ:  H on qubit 0, then a CNOT chain.  (|0..0> + |1..1>) / sqrt(2)
// ---------------------------------------------------------------------------

fn ghz(n: usize) -> bool {
    let register = Register::new(n);
    let context = RefCell::new(Context::default());
    println!("GHZ start... n: {n}");

    let start = Instant::now();
    let ops = Ops::new(&register, &context);
    let mut state = GcflobddT::mk_basis_vector(0, 1.0f64, 0.0, register.vector(n), &context);
    let hadamard = place(&ops, n, 0, &[(0, walsh::<f64>())], &context);
    state = hadamard.mk_matvec(&state, &context);
    for i in 0..n - 1 {
        let gate = controlled(&ops, n, i, i + 1, pauli_x::<f64>(), &context);
        state = gate.mk_matvec(&state, &context);
    }
    state = state.mk_scale(&std::f64::consts::FRAC_1_SQRT_2, &context);
    let duration = start.elapsed();

    // Exactly two computational basis states carry amplitude, both 1/sqrt(2).
    let zeros = vec![false; n];
    let ones = vec![true; n];
    let mut mixed = vec![false; n];
    mixed[n / 2] = true;
    let correct = close(state.evaluate(&zeros), std::f64::consts::FRAC_1_SQRT_2)
        && close(state.evaluate(&ones), std::f64::consts::FRAC_1_SQRT_2)
        && close(state.evaluate(&mixed), 0.0);

    println!("is same: {}", correct as u8);
    report(duration, &state, &context);
    correct
}

// ---------------------------------------------------------------------------
// GHZ, the reference implementation's way:  a 2n-qubit register held as a
// MATRIX, entangled by one product of CNOTs.
//
// `quantum_algos.cpp`'s `QuantumAlgos::GHZ` does not run the textbook circuit
// above. It works at `level = ceil(log2 n) + 2`, which is 4n variables -- an
// operator on 2n qubits -- and:
//
//   * builds `F = CNOT(0->n) * ... * CNOT(n-1->n)`, n separate CNOTs onto one
//     shared target, multiplied together as matrices rather than applied in
//     turn to a state;
//   * multiplies it into `1 (x) |0..0><0..01|`, the all-ones matrix on the low
//     n qubits tensored with a single-entry matrix on the high n;
//   * applies a Walsh layer to all 2n qubits.
//
// What comes out is the (n+1)-qubit GHZ state on qubits `0..=n`, carried inside
// a 2n-qubit operator whose remaining n-1 row bits and n column bits are free.
// That is a strictly larger object than the state vector `ghz` builds, and the
// point of having both is to be able to say how much of the reference's
// diagram is the algorithm and how much is the padding.
//
// `Int` amplitudes for the same reason Bernstein-Vazirani uses them: with
// unnormalised Walsh gates the surviving entries are exactly `2^n`, which is
// past `f64` at 1024 qubits and past it by 19000 digits at 65536. The reference
// carries the `2^-2n` inside its Walsh and leans on 100-digit floats instead;
// either way the diagram is the same, only the numbers in the return map
// differ.
// ---------------------------------------------------------------------------

fn ghz_matrix(n: usize) -> bool {
    let register = Register::new(2 * n);
    let context = RefCell::new(Context::default());
    println!("GHZ (matrix) start... n: {n}");

    let start = Instant::now();
    let ops = Ops::new(&register, &context);
    // F: every qubit below n controls the same target, qubit n.
    let mut operator = controlled(&ops, 2 * n, 0, n, pauli_x::<Int>(), &context);
    for i in 1..n {
        let cnot = controlled(&ops, 2 * n, i, n, pauli_x::<Int>(), &context);
        operator = operator.mk_matmul(&cnot, &context);
    }
    // The operand F multiplies. Both halves are matrices over n qubits, so
    // this is one Kronecker product of the register's two top-level blocks,
    // matching `KroneckerProduct2Vocs(NoDistinctionNode, MkBasisVector)`:
    // index 1 over 2n variables is the assignment `0...01`, which in the
    // interleaved order is row `0`, column `1`.
    let low = GcflobddT::mk_constant(Int::one(), register.matrix(n), &context);
    let high = GcflobddT::mk_basis_vector(1, Int::one(), Int::zero(), register.matrix(n), &context);
    let mut state = low.mk_kron(&high, register.matrix(2 * n), &context);
    state = operator.mk_matmul(&state, &context);
    let hadamard = uniform(&ops, 2 * n, &walsh::<Int>(), &context);
    state = hadamard.mk_matmul(&state, &context);
    let duration = start.elapsed();

    // Unnormalised, the entry at (row, col) is exactly `2^n` when the first
    // n+1 row bits agree and the column's high half is `0..01`, and exactly 0
    // otherwise; the rest of the row and column indices are free. Reading the
    // first n+1 row bits is what the reference samples, and `all ones or all
    // zeros` is what it checks.
    let column = {
        let mut column = vec![false; 2 * n];
        column[2 * n - 1] = true;
        column
    };
    let entry = |row: &[bool]| -> Int {
        let assignment: Vec<bool> = row
            .iter()
            .zip(&column)
            .flat_map(|(r, c)| [*r, *c])
            .collect();
        state.evaluate(&assignment)
    };
    let expected = Int::power_of_two(n as i32);
    let zeros = vec![false; 2 * n];
    let mut ones = vec![false; 2 * n];
    ones[..=n].fill(true);
    let mut mixed = ones.clone();
    mixed[n / 2] = false;
    let correct = entry(&zeros) == expected
        && entry(&ones) == expected
        && entry(&mixed) == Int::zero()
        // Column 0: the right row, in a column the state does not occupy.
        && state.evaluate(&vec![false; 4 * n]) == Int::zero();

    println!("is same: {}", correct as u8);
    report(duration, &state, &context);
    correct
}

// ---------------------------------------------------------------------------
// Bernstein-Vazirani:  one query recovers the secret string.
//
// n data qubits plus an ancilla, padded to 2n qubits as the reference
// implementation does.  The oracle is built outside the timed region, again
// as there -- it is the query, not the algorithm.
// ---------------------------------------------------------------------------

fn bernstein_vazirani(n: usize, seed: u64) -> bool {
    let register = Register::new(2 * n); // n data + ancilla, padded to 2n
    let context = RefCell::new(Context::default());
    let secret = secret_bits(n, seed);
    let ancilla = n;
    println!("BV start... n: {n} seed: {seed}");

    let ops = Ops::new(&register, &context);
    // U_f: |x>|y> -> |x>|y xor (a.x)>, a chain of CNOTs onto the ancilla.
    let oracle = {
        let mut oracle: Option<GcflobddT<Int>> = None;
        for (i, bit) in secret.iter().enumerate() {
            if !bit {
                continue;
            }
            let cnot = controlled(&ops, 2 * n, i, ancilla, pauli_x::<Int>(), &context);
            oracle = Some(match oracle {
                None => cnot,
                Some(previous) => previous.mk_matmul(&cnot, &context),
            });
        }
        oracle
    };

    let start = Instant::now();
    let mut state =
        GcflobddT::mk_basis_vector(0, Int::one(), Int::zero(), register.vector(2 * n), &context);
    // Ancilla to |1>, then Walsh on the data qubits and the ancilla.
    let flip = place(&ops, 2 * n, 0, &[(ancilla, pauli_x::<Int>())], &context);
    state = flip.mk_matvec(&state, &context);
    // H on the n data qubits and on the ancilla. The data half is uniform, so
    // it folds by doubling; only the ancilla needs placing.
    let data_layer = uniform(&ops, n, &walsh::<Int>(), &context);
    let ancilla_layer = place(&ops, n, n, &[(ancilla, walsh::<Int>())], &context);
    let hadamard = data_layer.mk_kron(&ancilla_layer, register.matrix(2 * n), &context);
    state = hadamard.mk_matvec(&state, &context);
    if let Some(oracle) = &oracle {
        state = oracle.mk_matvec(&state, &context);
    }
    // H on the data qubits only; the ancilla is left alone.
    let hadamard = uniform(&ops, n, &walsh::<Int>(), &context).mk_kron(
        ops.identity(n),
        register.matrix(2 * n),
        &context,
    );
    state = hadamard.mk_matvec(&state, &context);
    let duration = start.elapsed();

    // Unnormalised, the data register holds the secret with amplitude exactly
    // 2^n and every other string exactly 0; the 2^(-(2n+1)/2) that would make
    // this a unit vector stays symbolic, since it is not an integer.
    let mut expected = secret.clone();
    expected.resize(2 * n, false);
    let mut other = expected.clone();
    other[0] = !other[0];
    let correct = state.evaluate(&expected) == Int::power_of_two(n as i32)
        && state.evaluate(&other) == Int::zero();

    println!("equal: {}", correct as u8);
    report(duration, &state, &context);
    correct
}

// ---------------------------------------------------------------------------
// Deutsch-Jozsa:  constant or balanced, in one query.
// ---------------------------------------------------------------------------

fn deutsch_jozsa(n: usize, seed: u64) -> bool {
    let register = Register::new(2 * n);
    let context = RefCell::new(Context::default());
    let balanced = seed % 2 == 1;
    let ancilla = n;
    println!("DJ start... n: {n} seed: {seed} balanced: {balanced}");

    let ops = Ops::new(&register, &context);
    // Balanced: f(x) = x_0 xor ... xor x_{n-1}, i.e. a CNOT from every qubit.
    // Constant: f(x) = 0, i.e. no oracle at all.
    let oracle = if balanced {
        let mut oracle: Option<GcflobddT<Int>> = None;
        for i in 0..n {
            let cnot = controlled(&ops, 2 * n, i, ancilla, pauli_x::<Int>(), &context);
            oracle = Some(match oracle {
                None => cnot,
                Some(previous) => previous.mk_matmul(&cnot, &context),
            });
        }
        oracle
    } else {
        None
    };

    let start = Instant::now();
    let mut state =
        GcflobddT::mk_basis_vector(0, Int::one(), Int::zero(), register.vector(2 * n), &context);
    let flip = place(&ops, 2 * n, 0, &[(ancilla, pauli_x::<Int>())], &context);
    state = flip.mk_matvec(&state, &context);
    // H on the n data qubits and on the ancilla. The data half is uniform, so
    // it folds by doubling; only the ancilla needs placing.
    let data_layer = uniform(&ops, n, &walsh::<Int>(), &context);
    let ancilla_layer = place(&ops, n, n, &[(ancilla, walsh::<Int>())], &context);
    let hadamard = data_layer.mk_kron(&ancilla_layer, register.matrix(2 * n), &context);
    state = hadamard.mk_matvec(&state, &context);
    if let Some(oracle) = &oracle {
        state = oracle.mk_matvec(&state, &context);
    }
    // H on the data qubits only; the ancilla is left alone.
    let hadamard = uniform(&ops, n, &walsh::<Int>(), &context).mk_kron(
        ops.identity(n),
        register.matrix(2 * n),
        &context,
    );
    state = hadamard.mk_matvec(&state, &context);
    let duration = start.elapsed();

    // Constant: all the amplitude sits on |0...0>, exactly 2^n unnormalised.
    // Balanced: none of it does, exactly 0.
    let zeros = vec![false; 2 * n];
    let amplitude = state.evaluate(&zeros);
    let correct = if balanced {
        amplitude == Int::zero()
    } else {
        amplitude == Int::power_of_two(n as i32)
    };

    println!("is_correct: {}", correct as u8);
    report(duration, &state, &context);
    correct
}

// ---------------------------------------------------------------------------
// QFT:  the textbook ladder -- reversal, then H and controlled phases.
// ---------------------------------------------------------------------------

fn qft(n: usize, seed: u64) -> bool {
    // As for Grover: the 2^(-n/2) normalisation wants an integer exponent.
    assert_eq!(
        n % 2,
        0,
        "QFT needs an even qubit count so that its 2^(-n/2) scale is exact"
    );
    let register = Register::new(n);
    let context = RefCell::new(Context::default());
    let input = secret_bits(n, seed);
    println!("QFT start... n: {n} seed: {seed}");

    let index: Vec<bool> = input.clone();
    let start = Instant::now();
    let ops = Ops::new(&register, &context);
    let mut state = {
        // |input>, built from its bits: a basis vector one qubit at a time.
        let mut state =
            GcflobddT::mk_basis_vector(0, C64::one(), C64::zero(), register.vector(n), &context);
        let flips: Vec<_> = index
            .iter()
            .enumerate()
            .filter(|(_, bit)| **bit)
            .map(|(q, _)| (q, pauli_x::<C64>()))
            .collect();
        if !flips.is_empty() {
            state = place(&ops, n, 0, &flips, &context).mk_matvec(&state, &context);
        }
        state
    };

    for i in 0..n / 2 {
        let gate = swap::<C64>(&ops, n, i, n - 1 - i, &context);
        state = gate.mk_matvec(&state, &context);
    }
    for i in (0..n).rev() {
        let hadamard = place(&ops, n, 0, &[(i, walsh::<C64>())], &context);
        state = hadamard.mk_matvec(&state, &context);
        for j in 0..i {
            let theta = std::f64::consts::TAU / 2f64.powi((i - j + 1) as i32);
            let gate = controlled(&ops, n, j, i, phase_gate::<C64>(theta), &context);
            state = gate.mk_matvec(&state, &context);
        }
    }
    state = state.mk_scale(&C64::power_of_two(-(n as i32) / 2), &context);
    let duration = start.elapsed();

    // Every amplitude has magnitude 2^(-n/2), and amplitude k is
    // exp(2 pi i * s * k / 2^n) / sqrt(2^n). Check the ones a usize can index.
    let uniform_magnitude = 2f64.powi(-(n as i32) / 2);
    let value = |bits: &[bool]| state.evaluate(bits);
    let zeros = vec![false; n];
    let mut correct = close(value(&zeros).magnitude(), uniform_magnitude);
    if n <= 62 {
        let s: u64 = index
            .iter()
            .enumerate()
            .map(|(q, bit)| (*bit as u64) << (n - 1 - q))
            .sum();
        for k in [1u64, 2, 3] {
            if k >= (1u64 << n) {
                break;
            }
            let bits: Vec<bool> = (0..n).map(|q| (k >> (n - 1 - q)) & 1 == 1).collect();
            let expected = C64::phase(
                std::f64::consts::TAU * (s.wrapping_mul(k) % (1u64 << n)) as f64
                    / (1u64 << n) as f64,
            );
            let got = value(&bits);
            correct &= close(got.re, expected.re * uniform_magnitude)
                && close(got.im, expected.im * uniform_magnitude);
        }
    }

    println!("is_correct: {}", correct as u8);
    report(duration, &state, &context);
    correct
}

// ---------------------------------------------------------------------------
// Grover:  amplify one marked string out of 2^n.
//
// Unlike everything above, this algorithm's cost is inherently exponential:
// it needs floor((pi/4) 2^(n/2)) iterations, whatever the representation. Two
// ways to pay that are implemented -- evolving the state one iteration at a
// time, and exponentiating the operator by squaring -- because the reference
// implementation takes the second and it is where its answers go wrong.
// ---------------------------------------------------------------------------

/// `m^exponent`, by binary exponentiation: `O(log exponent)` matrix multiplies
/// in place of `exponent` matrix-vector ones.
///
/// This is the shape of the reference's `MultiplyRec`, and the reason its
/// Grover gets through 51,471 iterations in a fraction of a second -- the
/// operator is raised to a power symbolically rather than the state being
/// evolved. It is only sound if the multiply is accurate enough to survive
/// `log2(exponent)` squarings, which BENCHMARKS.md works out.
fn matrix_power<'g, T: Amplitude>(
    operator: &GcflobddT<'g, T>,
    exponent: &rug::Integer,
    identity: &GcflobddT<'g, T>,
    context: &RefCell<Context<'g>>,
) -> GcflobddT<'g, T> {
    let mut result = identity.clone();
    let mut base = operator.clone();
    let bits = exponent.significant_bits();
    for bit in 0..bits {
        if exponent.get_bit(bit) {
            result = result.mk_matmul(&base, context);
        }
        if bit + 1 < bits {
            base = base.mk_matmul(&base, context);
        }
    }
    result
}

/// `floor((pi/4) 2^(n/2))`, the standard iteration count, computed exactly.
///
/// An arbitrary-precision integer rather than a `u128`: at 1024 qubits this is
/// a 512-bit number, and the reference computes it the same way, from a
/// `cpp_dec_float` pi.
fn grover_iterations(n: usize) -> rug::Integer {
    let precision = u32::try_from(n + 64).expect("precision fits a u32");
    let pi = rug::Float::with_val(precision, rug::float::Constant::Pi);
    let scaled = (pi / 4u32) << u32::try_from(n / 2).expect("n/2 fits a u32");
    scaled
        .floor()
        .to_integer()
        .expect("the iteration count is finite")
}

/// The assignment carrying the largest-magnitude amplitude.
///
/// The reference draws one sample from `|amplitude|^2` instead; taking the peak
/// is deterministic and is what a sampler converges to, and the success
/// *probability* is reported separately so nothing is hidden by the choice.
fn peak<T: Amplitude>(state: &GcflobddT<'_, T>) -> Vec<bool> {
    let (best, _) = state
        .values()
        .iter()
        .enumerate()
        .max_by(|(_, a), (_, b)| a.magnitude().total_cmp(&b.magnitude()))
        .expect("a state has at least one amplitude");
    state
        .find_one_path_to_index(best)
        .into_iter()
        // A don't-care bit means every string through this exit has the same
        // amplitude, so any completion is as good an answer as another.
        .map(|bit| bit.unwrap_or(false))
        .collect()
}

fn bit_string(bits: &[bool]) -> String {
    bits.iter().map(|b| if *b { '1' } else { '0' }).collect()
}

/// How much of the result to check, which is what sets the size ceiling.
///
/// The checks themselves cost almost nothing -- two `evaluate`s and some scalar
/// arithmetic against a diagram the simulation already built -- so weakening
/// one buys no speed. What it buys is *reach*, because each check needs its own
/// values to be representable:
///
/// - [`Theory`](Check::Theory) compares the whole state against
///   `sin((2k+1)theta)` and `cos((2k+1)theta)/sqrt(N-1)` in `f64`, so it stops
///   at 2046 qubits, where `N = 2^n` becomes infinite;
/// - [`Answer`](Check::Answer) only decodes the peak amplitude, which stays
///   near 1 at any size;
/// - [`None`](Check::None) builds the state and stops.
#[derive(Clone, Copy, PartialEq)]
enum Check {
    Theory,
    Answer,
    None,
}

impl Check {
    fn parse(name: &str) -> Option<Self> {
        match name {
            "theory" | "full" => Some(Self::Theory),
            "answer" => Some(Self::Answer),
            "none" => Some(Self::None),
            _ => None,
        }
    }
}

fn grover<T: Amplitude>(n: usize, seed: u64, exponentiate: bool, check: Check) -> bool {
    // sqrt(N) = 2^(n/2) runs through the iteration count, the initial amplitude
    // and the theory check, and every amplitude type here carries an integer
    // exponent, so require the half to be one. The grammar has no such
    // preference -- the register's tree divides any count -- this is the
    // algorithm's arithmetic asking, not the representation.
    assert_eq!(
        n % 2,
        0,
        "Grover needs an even qubit count so that sqrt(2^n) = 2^(n/2) is exact"
    );
    let register = Register::new(n);
    let context = RefCell::new(Context::default());
    let secret = secret_bits(n, seed);
    let iterations = grover_iterations(n);
    let mode = if exponentiate {
        "exponentiate"
    } else {
        "iterate"
    };
    println!("Grover start... n: {n} seed: {seed} iterations: {iterations} mode: {mode}");
    println!("grammar: {}", register.shape());

    let start = Instant::now();
    let ops = Ops::new(&register, &context);

    // U_w = I - 2|w><w|: the phase oracle. |w><w| is one projector per qubit.
    let marked: Vec<_> = secret
        .iter()
        .enumerate()
        .map(|(qubit, bit)| (qubit, projector::<T>(*bit as usize)))
        .collect();
    let oracle = ops.identity(n).mk_matadd(
        &place(&ops, n, 0, &marked, &context).mk_scale(&T::of(-2.0), &context),
        &context,
    );

    // U_s = (2/N) J - I: the diffusion operator, J the all-ones matrix -- which
    // is a Kronecker power of the all-ones 2x2, so it folds by doubling.
    let all_ones = uniform(&ops, n, &[T::one(), T::one(), T::one(), T::one()], &context);
    let diffusion = all_ones
        .mk_scale(&T::power_of_two(1 - n as i32), &context)
        .mk_matadd(&ops.identity(n).mk_scale(&T::of(-1.0), &context), &context);

    let operator = diffusion.mk_matmul(&oracle, &context);

    // |s>: the uniform superposition, every amplitude 1/sqrt(N). Compared
    // against the type's own zero rather than through `magnitude`, whose `f64`
    // would call 2^-2048 an underflow when a wide float holds it exactly.
    let initial = T::power_of_two(-(n as i32) / 2);
    assert!(
        initial != T::zero_like(&initial),
        "2^(-{n}/2) underflows this amplitude type"
    );
    let mut state = GcflobddT::mk_constant(initial, register.vector(n), &context);
    if exponentiate {
        let power = matrix_power(&operator, &iterations, ops.identity(n), &context);
        state = power.mk_matvec(&state, &context);
    } else {
        let steps = iterations
            .to_u64()
            .expect("more than 2^64 iterations cannot be walked one at a time");
        for _ in 0..steps {
            state = operator.mk_matvec(&state, &context);
        }
    }
    let answer = (check != Check::None).then(|| peak(&state));
    let duration = start.elapsed();

    let correct = match &answer {
        None => {
            println!("equal: na");
            false
        }
        Some(answer) => {
            let correct = *answer == secret;
            // The strings are `n` characters each, so print them only while
            // that is readable; the flag is what the harness parses anyway.
            if n <= 1024 {
                println!("s: {} ans_s: {}", bit_string(&secret), bit_string(answer));
            }
            println!("equal: {}", correct as u8);
            correct
        }
    };

    // After k iterations the marked amplitude is exactly sin((2k+1)theta) and
    // every other one cos((2k+1)theta)/sqrt(N-1), with theta = asin(1/sqrt(N)).
    // Checking both pins down the whole state, since it holds only those two
    // values -- far stronger than checking that one sample came back right.
    let matches_theory = if check == Check::Theory {
        let root_n = 2f64.powi((n / 2) as i32);
        // Past 2046 qubits `N = 2^n` is f64's infinity and the expected
        // unmarked amplitude underflows to zero, which would make the second
        // half of the check below compare 0 against 0 and pass for any state at
        // all. Refuse rather than claim a verification that is not happening:
        // what this needs is a wide float in the checker, not in the
        // simulation. Larger sizes have to drop to `Check::Answer`.
        assert!(
            root_n.is_finite(),
            "the theory check is f64 arithmetic and cannot reach {n} qubits; \
             pass `answer` or `none` as the check argument"
        );
        let angle = (2.0 * iterations.to_f64() + 1.0) * (1.0 / root_n).asin();
        // sqrt(N - 1), written so that it neither overflows at 1024 qubits
        // (where N = 2^1024 is f64's infinity) nor rounds to sqrt(N) at 4,
        // where the difference between sqrt(15) and 4 is 3%.
        let root_n_minus_one = root_n * (1.0 - 2f64.powi(-(n as i32))).sqrt();
        let mut unmarked = secret.clone();
        unmarked[0] = !unmarked[0];
        let near = |actual: f64, expected: f64| (actual - expected).abs() <= 1e-6;
        let marked_amplitude = state.evaluate(&secret).magnitude();
        let matches = near(marked_amplitude, angle.sin().abs())
            && near(
                state.evaluate(&unmarked).magnitude(),
                (angle.cos() / root_n_minus_one).abs(),
            );
        println!(
            "probability: {:.6e} theory: {:.6e} matches theory: {}",
            marked_amplitude * marked_amplitude,
            angle.sin().powi(2),
            matches as u8
        );
        matches
    } else {
        // Nothing was verified, so say so rather than print a flag that would
        // be read as a pass.
        println!("matches theory: na");
        false
    };
    report(duration, &state, &context);
    // Whether the checks that ran passed. Under `Theory` the answer alone is
    // not enough: a state that was only partly amplified still peaks on the
    // marked string, which is exactly how the reference's Grover -- and this
    // one in `f64` past 108 qubits -- reads correct while being wrong.
    match check {
        Check::Theory => correct && matches_theory,
        Check::Answer => correct,
        Check::None => true,
    }
}

// ---------------------------------------------------------------------------

fn usage(program: &str) -> ! {
    eprintln!(
        "usage: {program} <test> <size> [seed] [check]\n\
         tests: testGHZAlgo | testGHZAlgoMatrix | testBVAlgo | testDJAlgo\n\
         \x20      | testQFT | testGroversAlgo | testGroversAlgoFast\n\
         \x20      | testGroversAlgoBig\n\
         \x20      (ghz | ghz-matrix | bv | dj | qft | grover | grover-fast\n\
         \x20       | grover-big)\n\
         size:  <p> for n = 2^p qubits, matching the reference CFLOBDD harness,\n\
         \x20      or qN for exactly N qubits (e.g. q200)\n\
         check: theory (default) | answer | none -- Grover only; `theory` is\n\
         \x20      f64 arithmetic and stops at 2046 qubits"
    );
    std::process::exit(2)
}

/// Squaring the Grover operator needs roughly `n/2` bits of mantissa before the
/// rotation is representable at all; `n + 64` leaves room.
fn grover_precision(qubits: usize) -> u32 {
    u32::try_from(qubits + 64).expect("precision fits a u32")
}

/// The qubit count named by the size argument.
///
/// A bare integer is the reference harness's `p`, meaning `2^p` qubits, so that
/// the same command line drives either binary. `qN` asks for exactly `N`, which
/// the reference cannot express and nothing here needs to be a power of two:
/// the register's grammar tree is aligned-balanced, so any count divides.
fn qubit_count(size: &str) -> Option<usize> {
    match size.strip_prefix('q') {
        Some(exact) => exact.parse().ok().filter(|n| *n >= 2),
        None => size
            .parse::<u32>()
            .ok()
            .filter(|p| *p < usize::BITS)
            .map(|p| 1usize << p),
    }
}

/// Every algorithm at a small size, with every correctness check asserted --
/// what `cargo test --test quantum` runs, since this target has no harness of
/// its own. The benchmark paths only *report* correctness, so that a run which
/// comes back wrong still yields a measurement.
fn smoke() {
    // The direct constructor must produce the *same diagram* as the sum it
    // replaces -- equality here is pointer equality on the interned node plus
    // the value map, so a build that was merely correct entry-by-entry would
    // fail. Uneven qubit counts included, and both gate shapes the algorithms
    // below use.
    for qubits in [2usize, 3, 4, 5, 6, 7, 8] {
        let register = Register::new(qubits);
        let context = RefCell::new(Context::default());
        let ops = Ops::new(&register, &context);
        for control in 0..qubits {
            for target in 0..qubits {
                if control == target {
                    continue;
                }
                for gate in [pauli_x::<C64>(), phase_gate::<C64>(0.7), walsh::<C64>()] {
                    assert_eq!(
                        controlled(&ops, qubits, control, target, gate, &context),
                        controlled_composed(&ops, qubits, control, target, gate, &context),
                        "controlled gate on {qubits} qubits, control {control}, target {target}"
                    );
                }
            }
        }
    }

    // Counts that are not powers of two, so the aligned-balanced tree is
    // exercised where its splits are uneven: 6 divides 3/3 and then 2/1, 10
    // divides 5/5 and then 3/2. GHZ, BV and DJ take odd counts too; Grover and
    // QFT need an even one for their 2^(n/2).
    for qubits in [4usize, 5, 6, 7] {
        assert!(ghz(qubits), "GHZ at {qubits} qubits");
        assert!(ghz_matrix(qubits), "GHZ (matrix) at {qubits} qubits");
        for seed in 1..=3 {
            assert!(
                bernstein_vazirani(qubits, seed),
                "BV at {qubits} qubits, seed {seed}"
            );
            // Even seeds are the constant oracle, odd ones the balanced oracle.
            assert!(
                deutsch_jozsa(qubits, seed),
                "DJ at {qubits} qubits, seed {seed}"
            );
        }
    }
    for qubits in [4usize, 6, 10] {
        for seed in 1..=3 {
            assert!(qft(qubits, seed), "QFT at {qubits} qubits, seed {seed}");
            assert!(
                grover::<f64>(qubits, seed, false, Check::Theory),
                "Grover iterated at {qubits} qubits, seed {seed}"
            );
            assert!(
                grover::<f64>(qubits, seed, true, Check::Theory),
                "Grover exponentiated at {qubits} qubits, seed {seed}"
            );
            set_working_precision(grover_precision(qubits));
            assert!(
                grover::<Real>(qubits, seed, true, Check::Theory),
                "Grover in a wide float at {qubits} qubits, seed {seed}"
            );
        }
    }
    println!("\nall algorithms verified at small sizes, powers of two or not");
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() == 1 {
        return smoke();
    }
    if args.len() < 3 {
        usage(&args[0]);
    }
    let n = qubit_count(&args[2]).unwrap_or_else(|| usage(&args[0]));
    let seed: u64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(1);
    let check = match args.get(4) {
        None => Check::Theory,
        Some(name) => Check::parse(name).unwrap_or_else(|| usage(&args[0])),
    };

    match args[1].as_str() {
        "testGHZAlgo" | "ghz" => ghz(n),
        "testGHZAlgoMatrix" | "ghz-matrix" => ghz_matrix(n),
        "testBVAlgo" | "bv" => bernstein_vazirani(n, seed),
        "testDJAlgo" | "dj" => deutsch_jozsa(n, seed),
        "testQFT" | "qft" => qft(n, seed),
        "testGroversAlgo" | "grover" => grover::<f64>(n, seed, false, check),
        "testGroversAlgoFast" | "grover-fast" => grover::<f64>(n, seed, true, check),
        "testGroversAlgoBig" | "grover-big" => {
            set_working_precision(grover_precision(n));
            grover::<Real>(n, seed, true, check)
        }
        _ => usage(&args[0]),
    };
}
