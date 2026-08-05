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
    /// an `f64` literal can carry.
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

/// The grammar towers for a register of `2^p` qubits.
///
/// `matrix[k]` describes an operator on `2^k` qubits (`2^(k+1)` variables) and
/// `vector[k]` a state of `2^k` qubits. Each level is the concatenation of two
/// copies of the one below, so every subtree of `matrix[k]` *is* `matrix[k-1]`
/// by pointer -- which is what lets [`GcflobddT::mk_kron`] assemble operators
/// out of single-qubit gates.
struct Levels {
    matrix: Vec<Grammar>,
    vector: Vec<Grammar>,
}

impl Levels {
    fn new(p: usize) -> Self {
        let mut matrix = vec![Grammar::new(&["S0 -> a a".to_string()]).unwrap()];
        let mut vector = vec![matrix[0].halved()];
        for k in 0..p {
            let m = matrix[k].concat(&matrix[k]);
            let v = vector[k].concat(&vector[k]);
            matrix.push(m);
            vector.push(v);
        }
        Self { matrix, vector }
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
    levels: &'g Levels,
    identity: Vec<GcflobddT<'g, T>>,
}

impl<'g, T: Amplitude> Ops<'g, T> {
    fn new(levels: &'g Levels, context: &RefCell<Context<'g>>) -> Self {
        let identity = (0..levels.matrix.len())
            .map(|k| GcflobddT::mk_identity(T::one(), T::zero(), &levels.matrix[k], context))
            .collect();
        Self { levels, identity }
    }
}

/// The operator that applies `gates` at their qubit positions and the identity
/// everywhere else, over the `2^k`-qubit block starting at `offset`.
///
/// `gates` must be sorted by qubit and already restricted to that block; each
/// level splits it at the midpoint rather than re-scanning it, so a full
/// n-qubit layer costs `O(n log n)` and not `O(n^2)`. Subtrees holding no gate
/// are the cached identity outright.
fn place<'g, T: Amplitude>(
    ops: &Ops<'g, T>,
    k: usize,
    offset: usize,
    gates: &[(usize, Gate<T>)],
    context: &RefCell<Context<'g>>,
) -> GcflobddT<'g, T> {
    debug_assert!(
        gates.windows(2).all(|w| w[0].0 <= w[1].0),
        "gates must be sorted"
    );
    let Some(first) = gates.first() else {
        return ops.identity[k].clone();
    };
    if k == 0 {
        debug_assert_eq!(gates.len(), 1);
        return GcflobddT::from_matrix(&rows(&first.1), &ops.levels.matrix[0], context);
    }
    let half = 1usize << (k - 1);
    let mid = gates.partition_point(|(qubit, _)| *qubit < offset + half);
    let low = place(ops, k - 1, offset, &gates[..mid], context);
    let high = place(ops, k - 1, offset + half, &gates[mid..], context);
    low.mk_kron(&high, &ops.levels.matrix[k], context)
}

/// The same gate on every qubit of a `2^k` block, folded by doubling.
///
/// `O(k)` rather than the `O(2^k log 2^k)` that placing each qubit's gate
/// separately would cost -- every factor is identical, so each level is the
/// previous one squared. A full Hadamard layer is the reason this matters.
fn uniform<'g, T: Amplitude>(
    ops: &Ops<'g, T>,
    k: usize,
    gate: &Gate<T>,
    context: &RefCell<Context<'g>>,
) -> GcflobddT<'g, T> {
    let mut operator = GcflobddT::from_matrix(&rows(gate), &ops.levels.matrix[0], context);
    for level in 1..=k {
        operator = operator.mk_kron(&operator, &ops.levels.matrix[level], context);
    }
    operator
}

/// `|0><0|_c (x) I  +  |1><1|_c (x) U_t`: the standard controlled gate, and the
/// reason matrix addition is needed at all.
fn controlled<'g, T: Amplitude>(
    ops: &Ops<'g, T>,
    k: usize,
    control: usize,
    target: usize,
    gate: Gate<T>,
    context: &RefCell<Context<'g>>,
) -> GcflobddT<'g, T> {
    let off = place(ops, k, 0, &[(control, projector::<T>(0))], context);
    let mut on_gates = [(control, projector::<T>(1)), (target, gate)];
    on_gates.sort_by_key(|(qubit, _)| *qubit);
    let on = place(ops, k, 0, &on_gates, context);
    off.mk_matadd(&on, context)
}

fn swap<'g, T: Amplitude>(
    ops: &Ops<'g, T>,
    k: usize,
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
    let mut sum = place(ops, k, 0, &terms[0], context);
    for term in &terms[1..] {
        sum = sum.mk_matadd(&place(ops, k, 0, term, context), context);
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
    // `Duration:` in whole milliseconds is what the reference harness prints and
    // parses; `durationUs` is added because most of these runs finish inside one
    // millisecond, which that field cannot show.
    println!(
        "Duration: {} nodeCount: {nodes} edgeCount: {edges} totalCount: {} \
         durationUs: {} contextNodes: {}",
        elapsed.as_millis(),
        nodes + edges,
        elapsed.as_micros(),
        context.borrow().node_count()
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

fn ghz(p: usize) -> bool {
    let n = 1usize << p;
    let levels = Levels::new(p);
    let context = RefCell::new(Context::default());
    println!("GHZ start... n: {n}");

    let start = Instant::now();
    let ops = Ops::new(&levels, &context);
    let mut state = GcflobddT::mk_basis_vector(0, 1.0f64, 0.0, &levels.vector[p], &context);
    let hadamard = place(&ops, p, 0, &[(0, walsh::<f64>())], &context);
    state = hadamard.mk_matvec(&state, &context);
    for i in 0..n - 1 {
        let gate = controlled(&ops, p, i, i + 1, pauli_x::<f64>(), &context);
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
// Bernstein-Vazirani:  one query recovers the secret string.
//
// n data qubits plus an ancilla, padded to 2n qubits as the reference
// implementation does.  The oracle is built outside the timed region, again
// as there -- it is the query, not the algorithm.
// ---------------------------------------------------------------------------

fn bernstein_vazirani(p: usize, seed: u64) -> bool {
    let n = 1usize << p;
    let levels = Levels::new(p + 1); // n data + ancilla, padded to 2n
    let context = RefCell::new(Context::default());
    let secret = secret_bits(n, seed);
    let ancilla = n;
    println!("BV start... n: {n} seed: {seed}");

    let ops = Ops::new(&levels, &context);
    // U_f: |x>|y> -> |x>|y xor (a.x)>, a chain of CNOTs onto the ancilla.
    let oracle = {
        let mut oracle: Option<GcflobddT<Int>> = None;
        for (i, bit) in secret.iter().enumerate() {
            if !bit {
                continue;
            }
            let cnot = controlled(&ops, p + 1, i, ancilla, pauli_x::<Int>(), &context);
            oracle = Some(match oracle {
                None => cnot,
                Some(previous) => previous.mk_matmul(&cnot, &context),
            });
        }
        oracle
    };

    let start = Instant::now();
    let mut state =
        GcflobddT::mk_basis_vector(0, Int::one(), Int::zero(), &levels.vector[p + 1], &context);
    // Ancilla to |1>, then Walsh on the data qubits and the ancilla.
    let flip = place(&ops, p + 1, 0, &[(ancilla, pauli_x::<Int>())], &context);
    state = flip.mk_matvec(&state, &context);
    // H on the n data qubits and on the ancilla. The data half is uniform, so
    // it folds by doubling; only the ancilla needs placing.
    let data_layer = uniform(&ops, p, &walsh::<Int>(), &context);
    let ancilla_layer = place(&ops, p, n, &[(ancilla, walsh::<Int>())], &context);
    let hadamard = data_layer.mk_kron(&ancilla_layer, &levels.matrix[p + 1], &context);
    state = hadamard.mk_matvec(&state, &context);
    if let Some(oracle) = &oracle {
        state = oracle.mk_matvec(&state, &context);
    }
    // H on the data qubits only; the ancilla is left alone.
    let hadamard = uniform(&ops, p, &walsh::<Int>(), &context).mk_kron(
        &ops.identity[p],
        &levels.matrix[p + 1],
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

fn deutsch_jozsa(p: usize, seed: u64) -> bool {
    let n = 1usize << p;
    let levels = Levels::new(p + 1);
    let context = RefCell::new(Context::default());
    let balanced = seed % 2 == 1;
    let ancilla = n;
    println!("DJ start... n: {n} seed: {seed} balanced: {balanced}");

    let ops = Ops::new(&levels, &context);
    // Balanced: f(x) = x_0 xor ... xor x_{n-1}, i.e. a CNOT from every qubit.
    // Constant: f(x) = 0, i.e. no oracle at all.
    let oracle = if balanced {
        let mut oracle: Option<GcflobddT<Int>> = None;
        for i in 0..n {
            let cnot = controlled(&ops, p + 1, i, ancilla, pauli_x::<Int>(), &context);
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
        GcflobddT::mk_basis_vector(0, Int::one(), Int::zero(), &levels.vector[p + 1], &context);
    let flip = place(&ops, p + 1, 0, &[(ancilla, pauli_x::<Int>())], &context);
    state = flip.mk_matvec(&state, &context);
    // H on the n data qubits and on the ancilla. The data half is uniform, so
    // it folds by doubling; only the ancilla needs placing.
    let data_layer = uniform(&ops, p, &walsh::<Int>(), &context);
    let ancilla_layer = place(&ops, p, n, &[(ancilla, walsh::<Int>())], &context);
    let hadamard = data_layer.mk_kron(&ancilla_layer, &levels.matrix[p + 1], &context);
    state = hadamard.mk_matvec(&state, &context);
    if let Some(oracle) = &oracle {
        state = oracle.mk_matvec(&state, &context);
    }
    // H on the data qubits only; the ancilla is left alone.
    let hadamard = uniform(&ops, p, &walsh::<Int>(), &context).mk_kron(
        &ops.identity[p],
        &levels.matrix[p + 1],
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

fn qft(p: usize, seed: u64) -> bool {
    let n = 1usize << p;
    let levels = Levels::new(p);
    let context = RefCell::new(Context::default());
    let input = secret_bits(n, seed);
    println!("QFT start... n: {n} seed: {seed}");

    let index: Vec<bool> = input.clone();
    let start = Instant::now();
    let ops = Ops::new(&levels, &context);
    let mut state = {
        // |input>, built from its bits: a basis vector one qubit at a time.
        let mut state =
            GcflobddT::mk_basis_vector(0, C64::one(), C64::zero(), &levels.vector[p], &context);
        let flips: Vec<_> = index
            .iter()
            .enumerate()
            .filter(|(_, bit)| **bit)
            .map(|(q, _)| (q, pauli_x::<C64>()))
            .collect();
        if !flips.is_empty() {
            state = place(&ops, p, 0, &flips, &context).mk_matvec(&state, &context);
        }
        state
    };

    for i in 0..n / 2 {
        let gate = swap::<C64>(&ops, p, i, n - 1 - i, &context);
        state = gate.mk_matvec(&state, &context);
    }
    for i in (0..n).rev() {
        let hadamard = place(&ops, p, 0, &[(i, walsh::<C64>())], &context);
        state = hadamard.mk_matvec(&state, &context);
        for j in 0..i {
            let theta = std::f64::consts::TAU / 2f64.powi((i - j + 1) as i32);
            let gate = controlled(&ops, p, j, i, phase_gate::<C64>(theta), &context);
            state = gate.mk_matvec(&state, &context);
        }
    }
    state = state.mk_scale(&C64::of(2f64.powi(-(n as i32) / 2)), &context);
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
    exponent: u128,
    identity: &GcflobddT<'g, T>,
    context: &RefCell<Context<'g>>,
) -> GcflobddT<'g, T> {
    let mut result = identity.clone();
    let mut base = operator.clone();
    let mut exponent = exponent;
    while exponent > 0 {
        if exponent & 1 == 1 {
            result = result.mk_matmul(&base, context);
        }
        exponent >>= 1;
        if exponent > 0 {
            base = base.mk_matmul(&base, context);
        }
    }
    result
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

fn grover<T: Amplitude>(p: usize, seed: u64, exponentiate: bool) -> bool {
    let n = 1usize << p;
    let levels = Levels::new(p);
    let context = RefCell::new(Context::default());
    let secret = secret_bits(n, seed);
    // floor((pi/4) sqrt(N)) iterations, as the reference uses. The count itself
    // is what caps this at 254 qubits, whatever the amplitude type.
    let iterations = (std::f64::consts::FRAC_PI_4 * 2f64.powf(n as f64 / 2.0)).floor();
    assert!(
        iterations < u128::MAX as f64,
        "iteration count overflows u128"
    );
    let iterations = iterations as u128;
    let mode = if exponentiate {
        "exponentiate"
    } else {
        "iterate"
    };
    println!("Grover start... n: {n} seed: {seed} iterations: {iterations} mode: {mode}");

    let start = Instant::now();
    let ops = Ops::new(&levels, &context);

    // U_w = I - 2|w><w|: the phase oracle. |w><w| is one projector per qubit.
    let marked: Vec<_> = secret
        .iter()
        .enumerate()
        .map(|(qubit, bit)| (qubit, projector::<T>(*bit as usize)))
        .collect();
    let oracle = ops.identity[p].mk_matadd(
        &place(&ops, p, 0, &marked, &context).mk_scale(&T::of(-2.0), &context),
        &context,
    );

    // U_s = (2/N) J - I: the diffusion operator, J the all-ones matrix -- which
    // is a Kronecker power of the all-ones 2x2, so it folds by doubling.
    let all_ones = uniform(&ops, p, &[T::one(), T::one(), T::one(), T::one()], &context);
    let diffusion = all_ones
        .mk_scale(&T::power_of_two(1 - n as i32), &context)
        .mk_matadd(&ops.identity[p].mk_scale(&T::of(-1.0), &context), &context);

    let operator = diffusion.mk_matmul(&oracle, &context);

    // |s>: the uniform superposition, every amplitude 1/sqrt(N).
    let initial = T::power_of_two(-(n as i32) / 2);
    assert!(
        initial.magnitude() > 0.0,
        "2^(-{n}/2) underflows this amplitude type"
    );
    let mut state = GcflobddT::mk_constant(initial, &levels.vector[p], &context);
    if exponentiate {
        let power = matrix_power(&operator, iterations, &ops.identity[p], &context);
        state = power.mk_matvec(&state, &context);
    } else {
        for _ in 0..iterations {
            state = operator.mk_matvec(&state, &context);
        }
    }
    let answer = peak(&state);
    let duration = start.elapsed();

    // After k iterations the marked amplitude is exactly sin((2k+1)theta) and
    // every other one cos((2k+1)theta)/sqrt(N-1), with theta = asin(1/sqrt(N)).
    // Checking both pins down the whole state, since it holds only those two
    // values -- far stronger than checking that one sample came back right.
    let root_n = 2f64.powf(n as f64 / 2.0);
    let angle = (2.0 * iterations as f64 + 1.0) * (1.0 / root_n).asin();
    let mut unmarked = secret.clone();
    unmarked[0] = !unmarked[0];
    let near = |actual: f64, expected: f64| (actual - expected).abs() <= 1e-6;
    let marked_amplitude = state.evaluate(&secret).magnitude();
    let matches_theory = near(marked_amplitude, angle.sin().abs())
        && near(
            state.evaluate(&unmarked).magnitude(),
            (angle.cos() / (root_n * root_n - 1.0).sqrt()).abs(),
        );

    let correct = answer == secret;
    println!("s: {} ans_s: {}", bit_string(&secret), bit_string(&answer));
    println!("equal: {}", correct as u8);
    println!(
        "probability: {:.6e} theory: {:.6e} matches theory: {}",
        marked_amplitude * marked_amplitude,
        angle.sin().powi(2),
        matches_theory as u8
    );
    report(duration, &state, &context);
    // The answer alone is not enough: a state that was only partly amplified
    // still peaks on the marked string, which is exactly how the reference's
    // Grover -- and this one in `f64` past 108 qubits -- reads correct while
    // being wrong.
    correct && matches_theory
}

// ---------------------------------------------------------------------------

fn usage(program: &str) -> ! {
    eprintln!(
        "usage: {program} <test> <p> [seed]\n\
         tests: testGHZAlgo | testBVAlgo | testDJAlgo | testQFT | testGroversAlgo\n\
         \x20      | testGroversAlgoFast | testGroversAlgoBig\n\
         \x20      (ghz | bv | dj | qft | grover | grover-fast | grover-big)\n\
         qubit count is n = 2^p, matching the reference CFLOBDD harness"
    );
    std::process::exit(2)
}

/// Squaring the Grover operator needs roughly `n/2` bits of mantissa before the
/// rotation is representable at all; `n + 64` leaves room.
fn grover_precision(p: usize) -> u32 {
    u32::try_from((1usize << p) + 64).expect("precision fits a u32")
}

/// Every algorithm at a small size, with every correctness check asserted --
/// what `cargo test --test quantum` runs, since this target has no harness of
/// its own. The benchmark paths only *report* correctness, so that a run which
/// comes back wrong still yields a measurement.
fn smoke() {
    assert!(ghz(3), "GHZ at 8 qubits");
    for seed in 1..=3 {
        assert!(bernstein_vazirani(2, seed), "BV at 4 qubits, seed {seed}");
        // Even seeds are the constant oracle, odd ones the balanced oracle.
        assert!(deutsch_jozsa(2, seed), "DJ at 4 qubits, seed {seed}");
        assert!(qft(2, seed), "QFT at 4 qubits, seed {seed}");
        assert!(
            grover::<f64>(2, seed, false),
            "Grover iterated, seed {seed}"
        );
        assert!(
            grover::<f64>(2, seed, true),
            "Grover exponentiated, seed {seed}"
        );
        set_working_precision(grover_precision(2));
        assert!(
            grover::<Real>(2, seed, true),
            "Grover exponentiated in a wide float, seed {seed}"
        );
    }
    println!("\nall algorithms verified at small sizes");
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() == 1 {
        return smoke();
    }
    if args.len() < 3 {
        usage(&args[0]);
    }
    let p: usize = args[2].parse().unwrap_or_else(|_| usage(&args[0]));
    let seed: u64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(1);

    match args[1].as_str() {
        "testGHZAlgo" | "ghz" => ghz(p),
        "testBVAlgo" | "bv" => bernstein_vazirani(p, seed),
        "testDJAlgo" | "dj" => deutsch_jozsa(p, seed),
        "testQFT" | "qft" => qft(p, seed),
        "testGroversAlgo" | "grover" => grover::<f64>(p, seed, false),
        "testGroversAlgoFast" | "grover-fast" => grover::<f64>(p, seed, true),
        "testGroversAlgoBig" | "grover-big" => {
            set_working_precision(grover_precision(p));
            grover::<Real>(p, seed, true)
        }
        _ => usage(&args[0]),
    };
}
