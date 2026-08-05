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
    /// `2^exponent`, the amplitude these algorithms concentrate on.
    fn power_of_two(exponent: u32) -> Self {
        Self(rug::Integer::from(1) << exponent)
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

fn ghz(p: usize) {
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
}

// ---------------------------------------------------------------------------
// Bernstein-Vazirani:  one query recovers the secret string.
//
// n data qubits plus an ancilla, padded to 2n qubits as the reference
// implementation does.  The oracle is built outside the timed region, again
// as there -- it is the query, not the algorithm.
// ---------------------------------------------------------------------------

fn bernstein_vazirani(p: usize, seed: u64) {
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
    let correct = state.evaluate(&expected) == Int::power_of_two(n as u32)
        && state.evaluate(&other) == Int::zero();

    println!("equal: {}", correct as u8);
    report(duration, &state, &context);
}

// ---------------------------------------------------------------------------
// Deutsch-Jozsa:  constant or balanced, in one query.
// ---------------------------------------------------------------------------

fn deutsch_jozsa(p: usize, seed: u64) {
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
        amplitude == Int::power_of_two(n as u32)
    };

    println!("is_correct: {}", correct as u8);
    report(duration, &state, &context);
}

// ---------------------------------------------------------------------------
// QFT:  the textbook ladder -- reversal, then H and controlled phases.
// ---------------------------------------------------------------------------

fn qft(p: usize, seed: u64) {
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
}

// ---------------------------------------------------------------------------

fn usage(program: &str) -> ! {
    eprintln!(
        "usage: {program} <test> <p> [seed]\n\
         tests: testGHZAlgo | testBVAlgo | testDJAlgo | testQFT (ghz | bv | dj | qft)\n\
         qubit count is n = 2^p, matching the reference CFLOBDD harness"
    );
    std::process::exit(2)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
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
        _ => usage(&args[0]),
    }
}
