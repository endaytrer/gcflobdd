# gcflobdd vs. the reference C++ CFLOBDD

Quantum-algorithm and matrix-operation benchmarks, measured against
[`trishullab/cflobdd`](https://github.com/trishullab/cflobdd) (unweighted build,
`../cflobdd`) on the same machine.

**Machine**: Fedora 44, Linux 7.1.4, 24 cores, 62 GB RAM.
**Rust**: `cargo build --release --test quantum --features bigint`.
**C++**: prebuilt `./cflobdd`, gcc 16.1.1, Boost 1.90.

Reproduce with:

```bash
# the full ladder, 2 to 65536 qubits, both implementations
LABEL=rust-bigint ONLY=rust RUST_BIN=<bigint build> PMAX=16 SEEDS=1 \
  OUT=results/ladder.csv scripts/compare_cflobdd.sh ghz bv dj
ONLY=cpp PMAX=16 SEEDS=1 APPEND=1 OUT=results/ladder.csv scripts/compare_cflobdd.sh ghz bv dj

PMAX=4 scripts/compare_cflobdd.sh qft   # QFT separately; both sides die past 16 qubits
cargo test --test matmul --release      # matrix operations (this crate only)

# Grover, ten seeds per size.  GROVER_MODE picks this crate's variant:
# testGroversAlgo (honest iteration), testGroversAlgoFast (f64 exponentiation)
# or testGroversAlgoBig (wide-float exponentiation, the default).
LABEL=rust-bigint ONLY=rust RUST_BIN=<bigint build> PMIN=2 PMAX=8 \
  SEEDS="1 2 3 4 5 6 7 8 9 10" OUT=results/grover_compare.csv \
  scripts/compare_cflobdd.sh grover
ONLY=cpp PMIN=2 PMAX=6 SEEDS="1 2 3 4 5 6 7 8 9 10" APPEND=1 \
  OUT=results/grover_compare.csv scripts/compare_cflobdd.sh grover
```

## TL;DR

Both implementations run GHZ, Bernstein-Vazirani and Deutsch-Jozsa correctly at
every size from 2 to **65536 qubits**. At the top of that ladder:

| | this crate | reference C++ | |
|---|--:|--:|---|
| **GHZ**, 65536 qubits | 2.18 s | 2.35 s | parity |
| **Bernstein-Vazirani**, 65536 qubits | **167 ms** | 429 ms | **2.6x** |
| **Deutsch-Jozsa**, 65536 qubits | **24 ms** | 89 ms | **3.8x** |
| **QFT**, 16 qubits | **102 ms** | 194 ms | **1.9x** |
| **Grover**, 32 qubits | **2.0 ms**, 10/10 right | 430 ms, **0/10 right** | 216x |
| **Grover**, 64 qubits | **4.0 ms**, 10/10 right | 10.8 s, **0/10 right** | 2700x |
| GHZ diagram, 65536 qubits | **216** | 914 | 4.2x smaller |
| BV diagram, 65536 qubits | **33,493** | 58,799 | 1.8x smaller |
| QFT diagram, 16 qubits | **287** | 132,238 | 461x smaller |
| peak RSS, 65536 qubits | **392-737 MB** | 987-1091 MB | |

Every run finishes inside 5 s wall-clock, the largest being GHZ at 2.8 s.

**Where each wins.** This crate is faster on BV, DJ and QFT at every size --
1.4x to 8x, the wider ratios at small sizes partly an artefact of the
reference's millisecond timer -- and holds a 1.8x to 4x smaller diagram
throughout. GHZ is a tie: the two trade places by ±16% from 512 qubits up,
because that circuit is `n` sequential gates for both. Neither implementation
now reaches further than the other on those four: the `i128` ceiling that used
to stop BV and DJ at 64 qubits is gone (see *Arbitrary-precision coefficients*).

**Grover is the exception, and it is not a speed result.** The reference's
Grover returns wrong answers from 16 qubits up -- 0 of 10 seeds correct at 16,
32 and 64 -- which its own repository documents and attributes to how it raises
the Grover operator to a power. This crate's is correct at every size run, to
**256 qubits**, in 10 ms. The 216x and 2700x above are real but secondary: the
comparison is between an answer and a wrong answer.

## Results

Times are each implementation's *own* internal timer around the same phase of
the algorithm. Size is nodes+edges of the result diagram (`CountNodesAndEdges`
there, `count_nodes_and_edges` here). One seed per size; `results/ladder.csv`
has every run.

### GHZ

| qubits | rust | c++ | ratio | rust size | c++ size |
|--:|--:|--:|--:|--:|--:|
| 8 | 0.16 ms | 2 ms | 12.7x | 34 | 186 |
| 256 | 6.2 ms | 7 ms | 1.1x | 104 | 466 |
| 4096 | 97 ms | 89 ms | 0.9x | 160 | 690 |
| 16384 | 450 ms | 388 ms | 0.9x | 188 | 802 |
| 65536 | 2184 ms | 2350 ms | 1.1x | **216** | 914 |

Both diagrams are logarithmic: the reference gains 56 nodes+edges per doubling
of the qubit count, this crate 14. Runtime is linear in qubits for both, since
the circuit is a chain of `n` CNOTs applied one at a time. Neither implementation
has a structural edge here, and the ratio wanders around 1.0 accordingly.

### Bernstein-Vazirani

| qubits | rust | c++ | ratio | rust size | c++ size |
|--:|--:|--:|--:|--:|--:|
| 8 | 0.44 ms | 1 ms | 2.3x | 51 | 146 |
| 256 | 1.5 ms | 4 ms | 2.6x | 389 | 776 |
| 4096 | 14 ms | 32 ms | 2.3x | 3,129 | 5,627 |
| 16384 | 43 ms | 110 ms | 2.5x | 9,385 | 16,540 |
| 65536 | **167 ms** | 429 ms | **2.6x** | 33,493 | 58,799 |

A steady 2.3-2.6x from 256 qubits up, with the diagram consistently 1.8x
smaller. BV's diagram *must* grow linearly -- it encodes the planted secret, `n`
incompressible bits -- and both implementations are within a constant factor of
that bound.

### Deutsch-Jozsa

| qubits | rust | c++ | ratio | rust size | c++ size |
|--:|--:|--:|--:|--:|--:|
| 8 | 0.37 ms | 1 ms | 2.7x | 43 | 91 |
| 256 | 0.45 ms | 2 ms | 4.4x | 113 | 186 |
| 4096 | 1.8 ms | 7 ms | 4.0x | 169 | 262 |
| 16384 | 5.9 ms | 22 ms | 3.7x | 197 | 300 |
| 65536 | **24 ms** | 89 ms | **3.8x** | 225 | 338 |

The widest sustained margin, at a diagram that stays logarithmic on both sides.

### QFT

| qubits | rust (bigint) | rust (i128) | c++ | ratio | rust size | c++ size |
|--:|--:|--:|--:|--:|--:|--:|
| 8 | 1.2 ms | 2.4 ms | 2 ms | 1.7x | 29 | 644 |
| 16 | **102 ms** | 66 ms | 194 ms | **1.9x** | **287** | **132,238** |
| 32 | does not finish | does not finish | exhausts memory (9.3 GB) | - | - | - |

Medians of three seeds; QFT timings are noisy on both sides (±40% run to run at
16 qubits), so treat the ratio as approximate. QFT is the one algorithm here
that does *not* need arbitrary precision -- 16 qubits is far below the `i128`
ceiling -- and it runs about 1.5x faster without it, the largest `bigint`
overhead measured anywhere.

QFT is also where the representations diverge most: the reference's diagram goes
644 -> 132,238 nodes+edges from 8 to 16 qubits and then out of memory, while
this crate's stays at 287. Both fail at 32 qubits -- the reference on memory,
this crate on time.

### Grover

The one algorithm here whose cost is exponential whatever the representation:
it applies the same operator `M = U_s U_w` a fixed `floor((pi/4) 2^(n/2))`
times. There are two ways to pay that, and this crate implements both because
the reference takes the second:

- **honest iteration** -- apply `M` to the state `k` times (`testGroversAlgo`);
- **operator exponentiation** -- build `M^k` by repeated squaring, `O(log k)`
  matrix multiplies, then apply it once (`testGroversAlgoFast` in `f64`,
  `testGroversAlgoBig` in a wide float).

Head to head, this crate exponentiating in a wide float against the reference's
`testGroversAlgo`. Ten seeds per size; "right" counts runs whose answer is the
planted string. All rows are the `bigint` build; the default `i128` build gives
the same answers to 64 qubits and refuses past that, since the diffusion
operator is dense and its path counts reach `2^n`.

| qubits | rust | right | c++ | right | ratio | rust size | c++ size |
|--:|--:|--:|--:|--:|--:|--:|--:|
| 4 | 0.38 ms | **10/10** | 2 ms | 10/10 | 5x | 15 | 74 |
| 8 | 0.64 ms | **10/10** | 4.5 ms | 7/10 | 7x | 26 | 128 |
| 16 | 1.5 ms | **10/10** | 26.5 ms | **0/10** | 18x | 49 | 212 |
| 32 | 2.0 ms | **10/10** | 430 ms | **0/10** | 216x | 78 | 426 |
| 64 | 4.0 ms | **10/10** | 10.8 s | **0/10** | 2700x | 123 | 809 |
| 128 | 7.8 ms | **10/10** | not run | - | - | 204 | - |
| 256 | 10.2 ms | **10/10** | not run | - | - | 335 | - |

At 256 qubits that is a search over `2^256` items and `2.7 * 10^38` iterations
of the Grover operator, simulated in 10 ms and 6 MB. The reference's own timing
is erratic at 64 qubits -- 3.5 s to 20.7 s across the ten seeds -- and its peak
RSS reaches 1.37 GB against 5-6 MB here, though most of that gap is the ~867 MB
of caches it preallocates at startup regardless of the problem.

The degradation starts before 16 qubits: at 8 qubits it already misses 3 of 10
seeds, where theory puts the single-shot success probability at 0.99995.

**Correctness is checked differently on the two sides, and more strictly here.**
The reference draws one sample from `|amplitude|^2` and asks whether it equals
the planted string. This crate reports the *peak*-amplitude string -- what a
sampler converges to -- and then additionally checks the whole state against
theory: after `k` iterations the marked amplitude must be `sin((2k+1)theta)`
and every other one `cos((2k+1)theta)/sqrt(N-1)`, with `theta = asin(2^(-n/2))`.
Since the state holds exactly those two values, that check pins all of it. Both
hold at every size in the table, to within `1e-6`. The success probability is
0.961 at 4 qubits and above 0.9999 everywhere else, so the two decoding rules
agree with probability at least 0.96 anyway.

**How the reference fails.** Not by being slow or running out of memory -- it
stays under a second to 32 qubits and its diagram stays tiny. It returns a wrong
string, quickly. At 16 qubits the answers average 6.4 of 16 bits wrong and at 32
qubits 13.1 of 32, against a random-guess baseline of `n/2` -- so the state is
barely amplified rather than mis-sampled. The `cflobdd` repository's own
investigation reaches the same conclusion and localises it to `MultiplyRec`, its
memoized binary-splitting exponentiation of `M`, which carries a rescaling that
is applied on some paths and not on others.

### Reaching M^k: iteration, squaring, and how much mantissa it takes

The three modes, medians per size, with the theory check applied. Most of these
runs are milliseconds long and noisy -- treat differences under 2x as nothing.
The 796 s entry is a single run, for obvious reasons:

| qubits | iterate (f64) | exponentiate (f64) | exponentiate (wide) |
|--:|--:|--:|--:|
| 4 | 0.24 ms | 0.28 ms | 0.38 ms |
| 8 | 0.20 ms | 0.63 ms | 0.64 ms |
| 16 | 0.35 ms | 1.1 ms | 1.5 ms |
| 32 | 10.1 ms | 2.2 ms | 2.0 ms |
| 64 | 796 s | 3.2 ms | 4.0 ms |
| 128 | infeasible | 5.4 ms **wrong** | 7.8 ms |
| 256 | infeasible | 7.2 ms **wrong** | 10.2 ms |

Honest iteration is the fastest thing here up to 16 qubits, but it is
`Theta(2^(n/2))` matrix-vector products by construction: 51,471 of them at 32
qubits, and 3,373,259,426 at 64. That last run does finish -- 796 s, the right
answer, and a marked amplitude within `9e-7` of theory after 3.4 billion
floating-point steps -- and it is the last size at which iterating is an option
at all. Exponentiation replaces those 3.4 billion matrix-vector products with 47
matrix multiplies (31 squarings and 16 accumulations), which is the 4.0 ms in
the same row: a factor of 199,000. At 256 qubits it is 150 multiplies.

**Squaring costs precision, and exactly how much is calculable.** `M` rotates
the state by `2 asin(2^(-n/2)) ~= 2^(1-n/2)` radians per iteration, while its
matrix entries are `O(1)`. A `b`-bit mantissa cannot represent the difference
between `M^j` and the identity until `j * 2^(1-n/2) > 2^-b`, so with `f64`
(`b = 53`) every squaring is silently rounded away once `n > 2(b+1) = 108`: the
first several `M -> M^2 -> M^4` steps return the same matrix, the rotation they
should have accumulated is lost, and the search ends up having rotated a
fraction of the way. A ladder in powers of two can only bracket that between 64
and 128 qubits, and that is where it lands: `f64` verifies against theory at 64
and fails at 128, arriving at a success probability of `2.4e-7` instead of 1.

That failure is worth dwelling on because **it still reports the right answer**.
The state is partially amplified, so the marked string is still the peak and
`equal:` reads 1 at 128 and 256 qubits; only the theory check catches it. That
is the same signature as the reference's failure, which is why the `verified`
column exists in `results/grover_compare.csv` at all.

The fix is mantissa, not representation: `testGroversAlgoBig` carries `n + 64`
bits (320 at 256 qubits) and verifies at every size. It costs nothing
measurable here -- the median wide-float/`f64` ratio over matched seeds is 1.08,
well inside a run-to-run spread that runs 0.5x to 3x on runs this short -- since
the diagrams hold only a handful of distinct amplitudes and the work is
structural. Note the reference already carries 100-digit floats throughout,
ample for 108 qubits and well beyond, and still fails at 16: that is what rules
precision out as the cause of *its* failure.

### Matrix operations

No comparable C++ driver exists (`testMatrixMultiplication` is marked obsolete
and takes no size), so these are this crate only, from
`cargo test --test matmul --release`:

| operation | dimension | time | diagram |
|---|--:|--:|--:|
| Kronecker fold of a 2x2 | 2^128 square | <1 ms | 17 nodes |
| `I * I`, `(J-I) * I` | 2^32 square | <1 ms | 13 nodes |
| `(J-I) * e_0` (matrix-vector) | 2^32 | <1 ms | 25 nodes |
| dense random `A * A` | 256 x 256 | 5.0 s | 8,354 nodes |
| dense random `A * v` | 256 | 20 ms | 8,363 nodes |

A Kronecker fold costs exactly two nodes per doubling. The dense random case is
the algorithm's worst case by construction -- an unstructured matrix has nothing
to share -- and is three orders of magnitude slower than the structured ones at
the same dimension.

## Arbitrary-precision coefficients

The deferred semiring counts how many products coincide, and a Hadamard layer
over `n` qubits makes that count `2^n`. With `i128` coefficients that overflows
at 128 qubits -- exact arithmetic refusing to be wrong rather than a blow-up in
time or memory. The `bigint` feature swaps in [`rug::Integer`], which is what
`boost::multiprecision::cpp_int` does for the reference.

**It costs about 11%** on GHZ (median over the sizes where both builds run, 32
to 65536 qubits: 1.11x, never worse than 1.45x), and about 1.5x on QFT, whose
many-exit diagrams do proportionally more coefficient arithmetic. That is far
cheaper than it sounds, because the whole point of the deferred semiring is that
coefficient arithmetic happens once per *exit*, not once per matrix entry -- the
work is dominated by structure, not by numbers.

With `bigint` there is no qubit ceiling left, which is why the tables above run
to 65536 on every algorithm. Without it, BV and DJ stop at 64.

## Amplitudes need range, not just precision

BV and DJ hit a second ceiling that has nothing to do with coefficients: with
unnormalised Walsh gates their amplitudes run to exactly `2^n`, which leaves
`f64`'s exponent range at about 1024 qubits. The answers came back wrong at
1024, not slow.

Every amplitude in those two algorithms is an *integer*, so this crate's
benchmark holds them as `rug::Integer` and leaves the `2^(-(2n+1)/2)`
normalisation symbolic. That is exact at any size -- the answer at 65536 qubits
is `2^65536` on the nose -- and it is why the correctness column reads 1 all the
way up. The reference reaches for 100-digit floats for the same reason; integers
are both cheaper and exact here.

GHZ keeps `f64` (its amplitudes are ±1/sqrt(2) at every size) and QFT keeps a
double-precision complex.

## What made the difference

Four changes came out of profiling this comparison. The first two are in the
library; the last two are in how the benchmark drives it.

1. **Hashed value interning** (`ValueSet` in `src/gcflobdd/matmul/mod.rs`).
   Collapsing equal exit values used a linear scan, quadratic in the exit count.
   Harmless for boolean work; ruinous for a Fourier-transformed state with
   thousands of distinct amplitudes -- `perf` put 97.6% of a 16-qubit QFT in
   `substitute`/`collapse`. Hashing (via `MatMulValue::dedup_key`), with a scan
   retained below 16 entries: **QFT at 16 qubits, 2341 ms -> 65 ms, 31x**.

2. **Cached identity operators** (`Ops` in `tests/quantum.rs`). Placing a gate
   needs an identity for every subtree without one, and rebuilding those towers
   dominated the gate-heavy algorithms: 22% of GHZ was `add_gcflobdd_node`, 20%
   `add_return_map`. Building each level's identity once: **GHZ at 256 qubits,
   45.2 ms -> 7.3 ms, 6.2x**.

3. **Sorted gate slices** (`place`). Filtering the whole gate list at every tree
   node is `O(n^2)` for a full `n`-qubit layer -- fine at 256 qubits, hopeless
   at 65536. Splitting a sorted slice at each level makes it `O(n log n)`.

4. **Folding uniform layers by doubling** (`uniform`). A Hadamard layer has `n`
   identical factors, so it is `log n` squarings, not `n` placements. This is
   what the reference's `KroneckerPower` does. **DJ at 4096 qubits, 10.3 ms ->
   1.7 ms, 6.1x**; it turned DJ at 65536 from a 1.8x loss into a 3.8x win.

## Caveats

These matter for reading the numbers honestly.

- **The C++ timer has millisecond granularity**, so its figures below ~5 ms are
  rounded 1-4 ms and the small-size ratios are real but imprecise. This crate
  reports microseconds (`durationUs`). The large-size rows, where both are tens
  of milliseconds or more, are the trustworthy ones.
- **The circuits are not identical.** GHZ here is the textbook circuit -- H then
  a CNOT chain over `n` qubits, applied gate by gate; the reference multiplies
  `n` CNOT matrices together over a `2n`-qubit register and applies the product.
  Same state, different work. BV and DJ *do* follow the reference's structure,
  including building the oracle outside the timed region, as it does -- which is
  why their wall-clock (4.3-4.7 s at 65536 qubits) far exceeds the timed region.
- **A state stays a vector here.** `mk_matvec` keeps a state at `n` variables;
  the reference pads it into a `2n`-variable matrix and uses matrix multiply.
  That is a real advantage of this crate's API, not a measurement artifact, but
  it does mean less work per gate.
- **Value types differ.** Exact integers (BV, DJ), `f64` (GHZ) and a hand-rolled
  double-precision complex (QFT) here; `cpp_dec_float` and `cpp_complex_100`
  there. The reference is carrying 100-digit floats where this crate carries
  machine words or exact integers. It is a small part of either profile --
  CFLOBDD defers arithmetic to the top node -- and it does not explain the QFT
  diagram-size gap, which is structural.
- **The reference's QFT is unchecked.** Its harness prints no correctness line
  for QFT (`correct=na`). This crate's QFT is verified against
  `exp(2 pi i s k / 2^n) / sqrt(2^n)` at every size run.
- **One reference harness bug was hit**: `testBVAlgo` samples until it draws a
  non-zero string, so a seed whose secret is all zeros never terminates (seed 3
  at 2 qubits, 120 s timeout). BV rows use seed 1.
- **The same seed does not mean the same secret.** The reference draws its
  planted strings from `std::mt19937`, this crate from its own xorshift, in BV,
  DJ and Grover alike. Grover's oracle is a Kronecker fold of one projector per
  qubit, so a string with repeated runs of bits shares more subtrees than a
  scattered one and costs a little less; per-seed times are therefore not
  comparable across implementations, only the medians over ten seeds are.
- **Grover's answer is decoded differently on the two sides** -- peak amplitude
  here, one sample there. The success probability is reported alongside so the
  choice can be discounted; at 0.96 and above the two rules agree almost always.
- **Grover's reference numbers are from `testGroversAlgo`**, the
  `GroversAlgoWithV4` entry point. That repository also carries a
  `testGroversAlgoHonest` variant added while investigating the failure; it was
  not measured here, and the working tree it lives in has uncommitted changes.
