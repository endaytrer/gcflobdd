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
LABEL=rust-bigint ONLY=rust RUST_BIN=<bigint build> PMIN=2 PMAX=10 \
  SEEDS="1 2 3 4 5 6 7 8 9 10" OUT=results/grover_compare.csv \
  scripts/compare_cflobdd.sh grover
CPP_BIN=<c++ build> CPP_LABEL=cpp-fixed ONLY=cpp PMIN=2 PMAX=9 \
  SEEDS="1 2 3 4 5 6 7 8 9 10" APPEND=1 \
  OUT=results/grover_compare.csv scripts/compare_cflobdd.sh grover

# Past 2046 qubits the theory check cannot run, so ask for a weaker one:
#   <binary> testGroversAlgoBig <p> <seed> [theory | answer | none]
<bigint build> testGroversAlgoBig 14 1 answer      # 16384 qubits, 2.8 s
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
| **Grover**, 128 qubits | **7.8 ms** | 10 ms | 1.3x |
| **Grover**, 512 qubits | **18 ms** | 115 ms | **6.3x** |
| **Grover**, 1024 qubits | **35 ms** | segfaults | |
| **Grover**, 16384 qubits | **2.8 s** | out of reach | |
| GHZ diagram, 65536 qubits | **216** | 914 | 4.2x smaller |
| BV diagram, 65536 qubits | **33,493** | 58,799 | 1.8x smaller |
| QFT diagram, 16 qubits | **287** | 132,238 | 461x smaller |
| Grover diagram, 512 qubits | **570** | 1,028 | 1.8x smaller |
| peak RSS, 65536 qubits | **392-737 MB** | 987-1091 MB | |

Every run finishes inside 5 s wall-clock, the largest being GHZ at 2.8 s.

**Where each wins.** This crate is faster on BV, DJ and QFT at every size --
1.4x to 8x, the wider ratios at small sizes partly an artefact of the
reference's millisecond timer -- and holds a 1.8x to 4x smaller diagram
throughout. GHZ is a tie: the two trade places by ±16% from 512 qubits up,
because that circuit is `n` sequential gates for both. Neither implementation
now reaches further than the other on those four: the `i128` ceiling that used
to stop BV and DJ at 64 qubits is gone (see *Arbitrary-precision coefficients*).

**Grover needs a caveat.** The reference's Grover is wrong from 16 qubits up as
its `HEAD` stands -- 0 of 10 seeds correct at 16, 32 and 64 -- so its rows above
come from a build carrying a one-line fix to an uninitialised field in its
multiply, without which the comparison would be against a wrong answer. Fixed,
the two are close through the middle of the ladder (1.3-2x from 16 to 128
qubits) and diverge at the top, where this crate is 6.3x faster at 512 qubits
and still running at 1024 and beyond. Ours is also verified against theory at
every size to 1024 qubits, and answers correctly under a weaker check to
**16384**.

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
`testGroversAlgo`. Ten seeds per size to 128 qubits, three beyond; both sides
answered every run correctly. All rows here are the `bigint` build; the default
`i128` build gives the same answers to 64 qubits and refuses past that, since
the diffusion operator is dense and its path counts reach `2^n`.

**The reference is built from its `HEAD` plus a one-line fix.** As it stands, its
Grover returns wrong answers from 16 qubits up -- 0 of 10 seeds at 16, 32 and 64
-- so timing it against a correct implementation would measure nothing. An
uninitialised field in its multiply is responsible; initialising it makes the
reference correct at every size below, and much faster besides, because the same
defect was inflating its diagrams. That fix is one line in `matmult_map.cpp` and
belongs in that project; the numbers below are what it gets with the fix
applied.

| qubits | rust | c++ | ratio | rust size | c++ size |
|--:|--:|--:|--:|--:|--:|
| 4 | 0.38 ms | 2 ms | 5.3x | 15 | 47 |
| 8 | 0.64 ms | 2 ms | 3.1x | 26 | 73 |
| 16 | 1.5 ms | 3 ms | 2.1x | 49 | 110 |
| 32 | 2.0 ms | 4 ms | 2.0x | 78 | 161 |
| 64 | 4.0 ms | 6 ms | 1.5x | 123 | 250 |
| 128 | 7.8 ms | 10 ms | 1.3x | 204 | 388 |
| 256 | 10.2 ms | 30 ms | 2.9x | 335 | 617 |
| 512 | 18.4 ms | 115 ms | 6.3x | 570 | 1,028 |
| 1024 | **35.4 ms** | segfaults | - | 1,009 | - |

The two are within 1.3-2x of each other from 16 to 128 qubits, which is the
honest reading of the middle of this ladder -- and the reference's millisecond
timer makes the 4- and 8-qubit ratios unreliable, since 2 ms there is one tick.
The gap reopens at the top: 2.9x at 256 qubits and 6.3x at 512, because the
reference's cost per doubling grows faster than this crate's. It then crashes
with `SIGSEGV` at 1024 qubits, on all three seeds, about 2 s in. The diagram is
consistently about 2x smaller here at every size.

At 1024 qubits this crate is searching `2^1024` items -- `1.05 * 10^154`
iterations of the Grover operator, an exact 512-bit integer -- in 35 ms and
9.4 MB. The reference's peak RSS is ~847 MB throughout, but that is almost
entirely the caches it preallocates at startup regardless of problem size.

**1024 qubits is the verifier's limit, not the simulation's.** Checking against
theory is `f64` arithmetic: `N = 2^n` becomes infinite at 2048 qubits and the
expected unmarked amplitude underflows to zero, so the check would compare zero
against zero and pass for any state at all. Rather than claim a verification
that is not happening, `grover` asserts on that -- 2046 qubits is a hard edge
with a message pointing at the alternative. See *How far with a weaker check*
below for where the simulation itself gives out, which is much later.

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

### How far with a weaker check

The checks are not what costs anything -- they are two `evaluate` calls and some
scalar arithmetic against a diagram the simulation has already finished
building. At 1024 qubits, full theory check 33 ms, answer only 29 ms, no check
31 ms: one measurement's worth of noise. So weakening a check buys no speed at
all. What it buys is *reach*, because each check needs its own values to be
representable, and `grover` takes a `check` argument saying which to run:

| `check` | what it establishes | stops at |
|---|---|--:|
| `theory` (default) | the whole state: both amplitudes against `sin`/`cos` | 2046 qubits |
| `answer` | the peak amplitude decodes to the planted string | - |
| `none` | nothing; builds the state and reports its size | - |

With `answer`, one seed per size:

| qubits | time | peak RSS | diagram | answer |
|--:|--:|--:|--:|---|
| 1024 | 35 ms | 9.4 MB | 1,009 | right |
| 2048 | 58 ms | 14 MB | 1,772 | right |
| 4096 | 152 ms | 25 MB | 3,033 | right |
| 8192 | 598 ms | 60 MB | 5,186 | right |
| 16384 | **2.77 s** | 177 MB | 9,271 | right |
| 32768 | 14.7 s | 604 MB | 17,372 | right |

**16384 qubits is what fits in 5 s** -- a search over `2^16384` with an
iteration count of about `10^2466`. Beyond that the wall is time, not
correctness or memory: 32768 qubits still returns the right answer, in 14.7 s
and 604 MB.

The cost is superlinear and getting worse: successive doublings cost 1.6x, 2.6x,
3.9x, 4.6x, 5.3x, i.e. an exponent climbing through 2 towards about 2.4. That is
what you would expect from three things growing together -- the number of matrix
multiplies is `O(n)`, the diagram they run on grows about linearly in `n`
(1,009 nodes and edges at 1024 qubits, 17,372 at 32768), and both the amplitudes
and the path coefficients are `n`-bit numbers.

**What `answer` does not establish.** It checks the algorithm's actual output,
so a wrong answer is caught. It does *not* check amplitude magnitudes, which
means it would not catch a state that is only partly amplified but still peaks
on the right string -- exactly how `f64` exponentiation fails from 128 to 512
qubits above. Those rows would have read "right" under `answer`. What makes the
wide-float rows above trustworthy anyway is an argument rather than a
measurement: the mantissa is `n + 64` bits, and the requirement derived below is
`2(b+1) > n`, which `b = n + 64` satisfies at every size by a wide margin.
Turning that argument back into a measurement past 2046 qubits needs the
verification arithmetic moved to a wide float too; only the simulation has been.

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
| 512 | infeasible | 20.8 ms **wrong** | 18.4 ms |
| 1024 | infeasible | 36.3 ms **wrong** | 35.4 ms |

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
`equal:` reads 1 from 128 through 512 qubits; only the theory check catches it.
That is the same signature as the reference's failure, which is why the
`verified` column exists in `results/grover_compare.csv` at all. It only becomes
self-evident at 1024 qubits, where the rotation is lost outright: `M^k` comes
back a multiple of the identity, the state stays exactly uniform, its amplitude
overflows to `inf`, and the diagram collapses to a **single node** -- 1009 nodes
and edges in the wide float against 1 in `f64`, for the same circuit.

The fix is mantissa, not representation: `testGroversAlgoBig` carries `n + 64`
bits (320 at 256 qubits) and verifies at every size. It costs nothing
measurable here -- the median wide-float/`f64` ratio over matched seeds is 1.08,
well inside a run-to-run spread that runs 0.5x to 3x on runs this short -- since
the diagrams hold only a handful of distinct amplitudes and the work is
structural. Note the reference already carries 100-digit floats throughout,
ample for 660 qubits by this same requirement, and still fails at 16, so its
failure is not a precision one.

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
  `GroversAlgoWithV4` entry point, built in a clean worktree from that
  repository's `HEAD` plus the one-line fix described above -- nothing else. Its
  own working tree carries unrelated uncommitted changes to the multiply, and
  the binary sitting there was built after them, so neither was used.
  `results/grover_compare.csv` keeps both sets of rows, `cpp` for `HEAD` as-is
  and `cpp-fixed` for the patched build.
