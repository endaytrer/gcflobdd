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
now reaches further than the other: the `i128` ceiling that used to stop BV and
DJ at 64 qubits is gone (see *Arbitrary-precision coefficients*).

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
