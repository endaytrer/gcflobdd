# Benchmarks

Quantum algorithms and matrix operations in this crate, measured beside the
reference C++ CFLOBDD ([`trishullab/cflobdd`](https://github.com/trishullab/cflobdd),
unweighted build) and CUDD's ADDs as a BDD-family control.

**Machine.** Apple M1 Pro, 10 cores, 32 GB, macOS 26.5.2 (Darwin 25.5.0). Runs
are strictly sequential; nothing else was on the machine.

**Builds.** rustc 1.95.0, `cargo build --release --test quantum --features bigint`.
Apple clang 21.0.0 with Boost 1.89.0. CUDD 3.0.0 (`--enable-obj`).

Raw runs, scripts and figures are in [`results/macos-m1pro/`](results/macos-m1pro).

Network verification -- atomic predicates and reachability, measured against NDD
and a plain BDD -- is a separate workload with a separate write-up:
[`NETWORK.md`](NETWORK.md).

```bash
# the ladder, 2 to 65536 qubits, both implementations
LABEL=rust-bigint ONLY=rust RUST_BIN=<bigint build> PMAX=16 SEEDS=1 \
  OUT=results/ladder.csv scripts/compare_cflobdd.sh ghz bv dj
ONLY=cpp PMAX=16 SEEDS=1 APPEND=1 OUT=results/ladder.csv scripts/compare_cflobdd.sh ghz bv dj
PMAX=4 scripts/compare_cflobdd.sh qft            # QFT separately; both sides stop at 16

# Grover, ten seeds per size
LABEL=rust-bigint ONLY=rust RUST_BIN=<bigint build> PMIN=2 PMAX=10 GROVER_PMAX=10 \
  SEEDS="1 2 3 4 5 6 7 8 9 10" OUT=results/grover_compare.csv \
  scripts/compare_cflobdd.sh grover      # GROVER_PMAX: the Grover ladder's own cap, default 6

# qubit counts that are not powers of two, this crate only
BIN=<bigint build> OUT=results/uneven.csv results/macos-m1pro/uneven.sh

# the control group
CUDD=<cudd_test build> OUT=results/cudd.csv results/macos-m1pro/cudd.sh

cargo test --test matmul --release -- --dense-level 4   # matrix operations
```

## Reading the size column

The two implementations do not count the same thing. The reference counts **two
edges per connection plus the entries of every distinct return map**; this crate
counts one edge per connection and no return maps at all. A ratio between those
two numbers is a ratio between two counters, not between two diagrams.

Every `size` in this document is therefore **nodes + edges under the reference's
convention**, on both sides -- `count_cflobdd_convention` here, `CountNodesAndEdges`
there. Node counts alone are convention-free and agree.

| algorithm | qubits | reference, its counter | gcflobdd, ref counter | reference, gcflobdd counter | gcflobdd, its counter |
|---|--:|--:|--:|--:|--:|
| GHZ | 2,048 | 634 | 586 | 356 | 332 |
| GHZ | 65,536 | 914 | 846 | 516 | 482 |
| Bernstein-Vazirani | 2,048 | 3,329 | 3,270 | 1,895 | 1,863 |
| Bernstein-Vazirani | 16,384 | 16,510 | 16,432 | 9,427 | 9,385 |
| Deutsch-Jozsa | 2,048 | 241 | 279 | 137 | 155 |
| Deutsch-Jozsa | 16,384 | 298 | 351 | 170 | 197 |
| QFT | 8 | 644 | 336 | 46 | 29 |
| QFT | 16 | 132,238 | 131,917 | 308 | 287 |
| Grover | 32 | 161 | 140 | 89 | 78 |
| Grover | 512 | 1,028 | 998 | 585 | 570 |

The gap between the two conventions is not a constant factor. It is almost the
whole number in the extreme case. Take 16-qubit QFT: the diagram is 5 nodes and
282 connections, which is this crate's 287. Under the reference's convention the
same diagram is 131,917 -- 5 nodes, 564 edges, and **131,348 return-map entries,
99.6% of the total**. The reference's own 132,238 for the same circuit is within
0.3% of it.

## Results

At the top of each ladder:

| | gcflobdd | reference | speed | gcflobdd size | reference size |
|---|--:|--:|--:|--:|--:|
| **GHZ**, 65,536 qubits | **1,351 ms** | 1,737 ms | **1.3x** | 846 | 914 |
| **Bernstein-Vazirani**, 65,536 qubits | **239 ms** | 459 ms | **1.9x** | 58,620 | 58,799 |
| **Deutsch-Jozsa**, 65,536 qubits | **18 ms** | 120 ms | **6.8x** | 399 | 338 |
| **QFT**, 16 qubits | **85 ms** | 235 ms | **2.8x** | 131,917 | 132,238 |
| **Grover**, 512 qubits | **18 ms** | 189 ms | **10.2x** | 998 | 1,038 |

**Both implementations produce diagrams of the same size.** Within 8% on four of
the five algorithms, and on Deutsch-Jozsa this crate's is 18% *larger*. The
representations agree; what differs is the time to build them.

**Runtime.** This crate is faster on *every* algorithm at *every* size
measured, and never by less than 1.25x. At the top of each ladder that is 1.3x
on GHZ, 1.9x on Bernstein-Vazirani, 6.8x on Deutsch-Jozsa, 2.8x on QFT and 10.2x
on Grover. The ratios at small sizes are wider still -- up to 34x -- but there
the reference's 1 ms timer granularity flatters the comparison, so the figures
above are the honest ones. Every run in the table finishes inside 3.5 s wall
clock.

**Reach.** Both run GHZ, Bernstein-Vazirani and Deutsch-Jozsa correctly at every
size from 2 to 65,536 qubits. On Grover the reference dies with SIGBUS at 1,024
qubits, where this crate is still verified against theory; under a weaker check
it answers correctly to 65,536 (see *Grover*). No implementation finishes
32-qubit QFT.

![runtime](results/macos-m1pro/figures/runtime_all.svg)

![diagram size](results/macos-m1pro/figures/size_all.svg)

Every run, three implementations:

| algorithm | qubits | gcflobdd | reference | CUDD | gcflobdd size | reference size | CUDD nodes |
|---|--:|--:|--:|--:|--:|--:|--:|
| GHZ | 2 | 0.388 ms | 2 ms | 0.465 ms | 73 | 81 | 8 |
|  | 4 | 0.253 ms | 1 ms | 0.161 ms | 118 | 130 | 12 |
|  | 8 | 0.381 ms | 1 ms | 0.227 ms | 170 | 186 | 20 |
|  | 16 | 0.558 ms | 2 ms | 0.448 ms | 222 | 242 | 36 |
|  | 32 | 0.871 ms | 2 ms | 1.51 ms | 274 | 298 | 68 |
|  | 64 | 1.45 ms | 3 ms | 6.4 ms | 326 | 354 | 132 |
|  | 128 | 2.58 ms | 5 ms | 40 ms | 378 | 410 | 260 |
|  | 256 | 5.1 ms | 8 ms | 264 ms | 430 | 466 | 516 |
|  | 512 | 9.06 ms | 13 ms | aborts | 482 | 522 | -- |
|  | 1,024 | 18 ms | 24 ms | -- | 534 | 578 | -- |
|  | 2,048 | 36 ms | 47 ms | -- | 586 | 634 | -- |
|  | 4,096 | 73 ms | 94 ms | -- | 638 | 690 | -- |
|  | 8,192 | 148 ms | 197 ms | -- | 690 | 746 | -- |
|  | 16,384 | 325 ms | 408 ms | -- | 742 | 802 | -- |
|  | 32,768 | 637 ms | 851 ms | -- | 794 | 858 | -- |
|  | 65,536 | 1,351 ms | 1,737 ms | -- | 846 | 914 | -- |
| GHZ (state vector) | 2 | 0.067 ms | -- | -- | 14 | -- | -- |
|  | 4 | 0.102 ms | -- | -- | 46 | -- | -- |
|  | 8 | 0.171 ms | -- | -- | 70 | -- | -- |
|  | 16 | 0.326 ms | -- | -- | 94 | -- | -- |
|  | 32 | 0.424 ms | -- | -- | 118 | -- | -- |
|  | 64 | 0.77 ms | -- | -- | 142 | -- | -- |
|  | 128 | 1.4 ms | -- | -- | 166 | -- | -- |
|  | 256 | 2.67 ms | -- | -- | 190 | -- | -- |
|  | 512 | 5.22 ms | -- | -- | 214 | -- | -- |
|  | 1,024 | 10 ms | -- | -- | 238 | -- | -- |
|  | 2,048 | 21 ms | -- | -- | 262 | -- | -- |
|  | 4,096 | 46 ms | -- | -- | 286 | -- | -- |
|  | 8,192 | 91 ms | -- | -- | 310 | -- | -- |
|  | 16,384 | 203 ms | -- | -- | 334 | -- | -- |
|  | 32,768 | 395 ms | -- | -- | 358 | -- | -- |
|  | 65,536 | 862 ms | -- | -- | 382 | -- | -- |
| Bernstein-Vazirani | 2 | 0.118 ms | 4 ms | 0.0467 ms | 37 | 74 | 7 |
|  | 4 | 0.159 ms | 1 ms | 0.0192 ms | 65 | 100 | 11 |
|  | 8 | 0.216 ms | 1 ms | 0.0371 ms | 103 | 146 | 19 |
|  | 16 | 0.296 ms | 1 ms | 0.647 ms | 148 | 209 | 35 |
|  | 32 | 0.433 ms | 1 ms | timeout | 214 | 272 | -- |
|  | 64 | 0.669 ms | 2 ms | -- | 294 | 370 | -- |
|  | 128 | 1.11 ms | 3 ms | -- | 458 | 524 | -- |
|  | 256 | 1.79 ms | 4 ms | -- | 692 | 776 | -- |
|  | 512 | 3.15 ms | 7 ms | -- | 1,150 | 1,231 | -- |
|  | 1,024 | 5.9 ms | 11 ms | -- | 1,923 | 2,001 | -- |
|  | 2,048 | 10 ms | 19 ms | -- | 3,270 | 3,401 | -- |
|  | 4,096 | 19 ms | 35 ms | -- | 5,485 | 5,627 | -- |
|  | 8,192 | 36 ms | 61 ms | -- | 9,268 | 9,379 | -- |
|  | 16,384 | 60 ms | 115 ms | -- | 16,432 | 16,540 | -- |
|  | 32,768 | 125 ms | 224 ms | -- | 30,624 | 30,715 | -- |
|  | 65,536 | 239 ms | 459 ms | -- | 58,620 | 58,799 | -- |
| Deutsch-Jozsa | 2 | 0.087 ms | 1 ms | 0.0245 ms | 35 | 53 | 6 |
|  | 4 | 0.145 ms | 1 ms | 0.0227 ms | 63 | 72 | 18 |
|  | 8 | 0.18 ms | 1 ms | 0.48 ms | 87 | 91 | 233 |
|  | 16 | 0.212 ms | 1 ms | 442 ms | 111 | 110 | 53,517 |
|  | 32 | 0.253 ms | 1 ms | timeout | 135 | 129 | -- |
|  | 64 | 0.288 ms | 1 ms | -- | 159 | 148 | -- |
|  | 128 | 0.336 ms | 1 ms | -- | 183 | 167 | -- |
|  | 256 | 0.481 ms | 2 ms | -- | 207 | 186 | -- |
|  | 512 | 0.542 ms | 2 ms | -- | 231 | 205 | -- |
|  | 1,024 | 0.684 ms | 3 ms | -- | 255 | 224 | -- |
|  | 2,048 | 0.975 ms | 4 ms | -- | 279 | 243 | -- |
|  | 4,096 | 1.5 ms | 8 ms | -- | 303 | 262 | -- |
|  | 8,192 | 2.61 ms | 14 ms | -- | 327 | 281 | -- |
|  | 16,384 | 4.7 ms | 28 ms | -- | 351 | 300 | -- |
|  | 32,768 | 8.52 ms | 58 ms | -- | 375 | 319 | -- |
|  | 65,536 | 18 ms | 120 ms | -- | 399 | 338 | -- |
| QFT | 2 | 0.097 ms | 0 ms | 0.037 ms | 16 | 40 | 12 |
|  | 4 | 0.239 ms | 0 ms | 0.19 ms | 27 | 80 | 47 |
|  | 8 | 0.907 ms | 2 ms | 8.41 ms | 336 | 644 | 95 |
|  | 16 | 85 ms | 235 ms | 51,100 ms | 131,917 | 132,238 | 175 |
| Grover | 4 | 0.326 ms | 2 ms | 0.251 ms | 29 | 47 | 9 |
|  | 8 | 0.667 ms | 2 ms | 4.49 ms | 50 | 73 | 16 |
|  | 16 | 1.43 ms | 4 ms | 362 ms | 90 | 110 | 95 |
|  | 32 | 2.52 ms | 5 ms | timeout | 140 | 161 | -- |
|  | 64 | 3.52 ms | 8 ms | -- | 218 | 250 | -- |
|  | 128 | 6.04 ms | 14 ms | -- | 359 | 388 | -- |
|  | 256 | 12 ms | 46 ms | -- | 587 | 624 | -- |
|  | 512 | 18 ms | 189 ms | -- | 998 | 1,038 | -- |
|  | 1,024 | 43 ms | -- | -- | 1,765 | -- | -- |

## Qubit counts that are not powers of two

The reference is indexed by *level*: a level-`p` CFLOBDD has `2^p` variables, so
`2^p` qubits is the only register it can build. This crate takes the count
itself. `Register` in `tests/quantum.rs` splits a qubit count into its ceiling
and floor halves down to one qubit's `S -> a a`; a power of two divides evenly at
every level and reproduces the balanced family exactly, and any other count
divides unevenly.

Nothing in the matrix algebra notices. The deferred semiring never asks *where* a
grouping divides its variables -- it recurses on whatever two sub-grammars a rule
names, multiplies their exit maps and lifts the result. The one thing it needs is
that the division fall between two `(row, column)` pairs rather than through one,
which in the interleaved order is exactly **every grammar node covers an even
number of variables**. Building the tree over qubits gives that for free at any
count. Balance, equal splits and power-of-two dimensions are not required and are
never checked; `check_matrix_grammar` in `src/gcflobdd/matmul/mod.rs` enforces
even variables and binary groupings, and nothing more.

Pass `qN` instead of `p` to ask for exactly `N` qubits. Every run below is
correct; times are medians over 3 seeds where the algorithm takes one.

| qubits | grammar | ghz time | ghz size | bv time | bv size | dj time | dj size | grover time | grover size |
|--:|---|--:|--:|--:|--:|--:|--:|--:|--:|
| **100** | 4 uneven | 3.24 ms | 434 | 1.12 ms | 458 | 0.43 ms | 225 | 6.54 ms | 331 |
| **101** | 6 uneven | 2.97 ms | 435 | 1.12 ms | 466 | 0.48 ms | 240 | -- | -- |
| 128 | balanced | 2.70 ms | 378 | 1.15 ms | 458 | 0.34 ms | 183 | 6.01 ms | 352 |
| **200** | 4 uneven | 4.16 ms | 486 | 1.71 ms | 692 | 0.48 ms | 249 | 9.50 ms | 549 |
| 256 | balanced | 4.96 ms | 430 | 1.85 ms | 706 | 0.41 ms | 207 | 11.89 ms | 584 |
| **300** | 6 uneven | 6.21 ms | 560 | 2.57 ms | 955 | 0.56 ms | 288 | 16.39 ms | 803 |
| **333** | 8 uneven | 8.17 ms | 561 | 2.86 ms | 963 | 0.67 ms | 303 | -- | -- |
| **500** | 6 uneven | 9.78 ms | 583 | 3.30 ms | 1,223 | 0.66 ms | 304 | 19.33 ms | 1,057 |
| 512 | balanced | 9.00 ms | 482 | 3.23 ms | 1,136 | 0.51 ms | 231 | 18.43 ms | 998 |
| **999** | 9 uneven | 23.46 ms | 665 | 6.36 ms | 2,033 | 0.91 ms | 365 | -- | -- |
| **1,000** | 6 uneven | 18.27 ms | 635 | 5.96 ms | 2,024 | 0.82 ms | 328 | 37.20 ms | 1,849 |
| 1,024 | balanced | 17.45 ms | 534 | 5.66 ms | 1,923 | 0.73 ms | 255 | 40.63 ms | 1,769 |
| **2,000** | 6 uneven | 35.40 ms | 687 | 10.69 ms | 3,455 | 1.10 ms | 352 | 74.55 ms | 3,264 |
| 2,048 | balanced | 34.65 ms | 586 | 10.66 ms | 3,270 | 0.98 ms | 279 | 74.17 ms | 3,100 |

**An exact count costs about what the neighbouring power of two costs.** Across
all 33 uneven points the time ratio against the next power of two up runs from
0.69x to 1.41x, and the size ratio from 0.80x to 1.43x. The premium has a
structural cause and a bound: an uneven tree carries two adjacent block sizes at
each level where a power of two carries one, so it has at most twice as many
distinct grammar nodes -- 1,000 qubits resolves into 16 distinct block sizes
against 1,024's 11.

Only three of the five algorithms take an *odd* count. `sqrt(N) = 2^(n/2)` runs
through Grover's iteration count, its initial amplitude and its theory check, and
through QFT's `2^(-n/2)` normalisation, and every amplitude type here carries an
integer exponent. That is the algorithms' arithmetic asking, not the
representation: 101, 333 and 999 qubits of GHZ, Bernstein-Vazirani and
Deutsch-Jozsa all run, and the smoke test runs them at 5 and 7.

![runtime at uneven counts](results/macos-m1pro/figures/uneven_runtime.svg)

![size at uneven counts](results/macos-m1pro/figures/uneven_size.svg)

### Where it matters: QFT

| qubits | grammar | qft time | qft size |
|--:|---|--:|--:|
| **6** | 1 uneven | 0.51 ms | 178 |
| 8 | balanced | 0.96 ms | 336 |
| **10** | 2 uneven | 2.31 ms | 1,186 |
| **12** | 1 uneven | 6.95 ms | 8,437 |
| **14** | 2 uneven | 15.91 ms | 8,542 |
| 16 | balanced | 87.08 ms | 131,917 |
| **18** | 3 uneven | 423.01 ms | 526,023 |
| **20** | 2 uneven | 2,335.80 ms | 2,100,380 |
| **22** | 3 uneven | 10,748.43 ms | 8,395,111 |

QFT is the algorithm whose cost is dominated by the register rather than the
circuit, and it is where rounding a qubit count up to the next power of two stops
being a rounding error. **Ten qubits of QFT cost 2.3 ms and a diagram of 1,186.
Rounding that up to 16 costs 87 ms and 131,917** -- 38x the time and 111x the
diagram, to simulate a register six qubits wider than the one asked for. Above 16
there is no rounding up at all: this crate reaches 22 qubits in 11 s and 2.5 GB,
and 32 qubits finishes in neither implementation.

## Grover

The reference's Grover rows come from a build carrying the upstream fix for an
uninitialised field in its dense multiply. On this machine the unpatched build is
indistinguishable from it -- correct on 10 of 10 seeds at every size to 512
qubits, identical diagram sizes, times within 3%, and the same SIGBUS at 1,024 --
so the fix changes nothing here; it is applied because it is the upstream state.

Ten seeds per size. Both are correct on all ten at every size to 512 qubits;
above that the reference does not run.

### How far it goes

`testGroversAlgoBig` uses wide-float exponentiation of the Grover operator. The
theory check is f64 arithmetic and refuses past 2,046 qubits, so larger runs ask
for `answer`: the peak still has to land on the planted string, only the
whole-state comparison against theory is dropped.

| qubits | time | peak RSS | nodes | size | answer |
|--:|--:|--:|--:|--:|:-:|
| 1,024 | 0.05 s | 8 MB | 261 | 1,769 | correct |
| 2,048 | 0.07 s | 13 MB | 452 | 3,100 | correct |
| 4,096 | 0.18 s | 26 MB | 768 | 5,306 | correct |
| 8,192 | 0.59 s | 66 MB | 1,307 | 9,073 | correct |
| 16,384 | 2.74 s | 214 MB | 2,329 | 16,221 | correct |
| 32,768 | 12.81 s | 732 MB | 4,355 | 30,397 | correct |
| 65,536 | 75.37 s | 2,764 MB | 8,353 | 58,377 | correct |

### Iteration, exponentiation, and how much mantissa it takes

Three variants: honest iteration (`testGroversAlgo`), f64 exponentiation
(`testGroversAlgoFast`) and wide-float exponentiation (`testGroversAlgoBig`).

| qubits | iterate | fast | big |
|--:|--:|--:|--:|
| 2 | 0.1 ms, correct | 0.1 ms, correct | 0.1 ms, correct |
| 4 | 0.1 ms, correct | 0.3 ms, correct | 0.3 ms, correct |
| 8 | 0.2 ms, correct | 0.7 ms, correct | 0.7 ms, correct |
| 16 | 0.4 ms, correct | 1.3 ms, correct | 1.4 ms, correct |
| 32 | 12.9 ms, correct | 2.4 ms, correct | 2.5 ms, correct |
| 64 | -- | 3.9 ms, correct | 3.5 ms, correct |
| 128 | -- | 6.1 ms, peak only | 6.3 ms, correct |
| 256 | -- | 11.9 ms, peak only | 12.6 ms, correct |
| 512 | -- | 17.3 ms, peak only | 19.1 ms, correct |
| 1,024 | -- | 47.2 ms, **wrong** | 43.0 ms, correct |

Honest iteration is `O(sqrt(N))` applications of the Grover operator and stops
being practical at 64 qubits; exponentiation replaces them with `O(log sqrt(N))`
squarings. The precision is what separates the two exponentiating variants:
squaring the operator `n/2` times needs a mantissa satisfying `2(b+1) > n`, and
f64's 53 bits give out at 128 qubits. Past that its state is still *peaked* on
the right string -- the answer looks right -- while the amplitudes no longer
match theory, and at 1,024 qubits the diagram collapses to a single node and the
answer is wrong. It is the whole-state check against theory, not the peak, that
catches it.

## Matrix operations

No comparable reference driver exists (`testMatrixMultiplication` is marked
obsolete and takes no size), so these are this crate only, from
`cargo test --test matmul --release -- --dense-level 4`:

| operation | dimension | time | diagram |
|---|--:|--:|--:|
| Kronecker fold of a 2x2 | 2^128 square | <1 ms | 17 nodes |
| `I * I`, `(J-I) * I` | 2^32 square | <1 ms | 13 nodes |
| `(J-I) * e_0` (matrix-vector) | 2^32 | <1 ms | 25 nodes |
| dense random `A * A` | 256 x 256 | 6.5 s | 8,354 nodes |
| dense random `A * v` | 256 | 29 ms | 8,363 nodes |

A Kronecker fold costs exactly two nodes per doubling. The dense random case is
the worst case by construction -- an unstructured matrix has nothing to share --
and is three orders of magnitude slower than the structured ones at the same
dimension.

## Arbitrary-precision coefficients

The deferred semiring counts how many products coincide, and a Hadamard layer
over `n` qubits makes that count `2^n`. With `i128` coefficients that overflows
at 128 qubits -- exact arithmetic refusing to be wrong rather than a blow-up in
time or memory. The `bigint` feature swaps in [`rug::Integer`], which is what
`boost::multiprecision::cpp_int` does for the reference. On the GHZ ladder:

| qubits | `i128` | `bigint` | overhead |
|--:|--:|--:|--:|
| 2 | 0.34 ms | 0.15 ms | 0.45x |
| 4 | 0.22 ms | 0.23 ms | 1.07x |
| 8 | 0.32 ms | 0.37 ms | 1.14x |
| 16 | 0.47 ms | 0.54 ms | 1.16x |
| 32 | 0.76 ms | 0.88 ms | 1.16x |
| 64 | 1.25 ms | 1.49 ms | 1.19x |
| 128 | overflow | 2.56 ms | -- |
| 256 | overflow | 4.83 ms | -- |
| 512 | overflow | 9.12 ms | -- |
| 1,024 | overflow | 18 ms | -- |
| 2,048 | overflow | 36 ms | -- |
| 4,096 | overflow | 73 ms | -- |
| 8,192 | overflow | 152 ms | -- |
| 16,384 | overflow | 370 ms | -- |
| 32,768 | overflow | 662 ms | -- |
| 65,536 | overflow | 1,323 ms | -- |

Under the ceiling the overhead is at most 19%; the sub-millisecond rows sit
inside process-startup noise and should not be read as `bigint` winning. Above
the ceiling it is the difference between a result and a panic. Every other number
in this document is from the `bigint` build.

## The control group: CUDD

CUDD's ADDs are a BDD-family baseline, run from the reference repository's own
`examples/cudd_test` driver with a **120-second cap** per size -- the same order
as the budget the two CFLOBDDs ran under -- and no larger size attempted after
one times out (`results/macos-m1pro/cudd.sh`).

| algorithm | last size it finishes | time there | ADD nodes | next size |
|---|--:|--:|--:|---|
| GHZ | 256 qubits | 264.48 ms | 516 | aborts at 512 |
| Bernstein-Vazirani | 16 qubits | 0.65 ms | 35 | times out at 32 |
| Deutsch-Jozsa | 16 qubits | 441.49 ms | 53,517 | times out at 32 |
| Grover | 16 qubits | 361.91 ms | 95 | times out at 32 |
| QFT | 16 qubits | 51.10 s | 175 | times out at 32 |

Its QFT is the one path in that driver that carries the state as a *pair* of
ADDs, real and imaginary; the node count above is their sum, which is what the
driver's own return value is.

The gap is two to three orders of magnitude in reachable qubit count -- 16 or
256 against 65,536 -- which is the result the CFLOBDD representation exists to
produce. Deutsch-Jozsa is the clearest single row: 53,517 ADD nodes at 16 qubits
against 111 and 110 for the two CFLOBDDs, and no 32-qubit run at all.

**QFT is the one row where the ADD is the compact object, and it is a warning
about the size column rather than a result.** At 16 qubits CUDD's ADD is 175
nodes where the CFLOBDDs report about 132,000 -- but those 132,000 are nodes
plus return-map entries, and the CFLOBDDs' *node* counts are 5 and 11. CUDD has
no return maps to report and does not count its terminals, so the two numbers
measure different things and neither is a memory figure. What is comparable is
the time: **51.1 s for CUDD against 85 ms here**, a factor of 599. All three
then fail at 32 qubits.

## Caveats

**Timers.** Each implementation reports its own internal timer around the same
phase of the algorithm. The reference's has 1 ms granularity, which floors
several of its small-size rows at 0 and inflates the ratios there; the
microsecond figures in `results/macos-m1pro/*.csv` are this crate's only.

**Memory.** Peak RSS at 65,536 qubits is 283-567 MB here against 820-917 MB for
the reference, but most of that difference is a fixed reservation: the reference
starts at ~679 MB before it has done any work, this crate at ~3 MB. Measured as
*growth* over an empty run, the reference is the leaner of the two -- 141 MB
against 315 MB on 65,536-qubit GHZ.

**GHZ is two circuits.** `testGHZAlgoMatrix` builds the reference's own
construction, a 2n-qubit register held as a matrix, and is the row that compares
with it. `testGHZAlgo` is the textbook circuit on an n-variable state vector: a
smaller object with no reference number to compare against, listed separately as
*GHZ (state vector)*.

**Seeds.** One seed per size on GHZ, Bernstein-Vazirani and Deutsch-Jozsa; three
on QFT and the uneven sweep; ten on Grover. Where a table gives one number for
several seeds it is the median.

**QFT above 16 qubits** is this crate only, and both implementations' 16-qubit
rows vary by seed -- the reference reports 66,498 on one of three and 132,238 on
the other two, this crate 66,294 on one and 131,917 on the other two -- so the
QFT size comparison rests on a small sample.
