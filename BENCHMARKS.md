# Evaluation

GCFLOBDD is a decision diagram whose variable order is given by a *grammar*
rather than a list, so a function's structure can be shared at every scale
instead of only between subtrees. This measures what that buys, on two
workloads with independent baselines.

| workload | baselines |
|---|---|
| Quantum circuits and matrix algebra | reference C++ CFLOBDD ([`trishullab/cflobdd`](https://github.com/trishullab/cflobdd), unweighted build); CUDD 3.0.0 ADDs |
| Network verification: atomic predicates and all-pairs reachability | NDD ([XJTU-NetVerify/NDD](https://github.com/XJTU-NetVerify/NDD), NSDI '25); JDD BDD |

**Machine.** Apple M1 Pro, 10 cores, 32 GB, macOS 26.5.2 (Darwin 25.5.0). Runs
are strictly sequential; nothing else was on the machine.

**Builds.** rustc 1.95.0. Circuits: `cargo build --release --test quantum
--features bigint`. Network: `cargo build --release` in `gcflobdd-jni` (no
`rug`, no GMP). Apple clang 21.0.0 with Boost 1.89.0; CUDD 3.0.0
(`--enable-obj`); OpenJDK 24.0.1 with `-Xmx12g`; NDD at `c8414b4`, its core
recompiled from source rather than from its prebuilt jar, which predates NDD's
int-handle rewrite.

Raw runs, scripts and figures are in [`results/macos-m1pro/`](results/macos-m1pro);
the network harness is [`bench/netverify/`](bench/netverify).

```bash
# circuits, 2 to 65,536 qubits, both implementations
LABEL=rust-bigint ONLY=rust RUST_BIN=<bigint build> PMAX=16 SEEDS=1 \
  OUT=results/ladder.csv scripts/compare_cflobdd.sh ghz bv dj
ONLY=cpp PMAX=16 SEEDS=1 APPEND=1 OUT=results/ladder.csv scripts/compare_cflobdd.sh ghz bv dj
PMAX=4 scripts/compare_cflobdd.sh qft
LABEL=rust-bigint ONLY=rust RUST_BIN=<bigint build> PMIN=2 PMAX=10 GROVER_PMAX=10 \
  SEEDS="1 2 3 4 5 6 7 8 9 10" OUT=results/grover_compare.csv \
  scripts/compare_cflobdd.sh grover
BIN=<bigint build> OUT=results/uneven.csv results/macos-m1pro/uneven.sh   # uneven counts
CUDD=<cudd_test build> OUT=results/cudd.csv results/macos-m1pro/cudd.sh   # control group
cargo test --test matmul --release -- --dense-level 4                     # matrix algebra

# network verification
NDD_REPO=/path/to/NDD bench/netverify/build.sh
KS="4 6 8 10 12" SEEDS="1 2 3" OUT=results/netverify.csv scripts/compare_netverify.sh
python3 results/macos-m1pro/build_tables.py            # -> all_results.csv
python3 results/macos-m1pro/build_netverify_tables.py  # -> netverify_all.csv
python3 results/macos-m1pro/plot.py && python3 results/macos-m1pro/plot_netverify.py
```

## Reading the size column

Every `size` here is **nodes + edges**, counted the same way on both sides: two
edges per connection plus the entries of every distinct return map --
`count_nodes_and_edges` here, `CountNodesAndEdges` in the reference. Node counts
alone are convention-free and agree.

Return-map entries dominate that total whenever a diagram has many exits. A
16-qubit QFT is 5 nodes and 282 connections, but its 131,917 is 5 nodes, 564
edges and **131,348 return-map entries -- 99.6% of it**; the reference's own
132,238 for the same circuit is within 0.3%. CUDD has no return maps and does
not count terminals, so its node column measures a different thing and is not a
memory figure.

## Quantum circuits

One variant per algorithm: the fastest shape this crate can build, at the
precision that reaches the largest register. Grover is wide-float operator
exponentiation; every amplitude is from the `bigint` build, which has no qubit
ceiling.

**The two implementations do not build the same GHZ, and the row compares each
one's own construction.** This crate runs the textbook circuit -- a Hadamard and
a CNOT chain applied to an n-qubit *state vector*. The reference has no
state-vector path: `QuantumAlgos::GHZ` works at `2n` qubits, builds a product of
n CNOTs onto one shared target as a *matrix*, multiplies it into a tensored
basis state and applies a Walsh layer over all `2n` qubits. What comes out
carries the GHZ state inside an operator whose remaining row and column bits are
free -- a strictly larger object, which is why its size column settles at
about 2.4x this crate's from 8 qubits up rather than tracking it the way the
other algorithms do.
The reference's construction is not a worse implementation of the same thing;
it is a different thing. The GHZ row below is each implementation at what it
actually does, which makes its two size columns the only pair in the table not
measuring the same object.

| algorithm | qubits | gcflobdd | reference | speed-up | gcflobdd size | reference size |
|---|--:|--:|--:|--:|--:|--:|
| **GHZ** | 65,536 | **862 ms** | 1,737 ms | **2.0x** | 382 | 914 |
| **Bernstein-Vazirani** | 65,536 | **239 ms** | 459 ms | **1.9x** | 58,620 | 58,799 |
| **Deutsch-Jozsa** | 65,536 | **18 ms** | 120 ms | **6.8x** | 399 | 338 |
| **QFT** | 16 | **85 ms** | 235 ms | **2.8x** | 131,917 | 132,238 |
| **Grover** | 512 | **18 ms** | 189 ms | **10.2x** | 998 | 1,038 |

**On the four algorithms where both build the same object the diagrams are the
same size** -- within 4% on three of them, and on Deutsch-Jozsa this crate's is
18% *larger*. The representations agree; what differs is the time to build
them.

**Faster at every algorithm and every size measured**, never by less than 1.25x.
The ratios at small sizes are wider still, up to 34x, but there the reference's
1 ms timer granularity flatters the comparison, so the figures above are the
honest ones. Every run in the table finishes inside 3.5 s wall clock.

**Reach.** Both run Bernstein-Vazirani and Deutsch-Jozsa correctly from 2 to
65,536 qubits. On Grover the reference dies with SIGBUS at 1,024 qubits, where
this crate is still verified against theory; under a weaker check -- the peak
must land on the planted string, without the whole-state comparison -- it answers
correctly to 65,536 qubits in 75 s and 2.8 GB. CUDD reaches 16 qubits on three of
the five and 256 on GHZ. No implementation finishes 32-qubit QFT.

![runtime](results/macos-m1pro/figures/runtime_all.svg)

![diagram size](results/macos-m1pro/figures/size_all.svg)

**The control group is two to three orders of magnitude short in reachable
qubits.** Deutsch-Jozsa is the clearest row: 53,517 ADD nodes at 16 qubits
against 111 here, 442 ms against 0.212 ms, and no 32-qubit run at all. QFT is the
one place the ADD is the compact object, and it is a warning about the size
column rather than a result -- 175 ADD nodes against ~132,000, of which 99.6% are
return-map entries; what is comparable there is the time, **51.1 s against
85 ms**.

| algorithm | qubits | gcflobdd | reference | CUDD | gcflobdd size | reference size | CUDD nodes |
|---|--:|--:|--:|--:|--:|--:|--:|
| GHZ | 2 | 0.067 ms | 2 ms | 0.465 ms | 14 | 81 | 8 |
|  | 4 | 0.102 ms | 1 ms | 0.161 ms | 46 | 130 | 12 |
|  | 8 | 0.171 ms | 1 ms | 0.227 ms | 70 | 186 | 20 |
|  | 16 | 0.326 ms | 2 ms | 0.448 ms | 94 | 242 | 36 |
|  | 32 | 0.424 ms | 2 ms | 1.51 ms | 118 | 298 | 68 |
|  | 64 | 0.77 ms | 3 ms | 6.4 ms | 142 | 354 | 132 |
|  | 128 | 1.4 ms | 5 ms | 40 ms | 166 | 410 | 260 |
|  | 256 | 2.67 ms | 8 ms | 264 ms | 190 | 466 | 516 |
|  | 512 | 5.22 ms | 13 ms | aborts | 214 | 522 | -- |
|  | 1,024 | 10 ms | 24 ms | -- | 238 | 578 | -- |
|  | 2,048 | 21 ms | 47 ms | -- | 262 | 634 | -- |
|  | 4,096 | 46 ms | 94 ms | -- | 286 | 690 | -- |
|  | 8,192 | 91 ms | 197 ms | -- | 310 | 746 | -- |
|  | 16,384 | 203 ms | 408 ms | -- | 334 | 802 | -- |
|  | 32,768 | 395 ms | 851 ms | -- | 358 | 858 | -- |
|  | 65,536 | 862 ms | 1,737 ms | -- | 382 | 914 | -- |
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

## Registers of any width

The reference is indexed by *level*: a level-`p` CFLOBDD has `2^p` variables, so
`2^p` qubits is the only register it can build. This crate takes the count
itself. `Register` in `tests/quantum.rs` splits a qubit count into its ceiling
and floor halves down to one qubit's `S -> a a`; a power of two divides evenly at
every level, any other count divides unevenly, and nothing in the matrix algebra
notices. The deferred semiring never asks *where* a grouping divides its
variables. The one thing it needs is that the division fall between two
`(row, column)` pairs rather than through one -- in the interleaved order,
exactly *every grammar node covers an even number of variables*, which building
the tree over qubits gives for free at any count.

Pass `qN` instead of `p` to ask for exactly `N` qubits. Every run below is
correct; times are medians over 3 seeds where the algorithm takes one.

| qubits | grammar | ghz | ghz size | bv | bv size | dj | dj size | grover | grover size |
|--:|---|--:|--:|--:|--:|--:|--:|--:|--:|
| **100** | 4 uneven | 1.37 ms | 208 | 1.12 ms | 458 | 0.43 ms | 225 | 6.54 ms | 331 |
| **101** | 6 uneven | 1.49 ms | 209 | 1.12 ms | 466 | 0.48 ms | 240 | -- | -- |
| 128 | balanced | 1.60 ms | 166 | 1.15 ms | 458 | 0.34 ms | 183 | 6.01 ms | 352 |
| **200** | 4 uneven | 2.39 ms | 232 | 1.71 ms | 692 | 0.48 ms | 249 | 9.50 ms | 549 |
| 256 | balanced | 3.03 ms | 190 | 1.85 ms | 706 | 0.41 ms | 207 | 11.89 ms | 584 |
| **300** | 6 uneven | 4.07 ms | 271 | 2.57 ms | 955 | 0.56 ms | 288 | 16.39 ms | 803 |
| **333** | 8 uneven | 4.79 ms | 272 | 2.86 ms | 963 | 0.67 ms | 303 | -- | -- |
| **500** | 6 uneven | 6.05 ms | 287 | 3.30 ms | 1,223 | 0.66 ms | 304 | 19.33 ms | 1,057 |
| 512 | balanced | 5.19 ms | 214 | 3.23 ms | 1,136 | 0.51 ms | 231 | 18.43 ms | 998 |
| **999** | 9 uneven | 14.07 ms | 341 | 6.36 ms | 2,033 | 0.91 ms | 365 | -- | -- |
| **1,000** | 6 uneven | 12.03 ms | 311 | 5.96 ms | 2,024 | 0.82 ms | 328 | 37.20 ms | 1,849 |
| 1,024 | balanced | 11.62 ms | 238 | 5.66 ms | 1,923 | 0.73 ms | 255 | 40.63 ms | 1,769 |
| **2,000** | 6 uneven | 23.53 ms | 335 | 10.69 ms | 3,455 | 1.10 ms | 352 | 74.55 ms | 3,264 |
| 2,048 | balanced | 22.96 ms | 262 | 10.66 ms | 3,270 | 0.98 ms | 279 | 74.17 ms | 3,100 |

**An exact count costs about what the neighbouring power of two costs.** Across
all 33 uneven points the time ratio against the next power of two up runs from
0.78x to 1.41x and the size ratio from 0.80x to 1.43x. The premium has a
structural cause and a bound: an uneven tree carries two adjacent block sizes at
each level where a power of two carries one, so it has at most twice as many
distinct grammar nodes -- 1,000 qubits resolves into 16 distinct block sizes
against 1,024's 11.

Only three of the five algorithms take an *odd* count: `sqrt(N) = 2^(n/2)` runs
through Grover's iteration count and QFT's normalisation, and every amplitude
type here carries an integer exponent. That is the algorithms' arithmetic
asking, not the representation.

![runtime at uneven counts](results/macos-m1pro/figures/uneven_runtime.svg)

![size at uneven counts](results/macos-m1pro/figures/uneven_size.svg)

### Where it matters: QFT

| qubits | grammar | time | size |
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

QFT's cost is dominated by the register rather than the circuit, and it is where
rounding a qubit count up stops being a rounding error. **Ten qubits of QFT cost
2.3 ms and a diagram of 1,186; rounding that up to 16 costs 87 ms and 131,917**
-- 38x the time and 111x the diagram, to simulate a register six qubits wider
than the one asked for. Above 16 there is no rounding up available at all.

## Matrix operations

No comparable reference driver exists (`testMatrixMultiplication` is marked
obsolete and takes no size), so these are this crate only:

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

## Network verification

The second workload is the one NDD was built for: atomic predicates and
all-pairs reachability over a data plane. NDD ships no runnable network
benchmark -- its WAN driver hardcodes an absolute path on the authors' machine,
is excluded from its own Maven build, and no dataset is included -- so the data
plane is generated here: a k-ary fat tree with longest-prefix-match FIBs and
ClassBench-shaped ACLs, from a fixed seed. All five engines run the identical
driver over byte-identical inputs.

Three GCFLOBDD grammars over NDD's 104-bit five-tuple, all keeping the same leaf
order (srcip 32, dstip 32, srcport 16, dstport 16, proto 8):

| id | grammar |
|---|---|
| `field-grouped` | `S -> BDD(32) BDD(32) BDD(16) BDD(16) BDD(8)` -- NDD's own decomposition, expressed as a grammar |
| `aligned-balanced` | `S -> A B; A -> SI32 DI32; B -> SP16 C; C -> DP16 PR8`, each field its own binary tree down to single bits |
| `aligned-balanced-shared` | the same tree, but one `W32`/`W16`/`W8`/`W4`/`W2` shared by every field of that width |

All five engines agree on every derived quantity -- predicate count, atom count,
reachable pairs, and satisfying-assignment fractions -- at every size and seed
(`build_netverify_tables.py` re-checks this on every run).

### Runtime

Median of three seeds, whole run: forwarding predicates, ACL predicates, atomic
predicates, all-pairs reachability.

| k | BDD (JDD) | NDD | field-grouped | aligned-balanced | aligned-balanced-shared |
|--:|--:|--:|--:|--:|--:|
| 4 | **43 ms** | 50 ms | 76 ms | 48 ms | 49 ms |
| 6 | 145 ms | **129 ms** | 380 ms | 214 ms | 211 ms |
| 8 | 456 ms | **282 ms** | 1,265 ms | 661 ms | 652 ms |
| 10 | 1,403 ms | **643 ms** | 4,642 ms | 2,786 ms | 2,813 ms |
| 12 | 5,213 ms | **1,852 ms** | 14,876 ms | 9,534 ms | 9,676 ms |

![runtime](results/macos-m1pro/figures/netverify_runtime.svg)

**NDD's claim reproduces** on an independent harness: level with the BDD at k=4,
then 1.6x at k=8, 2.2x at k=10, 2.8x at k=12.

**GCFLOBDD is slower here by a steady factor.** The best grammar is level with
the BDD at k=4 and sits between 1.4x and 2.0x its time from k=6 up. The ratio has
no trend in k, so nothing is diverging -- it is a constant factor, not a scaling
problem. Against NDD it is 5.2x at k=12.

### Diagram size

Nodes in the largest single predicate built during the run. This is where the
representation, rather than its constant factor, shows up.

| k | BDD (JDD) | field-grouped | aligned-balanced | aligned-balanced-shared |
|--:|--:|--:|--:|--:|
| 4 | 337 | **18** | 137 | 88 |
| 6 | 361 | **19** | 136 | 91 |
| 8 | 429 | **20** | 158 | 107 |
| 10 | 383 | **20** | 154 | 104 |
| 12 | 399 | **21** | 172 | 115 |

![diagram size](results/macos-m1pro/figures/netverify_size.svg)

**Every GCFLOBDD grammar is smaller than the BDD, and the field-grouped one is
19x smaller** -- 21 nodes against 399, essentially flat (18 to 21) while the
predicate count grows 15x and the atom count 149x. One caveat: a GCFLOBDD node
is not a BDD node, and `field-grouped`'s 21 nodes each *contain* an ordinary BDD
over a whole 32- or 16-bit field. The aligned-balanced grammars recurse to single
bits and are the closer comparison: **115 against 399, 3.5x smaller**, with no
such caveat.

**Width-sharing pays, consistently and for free.** Sharing `W16` across the two
port fields and `W8` across the protocol and every byte of the others takes the
largest diagram from 172 nodes to 115 -- **a 32-36% reduction at every size**,
for a grammar of the same shape and the same field boundaries, at a runtime that
coincides within 2.1%.

### Engine-wide size and memory

Live nodes across the whole engine, and peak resident memory. NDD's size is NDD
nodes *plus* the label BDDs underneath. JDD keeps its live count private.

| k | NDD nodes+labels | field-grouped | aligned-balanced-shared | RSS: BDD | NDD | GCFLOBDD-abs |
|--:|--:|--:|--:|--:|--:|--:|
| 4 | 35,409 | **8,742** | 19,454 | 196 MB | 205 MB | **69 MB** |
| 6 | 71,938 | **31,641** | 80,741 | 206 MB | 220 MB | **131 MB** |
| 8 | 129,922 | **100,537** | 271,182 | **233 MB** | 243 MB | 319 MB |
| 10 | **255,808** | 273,055 | 863,278 | **457 MB** | 482 MB | 972 MB |
| 12 | **608,864** | 644,408 | 2,315,926 | 835 MB | **764 MB** | 3,132 MB |

![peak memory](results/macos-m1pro/figures/netverify_memory.svg)

Field-grouped lands within 6% of NDD's node count at k=12 -- unsurprising, since
it *is* NDD's decomposition under a different algebra. The memory picture
inverts with scale: GCFLOBDD starts at a third of the others, because its nodes
live outside the Java heap and the JVM's floor dominates the Java engines, and
ends at 3.7x the BDD's. Every row carries a JVM, so that column is comparable
between rows, not against a native process.

### How the work scales with the network

The operation count is engine-independent -- every backend executes the
identical sequence -- so one instrumented run per dataset fixes it for all five.
Nine rungs, k=4 to k=20:

| k | devices | FIB rules | ACL rules | predicates | atoms | total ops |
|--:|--:|--:|--:|--:|--:|--:|
| 4 | 20 | 180 | 72 | 60 | 506 | 11,443 |
| 8 | 80 | 840 | 288 | 341 | 7,821 | 188,667 |
| 12 | 180 | 2,460 | 648 | 892 | 75,408 | 2,527,751 |
| 16 | 320 | 5,520 | 1,152 | 1,768 | 463,680 | 21,175,281 |
| 20 | 500 | 10,500 | 1,800 | 3,080 | 2,007,704 | 140,465,058 |

The inputs are exact, not fitted: a k-ary fat tree has `5k^2/4` devices,
`5k^3/4 + 25k = Theta(D^1.5)` FIB rules and `9k^2/2 = 3.6D` ACL rules, and every
row matches those closed forms. What is measured, as a log-log fit over
D = 20..500: distinct predicates `Theta(D^1.21)` (R^2 0.9998), build operations
`Theta(D^1.12)` (0.9986), **atomic-predicate operations `Theta(D^3.3)`**
(0.9933).

That exponent is a property of the workload, not of any engine, and it decides
what an engine is worth. **On a `D^3.3` curve a constant factor buys very
little reach**: NDD's 5x per-operation advantage over GCFLOBDD is `5^(1/3.3)` in
network size -- **1.6x more devices** before hitting the same wall. Its 3x over
a plain BDD is 1.4x.

The exponent is also not settled. The rung-to-rung slope climbs from `D^3.2`
early to `D^4.1` at the top, because the atom count itself varies up to 1.86x
with the seed. The atom trajectory says why: for the first 90% of the predicate
sequence the atom count is *linear* in predicates processed, then multiplies by
390x in the last 10%. That break is exactly where the forwarding predicates end
and the ACLs begin. Longest-prefix-match predicates are sets of IP prefixes, and
prefixes are a **laminar family** -- any two are nested or disjoint -- so n of
them induce at most 2n-1 atoms. ACL predicates are five-tuple boxes that cut
across the destination-prefix nesting, and each one splits a large fraction of
the atoms already there. Read the exponent as "between 3 and 4 and drifting
upward", not as a constant.

## Caveats and what is not covered

**Timers.** Each implementation reports its own internal timer around the same
phase. The reference's has 1 ms granularity, which floors several small-size
rows at 0 and inflates the ratios there; the microsecond figures in
`results/macos-m1pro/*.csv` are this crate's only.

**Memory on circuits.** Peak RSS at 65,536 qubits is 283-567 MB here against
820-917 MB for the reference, but most of that is a fixed reservation -- the
reference starts at ~679 MB before doing any work, this crate at ~3 MB. Measured
as *growth* over an empty run the reference is the leaner of the two, 141 MB
against 315 MB on 65,536-qubit GHZ.

**Seeds.** One seed per size on GHZ, Bernstein-Vazirani and Deutsch-Jozsa in the
qubit ladder; three on QFT, the uneven sweep and the network ladder; ten on
Grover. Where a table gives one number for several seeds it is the median. GHZ
takes no seed, so where it carries three they are repeated runs.

**QFT above 16 qubits** is this crate only, and both implementations' 16-qubit
rows vary by seed -- the reference reports 66,498 on one of three and 132,238 on
the other two, this crate 66,294 on one and 131,917 on the other two -- so that
size comparison rests on a small sample.

**Precision.** Grover's whole-state check against theory is f64 arithmetic and
refuses past 2,046 qubits; beyond that the check is that the peak lands on the
planted string. f64 operator exponentiation is itself wrong above 128 qubits --
squaring the operator `n/2` times needs a mantissa satisfying `2(b+1) > n` -- and
at 1,024 qubits its diagram collapses to a single node while still *looking*
peaked. That is why the reported variant is the wide-float one.

**Not covered.** `exists`/`restrict` are not implemented, so NAT rewriting is
out of reach; nothing in the atomic-predicate or reachability kernels needs
them. Real operator data (Purdue, Stanford, Internet2) ships with neither
project. Incremental, rule-at-a-time verification is the natural next workload;
these are batch runs.
