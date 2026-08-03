# gcflobdd vs. the reference C++ CFLOBDD

Quantum-algorithm and matrix-operation benchmarks, measured against
[`trishullab/cflobdd`](https://github.com/trishullab/cflobdd) (unweighted build,
`../cflobdd`) on the same machine.

**Machine**: Fedora 44, Linux 7.1.4, 24 cores, 62 GB RAM.
**Rust**: `cargo build --release --test quantum` (rustc, `-C opt-level=3`).
**C++**: prebuilt `./cflobdd`, gcc 16.1.1, Boost 1.90.

Reproduce with:

```bash
scripts/compare_cflobdd.sh              # writes results/quantum_compare.csv
PMAX=4 scripts/compare_cflobdd.sh qft   # QFT only; the C++ side OOMs past 16 qubits
cargo test --test matmul --release      # matrix operations (this crate only)
```

## TL;DR

**This crate is faster on every algorithm at every size both implementations
run**, by 1x to 25x depending on algorithm and size, and its diagrams are 2x to
460x smaller. It is *not* a uniform win: the reference reaches 256 qubits on
Bernstein-Vazirani and Deutsch-Jozsa where this crate stops at 64, because its
path coefficients are arbitrary-precision integers and ours are `i128`.

| | this crate | reference C++ |
|---|---|---|
| GHZ, 256 qubits | 7.1 ms | 7.0 ms |
| Bernstein-Vazirani, 64 qubits | **0.9 ms** | 2 ms |
| Deutsch-Jozsa, 64 qubits | **0.4 ms** | 2 ms |
| QFT, 16 qubits | **58 ms**, 287 nodes+edges | 190 ms, **132,238** nodes+edges |
| max qubits, BV / DJ | 64 | **256** |
| max qubits, QFT | 16 (32 does not finish) | 16 (32 exhausts memory) |
| process startup | ~1 ms, 5 MB RSS | ~350 ms, 866 MB RSS |

## Results

Times are the median over seeds of each implementation's *own* internal timer,
around the same phase of the algorithm. Size is nodes+edges of the result
diagram (`CountNodesAndEdges` there, `count_nodes_and_edges` here).

### GHZ

| qubits | rust | c++ | ratio | rust size | c++ size |
|--:|--:|--:|--:|--:|--:|
| 4 | 90 us | 2 ms | 22x | 20 | 130 |
| 16 | 326 us | 2 ms | 6.1x | 48 | 242 |
| 64 | 2.1 ms | 3 ms | 1.4x | 76 | 354 |
| 256 | 7.1 ms | 7 ms | 1.0x | 104 | 466 |

Both are logarithmic in the qubit count; the reference gains 56 nodes+edges per
doubling, this crate 14. The time advantage narrows to parity at 256 qubits
because the two do different work here (see *Caveats*).

### Bernstein-Vazirani

| qubits | rust | c++ | ratio | rust size | c++ size |
|--:|--:|--:|--:|--:|--:|
| 4 | 104 us | 1 ms | 9.6x | 29 | 100 |
| 16 | 287 us | 2 ms | 7.0x | 77 | 209 |
| 64 | 948 us | 2 ms | 2.1x | 161 | 370 |
| 128 | **overflow** | 3 ms | - | - | 524 |
| 256 | **overflow** | 4 ms | - | - | 776 |

### Deutsch-Jozsa

| qubits | rust | c++ | ratio | rust size | c++ size |
|--:|--:|--:|--:|--:|--:|
| 4 | 120 us | 2 ms | 16.7x | 29 | 70 |
| 16 | 252 us | 2 ms | 7.9x | 57 | 108 |
| 64 | 367 us | 2 ms | 5.4x | 85 | 146 |
| 128 | **overflow** | 2 ms | - | - | 165 |
| 256 | **overflow** | 2 ms | - | - | 184 |

### QFT

| qubits | rust | c++ | ratio | rust size | c++ size |
|--:|--:|--:|--:|--:|--:|
| 4 | 270 us | <1 ms | - | 9 | 80 |
| 8 | 1.0 ms | 2 ms | 1.9x | 29 | 644 |
| 16 | 58 ms | 190 ms | 3.3x | **287** | **132,238** |
| 32 | does not finish | exhausts memory (9.3 GB) | - | - | - |

QFT is where the two diverge most. The reference's diagram explodes -- 644
nodes+edges at 8 qubits, 132,238 at 16, out of memory at 32 -- while this
crate's stays at 287. Both fail at 32 qubits, for different reasons: the
reference on memory, this crate on time.

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
to share -- and is 3 orders of magnitude slower than the structured ones at the
same dimension.

## What made the difference

Three changes came out of profiling this comparison; the first two are in the
library and are why the numbers above are what they are.

1. **Hashed value interning** (`ValueSet` in `src/gcflobdd/matmul/mod.rs`).
   Collapsing equal exit values used a linear scan, which is quadratic in the
   exit count -- fine for boolean work, ruinous for a Fourier-transformed state
   with thousands of distinct amplitudes. `perf` put 97.6% of QFT in
   `substitute`/`collapse`. Hashing values (via `MatMulValue::dedup_key`) with a
   scan retained below 16 entries took **QFT at 16 qubits from 2,341 ms to
   65 ms, 31x**, and left the small-exit cases unchanged.

2. **Cached identity operators** (`Ops` in `tests/quantum.rs`). Building a gate
   at qubit `i` needs an identity for every subtree without a gate, and
   rebuilding those towers dominated the gate-heavy algorithms: 22% of GHZ was
   `add_gcflobdd_node`, 20% `add_return_map`. Building each level's identity
   once took **GHZ at 256 qubits from 45.2 ms to 7.3 ms, 6.2x**.

3. **`i128` path coefficients** (`src/gcflobdd/matmul/map.rs`). `i64` overflowed
   at 64 qubits, which is where the deferred semiring's coefficients reach
   `2^n`. Widening doubled the reachable size and cost ~13% on the dense matrix
   multiply. The reference uses `boost::multiprecision::cpp_int` here, which is
   why it keeps going to 256.

## Caveats

These matter for reading the numbers honestly.

- **The C++ timer has millisecond granularity.** Every reference figure below
  ~5 ms is a rounded 1-4 ms, so the small-size ratios (the 22x and 16x rows) are
  real but imprecise. This crate reports microseconds (`durationUs`).
- **The circuits are not identical.** GHZ here is the textbook circuit -- H then
  a CNOT chain over `n` qubits, applied gate by gate; the reference multiplies
  `n` CNOT matrices together over a `2n`-qubit register and applies the product.
  Same state, different work, and the reason the GHZ ratio decays to 1.0x.
  BV and DJ *do* follow the reference's structure, including building the oracle
  outside the timed region, as it does.
- **The value types differ.** `f64` and a hand-rolled `C64` here;
  `cpp_dec_float` and `cpp_complex_100` there. That favours this crate on
  arithmetic. It is a small part of the profile -- CFLOBDD defers arithmetic to
  the top node -- and it does not explain the QFT diagram-size gap, which is
  structural. But at 256 qubits the reference is carrying 100-digit floats and
  this crate is not.
- **A state stays a vector here.** `mk_matvec` keeps a state at `n` variables;
  the reference pads it into a `2n`-variable matrix and uses matrix multiply.
  That is a genuine advantage of this crate's API, not a measurement artifact,
  but it is doing less work per gate as a result.
- **The reference's QFT is unchecked.** Its harness prints no correctness line
  for QFT (`correct=na`). This crate's QFT is verified against
  `exp(2 pi i s k / 2^n) / sqrt(2^n)` at every size run.
- **One reference harness bug was hit**: `testBVAlgo` samples until it draws a
  non-zero string, so a seed whose secret is all zeros never terminates (seed 3
  at 2 qubits, 120 s timeout). BV rows use seed 1.

## The scaling limit

Bernstein-Vazirani and Deutsch-Jozsa panic with `matmul coefficient overflow`
at 128 qubits. This is not a blow-up in time or memory -- it is exact
arithmetic refusing to be wrong.

The deferred semiring records how many products coincide, and a Hadamard layer
over `n` qubits makes that count `2^n`: `2^127` is the last one an `i128`
holds. The reference sidesteps this with `cpp_int`, at the cost of a heap
allocation per coefficient.

Closing that gap means a coefficient type that starts small and promotes to a
bignum on overflow. That would extend BV/DJ to any size at a small cost on
everything else -- the natural next step, and the one thing keeping this from
being a clean sweep.
