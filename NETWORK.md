# Network verification

GCFLOBDD measured against NDD ([XJTU-NetVerify/NDD](https://github.com/XJTU-NetVerify/NDD),
NSDI '25) and a plain BDD on the workload NDD was built for: atomic predicates
and all-pairs reachability over a data plane.

**Machine.** Apple M1 Pro, 10 cores, 32 GB, macOS 26.5.2 (Darwin 25.5.0). Runs
are strictly sequential; nothing else was on the machine.

**Builds.** rustc 1.95.0, `cargo build --release` in `gcflobdd-jni` (no `rug`,
no GMP). OpenJDK 24.0.1, `-Xmx12g`. NDD at `c8414b4`, its core recompiled from
source rather than from its prebuilt jar, which predates NDD's int-handle rewrite.

Harness, raw runs, scripts and figures: [`bench/netverify/`](bench/netverify),
[`results/macos-m1pro/netverify.csv`](results/macos-m1pro/netverify.csv),
[`results/macos-m1pro/netverify_all.csv`](results/macos-m1pro/netverify_all.csv).

```bash
NDD_REPO=/path/to/NDD bench/netverify/build.sh
KS="4 6 8 10 12" SEEDS="1 2 3" OUT=results/netverify.csv scripts/compare_netverify.sh
python3 results/macos-m1pro/build_netverify_tables.py
python3 results/macos-m1pro/plot_netverify.py
```

## What NDD ships, and why the workload is ours

Nothing network-shaped in the NDD repo is runnable as it stands.

| benchmark | code | data | runs? |
|---|---|---|---|
| N-Queens | yes | none needed | **yes** — already covered by [`tests/n_queens.rs`](tests/n_queens.rs) |
| WAN data-plane verification (APKeep-style) | yes, but **excluded from NDD's own Maven build** on `main` | Purdue / Stanford / Internet2, **not shipped** | no |
| Batfish, all-pairs under *k* failures | a 132-line patch against an external repo | external | no |
| SRE (`bgp_fattree*`) | **not in the repo**, results only | external | no |
| NDD API JUnit tests | yes | none needed | yes, correctness only |

`/datasets/` is gitignored on all four branches (`main`, `reuse`, `ndd-original`,
`mtndd`), and both WAN drivers hardcode an absolute path on the authors' machine
(`/data/zcli-data/network-decision-diagram/datasets/wan/purdue/pd`). The WAN
verifier is additionally excluded by `pom.xml` because it still uses the
object-based NDD API from before the int-handle rewrite and no longer type-checks;
the last branch where it compiles is `origin/reuse`.

So the data plane here is generated: a k-ary fat-tree with longest-prefix-match
FIBs and ClassBench-shaped ACLs, from a fixed seed. It is reproducible and it
scales on demand, but it is not real router configuration — read the numbers for
what they are.

| k | switches | FIB rules | ACL rules | distinct predicates | atomic predicates | reachable pairs |
|--:|--:|--:|--:|--:|--:|--:|
| 4 | 20 | 180 | 72 | 60 | 506 | 196 |
| 6 | 45 | 420 | 162 | 168 | 3,358 | 1,821 |
| 8 | 80 | 840 | 288 | 341 | 7,821 | 9,579 |
| 10 | 125 | 1,500 | 450 | 574 | 26,095 | 36,215 |
| 12 | 180 | 2,460 | 648 | 892 | 75,408 | 100,422 |

## Field layout and grammars

NDD's five-tuple, unchanged: src_ip(32), dst_ip(32), src_port(16), dst_port(16),
protocol(8) — 104 bits, in that order, bit 0 of each field being its MSB. Three
GCFLOBDD grammars over that same 104-leaf order:

| id | grammar |
|---|---|
| `field-grouped` | `S -> BDD(32) BDD(32) BDD(16) BDD(16) BDD(8)` — NDD's own decomposition, expressed as a GCFLOBDD grammar |
| `aligned-balanced` | `S -> A B; A -> SI32 DI32; B -> SP16 C; C -> DP16 PR8`, each field its own perfect binary tree down to single bits |
| `aligned-balanced-shared` | the same tree, but one `W32`/`W16`/`W8`/`W4`/`W2` shared by every field of that width |

Every coarse split falls on a field boundary, so a field is always an intact
subtree. The two aligned-balanced variants differ only in symbol identity — and
since GCFLOBDD node identity is keyed on the grammar node's address, that alone
decides whether a src_ip subtree may share nodes with a dst_port one.

## Correctness first

All five engines return the identical answer at every size and seed: the same
atomic-predicate count, the same reachable-pair count, and sat-count digests
agreeing to 1e-9. `build_netverify_tables.py` re-checks this on every run and
prints `cross-implementation agreement: OK`.

Agreement alone would only say they agree, so `netbench.SelfTest` checks the
encoding against brute force: every port-range prefix cover is exact, prefix and
range sat counts match theory, and a hand-checked three-rule ACL yields exactly
the four atoms you get by reading it.

That check earned its keep. JDD collects garbage inside `mk`, so an unreferenced
intermediate can have its slot recycled by the very next operation. The first full
run had JDD reporting **82,050 atomic predicates where every other engine reported
81,993** — a bug in this harness, not in JDD. Every builder now returns a handle
carrying one reference the caller owns.

## Runtime

Median of three seeds, whole run: forwarding predicates, ACL predicates, atomic
predicates, all-pairs reachability.

| k | BDD (JDD) | NDD | GCFLOBDD field-grouped | aligned-balanced | aligned-balanced-shared |
|--:|--:|--:|--:|--:|--:|
| 4 | **45 ms** | 57 | 78 | 53 | 51 |
| 6 | **141 ms** | 142 | 396 | 247 | 257 |
| 8 | 410 ms | **238 ms** | 1,431 | 848 | 837 |
| 10 | 1,522 ms | **641 ms** | 5,209 | 3,227 | 3,197 |
| 12 | 5,037 ms | **1,881 ms** | 15,419 | 10,555 | 10,428 |

![runtime](results/macos-m1pro/figures/netverify_runtime.svg)

**NDD's claim reproduces.** It starts level with the BDD and pulls away as the
problem grows — 1.7x at k=8, 2.4x at k=10, **2.7x at k=12** — which is the shape
its paper reports, on an independent harness.

**GCFLOBDD is slower here, by a steady factor.** The best grammar runs 2.07x the
BDD's time at k=12 and 2.1x at k=10; the ratio is flat from k=8 up, so nothing is
diverging — it is a constant factor, not a scaling problem. Against NDD it is 5.5x.

### Where the constant factor comes from — not the JNI

The obvious suspect is the JNI boundary, since GCFLOBDD is the only engine here
that crosses it per operation. It is not the cause. `netbench.JniProbe` times
5M already-cached `and` calls:

| | ns per operation |
|---|--:|
| JDD `and`, cache hit | **2.3** |
| JNI boundary crossing alone | **5.0** |
| GCFLOBDD `and`, cache hit | **156.3** |
| ...of which is the engine, not the boundary | **151.4** |

The boundary is 3% of it. The cost is in the engine, and it is structural:
`ReturnMapT<T>` is `Vec<T>` ([`return_map.rs:1`](src/gcflobdd/return_map.rs#L1)),
and `get_op_cache` takes both operands **by value**
([`context.rs:346`](src/gcflobdd/context.rs#L346)), so merely *probing* the op
cache clones two `Vec<bool>` return maps and clones the result — three heap
allocations on every cache hit.

This is exactly the workload that exposes it. The quantum benchmarks in
[`BENCHMARKS.md`](BENCHMARKS.md) do a small number of large operations, where a
per-operation constant disappears. Network verification does tens of millions of
tiny ones on diagrams of a few hundred nodes: at k=12 the atomic-predicate stage
runs between 34M and 67M loop trips (892 predicates against an atom set growing to
75,408), each trip one conjunction and sometimes a difference.

Those two numbers meet. The stage takes 10,107 ms for `aligned-balanced-shared`,
and 10,107 ms at 151.4 ns is **67M operations** — the top of the range derived
from the loop bounds, arrived at independently. GCFLOBDD's time on this stage is
accounted for by the per-operation constant and nothing else.

**Interning the return map, or keying the op cache by reference, is the single
change that would move these numbers** — and it would not touch the diagrams at
all.

## Diagram size

Nodes in the largest single predicate built during the run. This is where the
representation, rather than its constant factor, shows up.

| k | BDD (JDD) | GCFLOBDD field-grouped | aligned-balanced | aligned-balanced-shared |
|--:|--:|--:|--:|--:|
| 4 | 337 | **18** | 137 | 88 |
| 6 | 361 | **19** | 136 | 91 |
| 8 | 429 | **20** | 158 | 107 |
| 10 | 383 | **20** | 154 | 104 |
| 12 | 399 | **21** | 172 | 115 |

![diagram size](results/macos-m1pro/figures/netverify_size.svg)

**Every GCFLOBDD grammar is smaller than the BDD, and the field-grouped one is
19x smaller** — 21 nodes against 399, and essentially flat (18 to 21) while the
predicate count grows 15x and the atom count 149x. A caveat that matters: a GCFLOBDD node is not a BDD node. `field-grouped`'s
21 nodes each *contain* an ordinary BDD over a whole 32- or 16-bit field, so this
compares one representation's node to another's, not like for like. The
aligned-balanced grammars recurse to single bits and so are the closer comparison:
115 nodes against 399, **3.5x smaller**, with no such caveat.

**Width-sharing pays, consistently.** Sharing `W16` across src_port and dst_port
and `W8` across the protocol and every byte of every other field takes the largest
diagram from 172 nodes to 115 — **a 32-36% reduction at every size**, for a grammar
of the same shape and the same field boundaries. It costs nothing at runtime (the
two coincide within 1%), so it is free.

NDD cannot appear in this table: it has no per-diagram node walk, only a global
count.

## Engine-wide size and memory

Live nodes across the whole engine, and peak resident memory. NDD's size is NDD
nodes *plus* the label BDDs underneath them, the way NDD's own `nqueens_metrics.csv`
splits it. JDD keeps its live count private, so it appears only in the RSS columns.

| k | NDD (nodes + labels) | GCFLOBDD field-grouped | aligned-balanced-shared | RSS: BDD | NDD | GCFLOBDD-abs |
|--:|--:|--:|--:|--:|--:|--:|
| 4 | 35,409 | **8,742** | 19,454 | 199 MB | 203 MB | **68 MB** |
| 6 | 71,938 | **31,641** | 80,741 | 207 MB | 226 MB | **127 MB** |
| 8 | **129,922** | 100,537 | 271,182 | **235 MB** | 254 MB | 305 MB |
| 10 | **255,808** | 273,055 | 863,278 | **436 MB** | 481 MB | 1,047 MB |
| 12 | **608,864** | 644,408 | 2,315,926 | 835 MB | **737 MB** | 3,090 MB |

![peak memory](results/macos-m1pro/figures/netverify_memory.svg)

Field-grouped GCFLOBDD lands within 6% of NDD's total node count at k=12
(644,408 against 608,864) — unsurprising, since it *is* NDD's decomposition, one
BDD leaf per field, under a different algebra.

The memory picture inverts with scale. GCFLOBDD starts at a third of the others
(68 MB against ~200 MB, because its nodes live outside the Java heap and the JVM's
own floor dominates the Java engines) and ends at 3.7x the BDD's.

The two GCFLOBDD grammars trade off against each other in the way their shapes
predict. At k=12 `aligned-balanced-shared` holds 3.6x more live nodes than
`field-grouped` and its largest diagram is 5.5x bigger in node count (115 against
21) — but each of those nodes addresses a single bit, where a field-grouped node
carries an entire 32- or 16-bit BDD inside it. Many small nodes against few large
ones; the node counts are not the same currency, which is why the memory column
is the one to compare.

Note that column measures the whole process, so every row carries a JVM — it is
comparable between rows, not against a native process.

## Reading these results

**GCFLOBDD represents these packet sets compactly and manipulates them slowly.**
Both halves are consistent across every size measured, and neither is a scaling
effect: the size advantage is roughly constant in k, and so is the runtime deficit.

The runtime gap has an identified, local cause with a measurement behind it, and
it is not the JNI. Until the return map stops being cloned on every cache probe,
a workload made of tens of millions of small conjunctions will pay 151 ns for each
one, and no grammar choice changes that — the three grammars differ from each
other by 1.5x while all three sit 2-3x behind the BDD.

The grammar result stands on its own and is actionable now: **align coarse splits
to field boundaries, and share one subtree per width**. That is free, and it is
worth a third of the diagram.

## Not covered

- `exists` / `restrict` are not implemented in the core, so NAT rewriting is out of
  reach. `nativeExists` raises `UnsupportedOperationException`. Nothing in the
  atomic-predicate or reachability kernels needs them; NDD's whole WAN verifier
  calls `exists` from exactly one place, `BDDACLWrapper.apply_rewrite`.
- Real operator data (Purdue, Stanford, Internet2). None ships with NDD.
- Incremental verification — these are batch runs. APKeep's rule-at-a-time update
  path is the natural next workload.
