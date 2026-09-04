# Network verification

GCFLOBDD measured against NDD ([XJTU-NetVerify/NDD](https://github.com/XJTU-NetVerify/NDD),
NSDI '25) and a plain BDD on the workload NDD was built for: atomic predicates
and all-pairs reachability over a data plane.

**Machine.** Apple M1 Pro, 10 cores, 32 GB, macOS 26.5.2 (Darwin 25.5.0). Runs
are strictly sequential; nothing else was on the machine.

**Builds.** rustc 1.95.0, `cargo build --release` in `gcflobdd-jni` (no `rug`,
no GMP), `smallvec` 1.15.2. OpenJDK 24.0.1, `-Xmx12g`. NDD at `c8414b4`, its core recompiled from
source rather than from its prebuilt jar, which predates NDD's int-handle rewrite.

Per-operation costs quoted below come from [`tests/opbench.rs`](tests/opbench.rs)
(`cargo test --release --test opbench`), which also reports allocations per
operation.

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
| 4 | **43 ms** | 50 | 76 | 48 | 49 |
| 6 | 145 ms | **129 ms** | 380 | 214 | 211 |
| 8 | 456 ms | **282 ms** | 1,265 | 661 | 652 |
| 10 | 1,403 ms | **644 ms** | 4,642 | 2,786 | 2,813 |
| 12 | 5,213 ms | **1,852 ms** | 14,876 | 9,534 | 9,676 |

![runtime](results/macos-m1pro/figures/netverify_runtime.svg)

**NDD's claim reproduces.** It starts level with the BDD and pulls away as the
problem grows — 1.6x at k=8, 2.2x at k=10, **2.8x at k=12** — which is the shape
its paper reports, on an independent harness.

**GCFLOBDD is slower here, by a steady factor.** The best grammar is level with
the BDD at k=4 and sits between 1.4x and 2.0x its time from k=6 up; the ratio has
no trend in k, so nothing is diverging — it is a constant factor, not a scaling
problem. Against NDD it is 5.2x at k=12.

These numbers are after the two changes described below, which — measured back to
back — took GCFLOBDD's per-conjunction cost from 1,926 ns to 1,605 ns and moved
this table 11-25% while the control engines held still, which is how we knew the
machine had not moved either. The table above is a later re-run of the whole
ladder, on which every engine, controls included, is a few percent slower than
that sitting; the per-operation costs below re-measure within noise.

### Where the constant factor comes from

The obvious suspect is the JNI boundary, since GCFLOBDD is the only engine here
that crosses it per operation. It is not the cause: the crossing costs 4.2 ns,
against 2.3 ns for a cached JDD `and`.

Neither is the operation cache, and an earlier version of this document got that
wrong. It observed that the atomic-predicate stage's runtime divided by the
cached-`and` cost came out near the loop-trip count, and concluded the constant
explained the stage. That was a coincidence of two numbers, not a cause.

**The atomic-predicate loop misses the cache almost every time.** It conjoins a
*different* atom with the predicate on every iteration, so the cache has nothing
to return and the work is the pair-map and reduction underneath.
[`tests/opbench.rs`](tests/opbench.rs) measures both paths, and a counting global
allocator alongside them:

Medians of three runs, one library change at a time, same benchmark throughout:

| | cached `and` | atomic-predicate loop | allocations per conjunction |
|---|--:|--:|--:|
| before | 88.4 ns | 1,926 ns | 54.5 |
| return map behind an `Rc` | **17.0 ns** | 1,917 ns | 49.5 |
| + small maps inline | 17.0 ns | **1,605 ns** | **21.2** |

Two changes, and they buy different things.

**The return map moved behind an `Rc`.** A diagram is cloned three times per
cache hit — twice to build the probe key, once to take the answer back out — and
each clone copied its return map, a heap allocation however few entries it held.
A refcount bump instead took a cached `and` from 88 ns to 17 ns, a 5.2x cut on
that path — and moved this workload **not at all**, because this workload does
not hit the cache. It is the right fix for the wrong bottleneck, kept because
5.2x on a fundamental operation is worth having.

**The small maps went inline** (`smallvec`). Counted by size class, 98% of the
allocations were under 128 bytes — return maps, reduce matrices, the connections
leaving a node, none more than a handful of words. Inline storage removed 28 of
the 49.5 allocations per conjunction and took the loop to 1,605 ns, **1.19x**,
which is what shows up in the runtime table.

**Not every map, though.** `SmallVec` pays a branch on every access to decide
inline-versus-spilled: a win where it saves an allocation, a loss where it does
not. The *transient* maps are built once and dropped, so they go inline. The
*finished* diagram's return map is read far more often than it is built — every
cache probe hashes and compares it — and putting that one inline too cost 10 ns
on every cached `and` and 9% on the loop, against the single allocation it saved.
It stays a plain `Vec` behind the `Rc`. Both halves are in
[`return_map.rs`](src/gcflobdd/return_map.rs).

One cost worth knowing: `Vec`'s `Drop` carries `#[may_dangle]` and `SmallVec`'s
does not, so dropck is now stricter. A grammar must be declared before the
`Context` that borrows it — which the borrow already implied, but the compiler
used to let the other order pass.

What remains is 21.2 allocations per conjunction, most of them the `Rc` for each
genuinely new interned node — the representation working, not overhead to shave.
Going further means changing how nodes are stored, not how maps are.

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
two coincide within 2.1%), so it is free.

NDD appears in the anatomy table below rather than this one. It ships no
per-diagram walk, but its node and edge accessors are public, so
[`NddBackend`](bench/netverify/src/netbench/backend/NddBackend.java) writes one --
and an NDD diagram has two sizes, its own nodes and the label BDDs under them,
which do not add up to a figure comparable with this column.

## Engine-wide size and memory

Live nodes across the whole engine, and peak resident memory. NDD's size is NDD
nodes *plus* the label BDDs underneath them, the way NDD's own `nqueens_metrics.csv`
splits it. JDD keeps its live count private, so it appears only in the RSS columns.

| k | NDD (nodes + labels) | GCFLOBDD field-grouped | aligned-balanced-shared | RSS: BDD | NDD | GCFLOBDD-abs |
|--:|--:|--:|--:|--:|--:|--:|
| 4 | 35,409 | **8,742** | 19,454 | 196 MB | 205 MB | **69 MB** |
| 6 | 71,938 | **31,641** | 80,741 | 206 MB | 220 MB | **131 MB** |
| 8 | 129,922 | **100,537** | 271,182 | **233 MB** | 243 MB | 319 MB |
| 10 | **255,808** | 273,055 | 863,278 | **457 MB** | 482 MB | 972 MB |
| 12 | **608,864** | 644,408 | 2,315,926 | 835 MB | **764 MB** | 3,132 MB |

![peak memory](results/macos-m1pro/figures/netverify_memory.svg)

Field-grouped GCFLOBDD lands within 6% of NDD's total node count at k=12
(644,408 against 608,864) — unsurprising, since it *is* NDD's decomposition, one
BDD leaf per field, under a different algebra.

The memory picture inverts with scale. GCFLOBDD starts at a third of the others
(69 MB against ~200 MB, because its nodes live outside the Java heap and the JVM's
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

## How the work scales with the network

The operation count is engine-independent -- every backend executes the identical
sequence -- so one run of `netbench.Anatomy` per dataset fixes it for all five.
Nine rungs, k=4 to k=20, medians of three seeds through k=16 and one seed beyond:

| k | devices | FIB rules | ACL rules | predicates | atoms | build ops | AP ops | total ops |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 4 | 20 | 180 | 72 | 60 | 506 | 8,552 | 2,891 | 11,443 |
| 6 | 45 | 420 | 162 | 168 | 3,358 | 19,182 | 39,921 | 59,103 |
| 8 | 80 | 840 | 288 | 341 | 7,821 | 35,845 | 152,822 | 188,667 |
| 10 | 125 | 1,500 | 450 | 574 | 26,095 | 58,998 | 692,871 | 751,869 |
| 12 | 180 | 2,460 | 648 | 892 | 75,408 | 90,080 | 2,437,671 | 2,527,751 |
| 14 | 245 | 3,780 | 882 | 1,279 | 184,882 | 129,681 | 7,106,599 | 7,236,280 |
| 16 | 320 | 5,520 | 1,152 | 1,768 | 463,680 | 178,965 | 20,996,316 | 21,175,281 |
| 18 | 405 | 7,740 | 1,458 | 2,370 | 1,082,892 | 238,687 | 59,298,537 | 59,537,224 |
| 20 | 500 | 10,500 | 1,800 | 3,080 | 2,007,704 | 309,641 | 140,155,417 | 140,465,058 |

The inputs are exact, not fitted. A k-ary fat tree has k pods of k/2 edge and k/2
aggregation switches plus (k/2)^2 core, so **D = 5k^2/4 devices**; the generator
writes k routes per edge and aggregation switch, k per core, plus 25k injected,
for **5k^3/4 + 25k FIB rules = Theta(D^1.5)**; and one ACL per edge switch, 9
rules each, for **9k^2/2 = 3.6D ACL rules = Theta(D)**. Every row matches those
closed forms exactly.

What is measured, as a log-log fit over D = 20..500:

| quantity | fit | R^2 |
|---|---|--:|
| distinct predicates | **Theta(D^1.21)** | 0.9998 |
| build operations | **Theta(D^1.12)** | 0.9986 |
| atomic predicates | Theta(D^2.6) | 0.9858 |
| **AP operations** | **Theta(D^3.3)** | 0.9933 |
| total operations | Theta(D^3.0) | 0.9776 |

Predicates grow more slowly than ports (Theta(D^1.5)) because fat-tree symmetry
makes many ports share a forwarding predicate; that exponent is the stablest thing
in the table, holding between 1.17 and 1.27 on every single rung. Building them
costs 30 to 48 operations per FIB rule -- the prefix encoding plus the
longest-prefix-match `diff`/`or`, the ratio falling as aggregate routes shorten
the average prefix -- so the build stage is linear in its input and stops
mattering above k=6.

### The AP exponent is not settled, and that is the interesting part

`Theta(D^3.3)` describes the measured range; it is not an asymptote. The
rung-to-rung slope climbs:

| k | 4->6 | 6->8 | 8->10 | 10->12 | 12->14 | 14->16 | 16->18 | 18->20 |
|---|--:|--:|--:|--:|--:|--:|--:|--:|
| AP ops | D^3.24 | D^2.33 | D^3.39 | D^3.45 | D^3.47 | D^4.06 | D^4.41 | D^4.08 |
| atoms | D^2.33 | D^1.47 | D^2.70 | D^2.91 | D^2.91 | D^3.44 | D^3.60 | D^2.93 |

The whole-range fit is reproducible across seeds (D^3.11, D^3.17, D^3.15 on the
seven rungs all three seeds ran), but the tail is not: seed to seed the last four
rungs give D^3.43 to D^4.11, because the atom count itself varies up to 1.86x with
the seed. Read the exponent as "between 3 and 4 and drifting upward", not as a
constant.

The atom trajectory says why. The loop's cost is `sum |A_i|` over the predicate
sequence, and that sequence is far from uniform -- at k=16:

| through the loop | 50% | 80% | 90% | 95% | 99% | end |
|---|--:|--:|--:|--:|--:|--:|
| atoms | 786 | 1,272 | 1,457 | **44,862** | 327,737 | 569,253 |

For the first 90% the atom count is *linear* in predicates processed, about 1.6
atoms each. Then it multiplies by 390x in the last 10%.

That break is exactly where the forwarding predicates end and the ACLs begin.
Longest-prefix-match forwarding predicates are sets of IP prefixes, and prefixes
are a **laminar family** -- any two are nested or disjoint -- so n of them induce
at most 2n-1 atoms, which is the linear stretch. ACL predicates are five-tuple
boxes constraining source, ports and protocol as well, so they cut *across* the
destination-prefix nesting and each one splits a large fraction of the atoms
already there. There are k^2/2 of them, 0.4D, and they are the entire
superlinearity. It is also why the work is nowhere near the `predicates x atoms`
rectangle: that ratio falls from 0.095 to 0.023 across the ladder.

### What the engine choice is worth, given that exponent

Because the operation count is fixed by the data plane and the engine only sets
nanoseconds per operation, the whole runtime is one multiplication. At k=20's
140.5M operations NDD takes 143 s, measured. Carrying the other two engines'
k=12 per-operation costs forward puts a BDD at ~344 s and GCFLOBDD at ~562 s --
extrapolations, not runs.

And on a `D^3.3` curve a constant factor buys very little reach. NDD's **5x**
per-operation advantage over GCFLOBDD is `5^(1/3.3)` in network size --
**1.6x more devices** before hitting the same wall. Its 3x over a plain BDD is
1.4x. The representation decides where you stop; the exponent decides that you
stop.

## Anatomy: why the per-operation costs differ

The runtime table is stage times. This is what those times are made of, measured
by [`netbench.Anatomy`](bench/netverify/src/netbench/Anatomy.java), which wraps
every engine in a counting [`PacketDd`](bench/netverify/src/netbench/CountingDd.java)
so the driver's operations can be counted and divided into the clock.

```bash
cd bench/netverify && source env.sh
java --enable-native-access=ALL-UNNAMED -Djava.library.path="$NETBENCH_LIB" \
  -cp "$NETBENCH_CP" netbench.Anatomy --backend ndd --data data/ft12_r300_a8_s1
```

**Every engine performs the identical number of operations.** At k=12 that is
89,890 to build the predicates and 2,137,316 to atomise them, the same on each of
the three run at that size; at k=10 all five agree on 58,998 and 590,195. Nothing
here is an algorithmic difference -- the whole gap is nanoseconds per operation.

At k=10, where every grammar was run: NDD 756 ns, BDD 2,176, aligned-balanced
3,881, aligned-balanced-shared 4,023, field-grouped 6,908. Field-grouped is the
slowest per operation and has *by far* the smallest diagrams (5 nodes median),
because each of its nodes wraps a whole 32-bit BDD -- another reading on which
node count and cost do not track each other.

| k=12 | ns per operation | median predicate | median atom |
|---|--:|--:|--:|
| NDD | **809** | 1 NDD node | 5 NDD nodes |
| BDD (JDD) | 2,446 | 32 nodes | 88 nodes |
| GCFLOBDD aligned-balanced-shared | 4,004 | 26 nodes | 45 nodes |

**The counter-intuitive row is NDD's.** Its diagrams are not smaller. An NDD
predicate averages 2.06 NDD nodes *plus* 42.4 label-BDD nodes underneath -- about
44 decision-diagram nodes in total, which is **more** than the BDD's 32, and more
than GCFLOBDD's 26. NDD is three times faster while holding the larger object.

So size is not what is being measured, and the usual "smaller diagram, faster
operations" reading does not survive contact with this table. What differs is
*where* the recursion happens.

**A five-tuple predicate constrains 1.31 of the 5 fields, on average.** A FIB
entry names a destination prefix and nothing else; most ACL rules name two fields.
NDD skips an unconstrained field outright -- `andRec` takes the earlier field and
recurses on one operand's children with the other left whole
([NDD.java:1007-1019](/Users/endaytrer/src/NDD/src/main/java/org/ants/jndd/diagram/NDD.java#L1007-L1019))
-- so its recursion is over a 2-node skeleton, and the bit-level work is handed to
a BDD over at most 32 variables. The monolithic BDD has no such seam: it re-walks
all 104 variables, 88 nodes for a median atom, on every operation.

That NDD's label engine *is* `jdd.bdd.BDD` -- the same library as the baseline
row, not a faster one -- makes this a controlled experiment. Same BDD code, same
operations, same data; only the field decomposition differs, and it is worth
**3.0x**. (That the label operations are also mostly cache hits, since every IP
field draws prefixes from one right-aligned shared variable pool, is inference
from the structure; NDD's caches were not instrumented.)

**GCFLOBDD's cost is the opposite shape: few steps, expensive ones.**
[`tests/workprofile.rs`](tests/workprofile.rs), with the `opcount` feature
counting recursive calls, puts one conjunction at **5.7 recursive calls**, 5.2
node interns, 11.9 return-map interns and 21.2 heap allocations -- roughly 330 ns
a step, against the BDD's ~28 ns. Fifteen times fewer steps, twelve times the cost
each.

That ratio is not an implementation defect to be tuned away; it is what the
representation is. A BDD's unit of work is three machine words in a flat array. A
CFLOBDD's is a grouping *and a return map*: combining two of them means
materialising the cross product of their exit vertices and then reducing it, which
is variable-length, heap-allocated, and quadratic in the exit counts. That
machinery is exactly what buys the shape/value decoupling -- and here there is
nothing to buy, because the predicates are already small in every representation.

```bash
cargo run --release --features opcount --test workprofile
cargo run --release --features opcount --test scaling
```

## The same engine on circuits

The contrast is not that quantum operations are cheap. They are not:

| | GHZ, 32,768 qubits | network, k=12 |
|---|--:|--:|
| top-level operations | ~65,500 | 2,227,206 |
| GCFLOBDD, ns per operation | ~6,050 | 4,004 |
| diagram | 59 nodes | 45 nodes (median atom) |
| what that diagram holds | 2^32,768 amplitudes | a set a BDD holds in 88 nodes |

A GCFLOBDD operation costs *more* microseconds in the circuit workload than in the
network one. Two other things differ, and they decide everything.

**The operation count is 34x smaller.** A circuit does work proportional to its
gates; atomic-predicate splitting does work proportional to predicates times
atoms, and the atom count grows 149x across this ladder.

**The compression is exponential rather than a constant factor.** Measured on
this machine, GHZ's state vector is 43 nodes at 2,048 qubits, 51 at 8,192, 59 at
32,768 -- **eight nodes per doubling**, so O(log n) where a BDD is O(n).
Deutsch-Jozsa at 16 qubits is 111 GCFLOBDD nodes against CUDD's 53,517, 0.212 ms
against 442 ms; CUDD times out at 32 qubits, while GCFLOBDD reaches 65,536 in
18 ms at 399 nodes ([BENCHMARKS.md](BENCHMARKS.md)).

Both properties come from the same place. A CFLOBDD level factors a function into
a *shape* shared by every subtree of that width and *values* carried separately in
return maps, so a function whose halves are the same function collapses to one
grouping per level. Parity and all-bits-equal have that property; so do the
tensor-product states and uniform superpositions circuits produce. An address
prefix does not: the top half is a chain of literals and the bottom half is
`true`, two different functions, with nothing for a level to share.

Which leaves the tax with nothing to pay for. Network verification pins the
variable count at 104 forever -- the five-tuple does not grow -- so scale arrives
as *more predicates*, not bigger ones, and there is no exponential to escape. The
currency is throughput, and NDD buys throughput with a decomposition that fits the
data exactly.

## Reading these results

**GCFLOBDD represents these packet sets compactly and manipulates them slowly.**
Both halves are consistent across every size measured, and neither is a scaling
effect: the size advantage is roughly constant in k, and so is the runtime deficit.

The runtime gap has a measured cause, and it is neither the JNI (4.2 ns a
crossing) nor the operation cache (17.0 ns a hit). It is that a cache *miss* —
which is what this workload is made of — costs 1,605 ns and 21.2 heap
allocations. Putting the small maps inline already took that from 1,926 ns and
54.5 allocations; what is left is mostly the `Rc` per new interned node, so the
next move would be to how nodes are stored rather than how maps are. No grammar
choice changes it: the three grammars differ from each other by 1.6x, and the
best of them is 1.8x behind the BDD at k=12.

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
