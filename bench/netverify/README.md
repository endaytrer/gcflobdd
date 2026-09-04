# Network verification: GCFLOBDD beside NDD and BDD

NDD ([XJTU-NetVerify/NDD](https://github.com/XJTU-NetVerify/NDD), NSDI '25) argues
its case on network verification. This harness meets it there: one driver, one
workload, five engines, byte-identical inputs.

Results and the write-up are in [`NETWORK.md`](../../NETWORK.md).

## Why the workload is ours

**NDD ships no network data.** `/datasets/` is gitignored on all four branches,
and both WAN drivers hardcode an absolute path on the authors' machine
(`/data/zcli-data/network-decision-diagram/datasets/wan/purdue/pd`). The WAN
verifier is also excluded from NDD's own Maven build on `main` — it still uses the
pre-rewrite object-based NDD API and no longer type-checks. The only benchmark
that ships runnable, with data, is N-Queens.

So the data plane is generated here: a k-ary fat-tree with longest-prefix-match
FIBs and ClassBench-shaped ACLs, from a fixed seed.

## Layout

| | |
|---|---|
| `src/netbench/Fields.java` | NDD's 104-bit five-tuple layout, bit 0 = MSB |
| `src/netbench/PacketDd.java` | the one interface all five engines implement |
| `src/netbench/backend/` | JDD-BDD, NDD, and GCFLOBDD (three grammars) |
| `src/netbench/Encoder.java` | rules → predicates; prefix, port range, protocol |
| `src/netbench/Verifier.java` | the kernels: LPM forwarding, ACLs, atomic predicates, reachability |
| `src/netbench/gen/Fattree.java` | the dataset generator |
| `src/netbench/Main.java` | driver; prints one `RESULT` line |
| `src/netbench/JniProbe.java` | per-operation cost, JNI boundary vs. engine |
| `src/application/wan/bdd/verifier/common/GcflobddEngine.java` | Java side of `gcflobdd-jni` |

`GcflobddEngine`'s package is fixed: the native symbols are exported as
`Java_application_wan_bdd_verifier_common_GcflobddEngine_*`.

## Build and run

```sh
NDD_REPO=/path/to/NDD ./build.sh        # cdylib + NDD core from source + this
./run.sh --backend ndd --data data/ft8_r200_a8_s1 --workload all
```

`build.sh` recompiles NDD's core from source rather than using its prebuilt jar,
which predates NDD's int-handle rewrite. It skips `AtomizedNDD`/`AtomizedNodeTable`,
exactly as NDD's own pom does.

Backends: `jdd`, `ndd`, `gcflobdd-fg`, `gcflobdd-ab`, `gcflobdd-abs`.
Workloads: `acl`, `ap`, `reach`, `equiv`, `all`.

Generate a dataset:

```sh
java -cp "$NETBENCH_CP" netbench.gen.Fattree --k 8 --routes 200 --acl-rules 8 --seed 1 \
  --out data/ft8_r200_a8_s1
```

The whole ladder, five engines × five sizes × three seeds:

```sh
KS="4 6 8 10 12" SEEDS="1 2 3" OUT=results/netverify.csv \
  scripts/compare_netverify.sh
python3 results/macos-m1pro/build_netverify_tables.py
python3 results/macos-m1pro/plot_netverify.py
```

## Reference discipline

Every `Encoder` method returns a handle carrying **one reference the caller owns**
and must `deref`. This is not bookkeeping hygiene — JDD collects garbage inside
`mk`, so an unreferenced intermediate can have its slot recycled by the very next
operation and the run then silently computes a different answer. It first showed
up as JDD reporting 82,050 atomic predicates where every other engine reported
81,993.

`build_netverify_tables.py` re-checks agreement on every run and prints
`cross-implementation agreement: OK` or the disagreements.
