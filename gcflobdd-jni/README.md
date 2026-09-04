# gcflobdd-jni

JNI bindings that expose the GCFLOBDD packet-set engine to Java, mirroring the
small subset of the `jdd.bdd.BDD` int-handle interface that the APKeep network
verifier uses. This lets GCFLOBDD be dropped in as a third BDD backend
(alongside jdd-BDD and Atomized-NDD) for an apples-to-apples comparison.

## Build

```sh
cargo build --release          # -> target/release/libgcflobdd_jni.so
```

The core `gcflobdd` dependency is pulled with `default-features = false,
features = ["fx-hash"]`, so this build has **no** `rug`/GMP (native C) dependency.

Load from Java with `-Djava.library.path=<dir containing libgcflobdd_jni.so>` and
`System.loadLibrary("gcflobdd_jni")`.

## Java contract

The matching Java class is
[`application.wan.bdd.verifier.common.GcflobddEngine`](../bench/netverify/src/application/wan/bdd/verifier/common/GcflobddEngine.java),
the Java side of the `netbench` network-verification harness. All native methods
are `static` and take the opaque engine pointer (`long`) as their first argument:

```java
package application.wan.bdd.verifier.common;

public final class GcflobddEngine implements AutoCloseable {
    static { System.loadLibrary("gcflobdd_jni"); }

    // Grammar selector; see "Grammar configurations" below.
    public static final int CONFIG_FIELD_GROUPED           = 0;
    public static final int CONFIG_ALIGNED_BALANCED        = 1;
    public static final int CONFIG_ALIGNED_BALANCED_SHARED = 2;

    private static native long   nativeNew(int config);
    private static native void   nativeDestroy(long ptr);

    private static native int    nativeCreateVar(long ptr);
    private static native int    nativeAnd(long ptr, int a, int b);
    private static native int    nativeOr(long ptr, int a, int b);
    private static native int    nativeNot(long ptr, int a);
    private static native int    nativeDiff(long ptr, int a, int b);   // a AND NOT b
    private static native int    nativeXor(long ptr, int a, int b);

    private static native int    nativeRef(long ptr, int a);      // returns a
    private static native int    nativeDeref(long ptr, int a);    // returns a
    private static native int    nativeGetRef(long ptr, int a);
    private static native int    nativeGc(long ptr);              // -> node count

    private static native double nativeSatCount(long ptr, int a); // raw count
    private static native int[]  nativeOneSat(long ptr, int a, int[] buffer);
    private static native int    nativeNodeCount(long ptr, int a);
    private static native long   nativeGetMemoryUsage(long ptr);
    private static native boolean nativeIsValid(long ptr, int a);
    private static native int    nativeNumVars(long ptr);

    // Per-handle diagram size, for the size tables in BENCHMARKS.md.
    private static native int    nativeDiagramNodes(long ptr, int a);
    private static native int    nativeDiagramEdges(long ptr, int a);
    private static native int    nativeConvTotal(long ptr, int a);

    private static native int    nativeExists(long ptr, int bdd, int cube);
}
```

`netbench` drives only a subset of this: `and`, `or`, `not`, `diff`, `ref`,
`deref`, `gc`, `satCount`, `nodeCount`, `memoryUsage`, `numVars`,
`diagramNodes`, `diagramEdges` and `convTotal`. The rest -- `xor`, `oneSat`,
`isValid`, `getRef`, `exists` -- round out the `jdd.bdd.BDD` shape and are
covered by the unit tests in `src/engine.rs`, but no current caller uses them.

## Semantics (matching jdd)

- **Constants:** handle `0 = false`, `1 = true`, pre-seeded and pinned.
- **Canonical ids:** equal formulas share one `int` handle (GCFLOBDD's per-
  `Context` canonical `Eq`/`Hash`), so APKeep's `==` comparison of atomic
  predicates is exact. `and(v, not v) == 0`, `not(0) == 1`, etc.
- **`createVar()`:** returns the next flat variable index (0..`numVars`), and
  **pins both the variable and its negation**, exactly like jdd's saturated
  nodes — so an unreferenced `not(var)` is never collected.
- **`ref`/`deref`:** per-handle reference count; `ref`/`deref` return the handle.
  Constants, variables, and variable negations are pinned (never collected).
- **`gc()`:** reclaims unpinned, reference-count-0 handles, then compacts the
  `Context`; returns the resulting node count. (APKeep's fattree harness never
  calls the backend `gc`, so there is no in-flight-collection hazard.)
- **`satCount(a)`:** returns the **raw** number of satisfying assignments as a
  `double` (the core computes `log2(count)`; the JNI layer exponentiates back).
  `2^104` fits comfortably in an `f64`.
- **`oneSat(a, buffer)`:** fills `buffer` (length `numVars`) with one satisfying
  assignment, indexed by flat variable index — `1` = true, `0` = false,
  `-1` = don't-care; an unsatisfiable function yields all `-1`. Returns the
  filled array (a fresh one if `buffer` is null / the wrong size).
- **`nodeCount(a)`:** GCFLOBDD has no cheap per-handle node count, so this
  reports the shared `Context`'s reachable node count (a global diagnostic).
- **`exists(bdd, cube)`:** existential quantification, used only by NAT
  rewriting. **Not yet implemented** (needs a canonicalizing CFLOBDD reduction);
  raises `UnsupportedOperationException`. The fattree benchmark does not use it.

## Grammar configurations

All three keep the same 104-leaf order — srcip(32)·dstip(32)·srcport(16)
·dstport(16)·proto(8) — so `createVar()`'s sequential index matches the field
layout (and matches jdd/NDD) whichever configuration is selected.

- **Config 0 — `FieldGrouped` (NDD-like):** one ordinary-BDD leaf per field:
  `S -> BDD(32) BDD(32) BDD(16) BDD(16) BDD(8)`.
- **Config 1 — `AlignedBalanced`:** a balanced binary tree recursed to single
  bits, with coarse splits forced onto field boundaries
  (`S -> A B; A -> SI32 DI32; B -> SP16 C; C -> DP16 PR8`) and each field a
  perfect binary tree of single bits. Every field carries its own symbols, so
  equal-width subtrees are distinct grammar nodes.
- **Config 2 — `AlignedBalancedShared`:** the same tree shape and the same field
  boundaries, but each subtree width is named once (`W32`, `W16`, `W8`, `W4`,
  `W2`) and shared by every field of that width. GCFLOBDD node identity is keyed
  on the grammar node's address, so this is what lets a src_ip subdiagram share
  nodes with a dst_port one. [`BENCHMARKS.md`](../BENCHMARKS.md) measures the
  difference: 32-36% fewer nodes in the largest diagram than Config 1.

The selector values above are the integers `nativeNew` takes, and match the
`CONFIG_*` constants on the Java class.

## Threading

The engine uses `Rc`/`RefCell` and is **not** thread-safe. Drive one engine
from a single thread; never share an engine or its handles across threads.
APKeep uses one static engine, which fits.
