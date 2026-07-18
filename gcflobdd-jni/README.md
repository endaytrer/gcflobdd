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

## Java contract (Workstream 3)

The matching Java class is
`application.wan.bdd.verifier.common.GcflobddEngine`. All native methods are
`static` and take the opaque engine pointer (`long`) as their first argument:

```java
package application.wan.bdd.verifier.common;

public final class GcflobddEngine {   // implements PacketBddEngine (WS3)
    static { System.loadLibrary("gcflobdd_jni"); }

    // config: 0 = FIELD_GROUPED (Config 1), 1 = ALIGNED_BALANCED (Config 2)
    private static native long   nativeNew(int config);
    private static native void   nativeDestroy(long ptr);

    private static native int    nativeCreateVar(long ptr);
    private static native int    nativeAnd(long ptr, int a, int b);
    private static native int    nativeOr(long ptr, int a, int b);
    private static native int    nativeNot(long ptr, int a);

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

    private static native int    nativeExists(long ptr, int bdd, int cube);
}
```

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

Both keep the same 104-leaf order — srcip(32)·dstip(32)·srcport(16)·dstport(16)
·proto(8) — so `createVar()`'s sequential index matches the field layout (and
matches jdd/NDD) for either configuration.

- **Config 1 — `FieldGrouped` (NDD-like):** one ordinary-BDD leaf per field:
  `S -> BDD(32) BDD(32) BDD(16) BDD(16) BDD(8)`.
- **Config 2 — `AlignedBalanced`:** a balanced binary tree recursed to single
  bits, with coarse splits forced onto field boundaries
  (`S -> A B; A -> SI32 DI32; B -> SP16 C; C -> DP16 PR8`) and each field a
  perfect binary tree of single bits.

## Threading

The engine uses `Rc`/`RefCell` and is **not** thread-safe. Drive one engine
from a single thread; never share an engine or its handles across threads.
APKeep uses one static engine, which fits.
