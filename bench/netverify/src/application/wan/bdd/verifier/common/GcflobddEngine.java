package application.wan.bdd.verifier.common;

/**
 * Java side of the {@code gcflobdd-jni} cdylib: a GCFLOBDD packet-set engine
 * behind the slice of the {@code jdd.bdd.BDD} int-handle interface that APKeep
 * uses.
 *
 * <p>The package and class name are load-bearing -- the native symbols are
 * exported as {@code Java_application_wan_bdd_verifier_common_GcflobddEngine_*}.
 *
 * <p>Handles are canonical: equal formulas share one {@code int}, so atomic
 * predicates can be compared with {@code ==}. Constants are {@code 0 = false}
 * and {@code 1 = true}. The engine is <b>not</b> thread-safe; drive one instance
 * from one thread.
 */
public final class GcflobddEngine implements AutoCloseable {

    static {
        System.loadLibrary("gcflobdd_jni");
    }

    /** One ordinary-BDD leaf per field: {@code S -> BDD(32) BDD(32) BDD(16) BDD(16) BDD(8)}. */
    public static final int CONFIG_FIELD_GROUPED = 0;
    /** Aligned-balanced with per-field symbols. */
    public static final int CONFIG_ALIGNED_BALANCED = 1;
    /** Aligned-balanced with one shared subtree per width. */
    public static final int CONFIG_ALIGNED_BALANCED_SHARED = 2;

    public static final int FALSE = 0;
    public static final int TRUE = 1;

    private long ptr;

    public GcflobddEngine(int config) {
        ptr = nativeNew(config);
        if (ptr == 0) {
            throw new IllegalArgumentException("unknown grammar config " + config);
        }
    }

    private long ptr() {
        if (ptr == 0) throw new IllegalStateException("engine already closed");
        return ptr;
    }

    @Override
    public void close() {
        if (ptr != 0) {
            nativeDestroy(ptr);
            ptr = 0;
        }
    }

    public int createVar()             { return nativeCreateVar(ptr()); }
    public int and(int a, int b)       { return nativeAnd(ptr(), a, b); }
    public int or(int a, int b)        { return nativeOr(ptr(), a, b); }
    public int not(int a)              { return nativeNot(ptr(), a); }
    public int diff(int a, int b)      { return nativeDiff(ptr(), a, b); }
    public int xor(int a, int b)       { return nativeXor(ptr(), a, b); }

    public int ref(int a)              { return nativeRef(ptr(), a); }
    public int deref(int a)            { return nativeDeref(ptr(), a); }
    public int getRef(int a)           { return nativeGetRef(ptr(), a); }
    public int gc()                    { return nativeGc(ptr()); }

    public double satCount(int a)      { return nativeSatCount(ptr(), a); }
    public int[] oneSat(int a, int[] b){ return nativeOneSat(ptr(), a, b); }
    public int nodeCount()             { return nativeNodeCount(ptr(), 0); }
    public long memoryUsage()          { return nativeGetMemoryUsage(ptr()); }
    public boolean isValid(int a)      { return nativeIsValid(ptr(), a); }
    public int numVars()               { return nativeNumVars(ptr()); }

    /** Nodes reachable from this diagram's root (per-handle, not global). */
    public int diagramNodes(int a)     { return nativeDiagramNodes(ptr(), a); }
    /** Connections leaving them, one edge per connection. */
    public int diagramEdges(int a)     { return nativeDiagramEdges(ptr(), a); }
    /** Size under the reference C++ CFLOBDD's counting convention. */
    public int convTotal(int a)        { return nativeConvTotal(ptr(), a); }

    /** Existential quantification -- not implemented; only NAT rewriting needs it. */
    public int exists(int a, int cube) { return nativeExists(ptr(), a, cube); }

    private static native long    nativeNew(int config);
    private static native void    nativeDestroy(long ptr);
    private static native int     nativeCreateVar(long ptr);
    private static native int     nativeAnd(long ptr, int a, int b);
    private static native int     nativeOr(long ptr, int a, int b);
    private static native int     nativeNot(long ptr, int a);
    private static native int     nativeDiff(long ptr, int a, int b);
    private static native int     nativeXor(long ptr, int a, int b);
    private static native int     nativeRef(long ptr, int a);
    private static native int     nativeDeref(long ptr, int a);
    private static native int     nativeGetRef(long ptr, int a);
    private static native int     nativeGc(long ptr);
    private static native double  nativeSatCount(long ptr, int a);
    private static native int[]   nativeOneSat(long ptr, int a, int[] buffer);
    private static native int     nativeNodeCount(long ptr, int a);
    private static native long    nativeGetMemoryUsage(long ptr);
    private static native boolean nativeIsValid(long ptr, int a);
    private static native int     nativeNumVars(long ptr);
    private static native int     nativeDiagramNodes(long ptr, int a);
    private static native int     nativeDiagramEdges(long ptr, int a);
    private static native int     nativeConvTotal(long ptr, int a);
    private static native int     nativeExists(long ptr, int bdd, int cube);
}
