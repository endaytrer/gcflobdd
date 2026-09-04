package netbench;

import application.wan.bdd.verifier.common.GcflobddEngine;
import netbench.backend.JddBackend;

/**
 * Cost of one decision-diagram operation when the answer is already cached, for
 * a Java-native engine and for GCFLOBDD behind JNI.
 *
 * <p>GCFLOBDD is the only backend here that crosses the JNI boundary per
 * operation; the two Java engines do not. This measures that difference so the
 * runtime tables can say how much of GCFLOBDD's time is the boundary rather than
 * the algorithm. Every call is a cache hit, so what is left is call overhead.
 */
public final class JniProbe {

    public static void main(String[] args) {
        int iters = args.length > 0 ? Integer.parseInt(args[0]) : 5_000_000;

        JddBackend jdd = new JddBackend(1 << 20, 1 << 18);
        int ja = jdd.var(Fields.SRC_IP, 0), jb = jdd.var(Fields.DST_IP, 0);
        long jddNs = time(iters, i -> jdd.and(ja, jb));
        jdd.close();

        try (GcflobddEngine g = new GcflobddEngine(GcflobddEngine.CONFIG_ALIGNED_BALANCED_SHARED)) {
            int ga = 0, gb = 0;
            for (int i = 0; i < Fields.TOTAL_BITS; i++) {
                int v = g.createVar();
                if (i == 0) ga = v;
                if (i == 32) gb = v;
            }
            final int a = ga, b = gb;
            long gNs = time(iters, i -> g.and(a, b));
            // A native that does essentially nothing: what is left is the cost
            // of crossing the boundary, separating it from the engine's own work.
            long crossNs = time(iters, i -> g.numVars());

            System.out.printf(
                "JNIPROBE iters=%d jdd_and_ns=%.1f gcflobdd_and_ns=%.1f "
                + "jni_crossing_ns=%.1f gcflobdd_engine_ns=%.1f%n",
                iters, jddNs / (double) iters, gNs / (double) iters,
                crossNs / (double) iters, (gNs - crossNs) / (double) iters);
        }
    }

    private interface Op { int apply(int i); }

    private static long time(int iters, Op op) {
        for (int i = 0; i < iters / 10; i++) op.apply(i);   // warm the JIT
        long t0 = System.nanoTime();
        int sink = 0;
        for (int i = 0; i < iters; i++) sink ^= op.apply(i);
        long dt = System.nanoTime() - t0;
        if (sink == 0xdeadbeef) System.err.print("");        // keep the loop live
        return dt;
    }
}
