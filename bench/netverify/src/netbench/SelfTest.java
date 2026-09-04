package netbench;

import java.util.ArrayList;
import java.util.List;

import application.wan.bdd.verifier.common.GcflobddEngine;
import netbench.backend.GcflobddBackend;
import netbench.backend.JddBackend;

/**
 * Checks the encoding against brute force, and the engines against each other,
 * on cases small enough to enumerate by hand.
 *
 * <p>The cross-engine agreement in the benchmark itself only says the engines
 * agree; it cannot say they are all agreeing on the right answer. This says that.
 */
public final class SelfTest {

    private static int failures = 0;

    private static void check(boolean ok, String what) {
        if (!ok) { System.out.println("  FAIL " + what); failures++; }
    }

    /** Every prefix cover must be exact: the union is the range, nothing spills. */
    private static void testCover() {
        int width = 8;
        for (int lo = 0; lo < 256; lo += 7) {
            for (int hi = lo; hi < 256; hi += 11) {
                boolean[] hit = new boolean[256];
                for (long[] p : Encoder.cover(lo, hi, width)) {
                    long base = p[0];
                    int len = (int) p[1];
                    int span = 1 << (width - len);
                    check(base % span == 0, "cover block " + base + "/" + len + " is aligned");
                    for (int v = 0; v < span; v++) {
                        int x = (int) base + v;
                        check(x < 256, "cover stays in range for [" + lo + "," + hi + "]");
                        if (x < 256) {
                            check(!hit[x], "cover blocks are disjoint at " + x);
                            hit[x] = true;
                        }
                    }
                }
                for (int v = 0; v < 256; v++) {
                    boolean want = v >= lo && v <= hi;
                    check(hit[v] == want,
                          "cover [" + lo + "," + hi + "] " + (want ? "includes" : "excludes") + " " + v);
                }
            }
        }
    }

    /**
     * The protocol field is 8 bits, so a predicate over it alone can be checked
     * by counting: a range of n values must have sat count n * 2^96.
     */
    private static void testRangeSatCount(PacketDd dd) {
        Encoder enc = new Encoder(dd);
        double rest = Math.pow(2, Fields.TOTAL_BITS - Fields.WIDTH[Fields.PROTOCOL]);
        int[][] cases = {{0, 0}, {6, 6}, {0, 255}, {1, 3}, {17, 200}, {128, 255}, {5, 5}};
        for (int[] c : cases) {
            int h = enc.range(Fields.PROTOCOL, c[0], c[1]);
            double want = (c[1] - c[0] + 1) * rest;
            double got = dd.satCount(h);
            check(Math.abs(got - want) / want < 1e-9,
                  dd.name() + " protocol range [" + c[0] + "," + c[1] + "] sat count");
            dd.deref(h);
        }
        // A /24 fixes 24 of the 104 bits.
        int p = enc.prefix(Fields.DST_IP, Rule.ipToLong("10.1.2.0"), 24);
        check(Math.abs(dd.satCount(p) - Math.pow(2, Fields.TOTAL_BITS - 24))
                      / Math.pow(2, Fields.TOTAL_BITS - 24) < 1e-9,
              dd.name() + " /24 prefix sat count");
        dd.deref(p);
    }

    /**
     * Three rules whose atoms can be read off by hand. Two disjoint /24s and one
     * /16 containing both: first match wins, so the /16 rule contributes only
     * what the two /24s left, and the header space splits into exactly four
     * atoms -- the two /24s, the rest of the /16, and everything outside it.
     */
    private static void testKnownAtoms(PacketDd dd) {
        Encoder enc = new Encoder(dd);
        List<Rule> rules = new ArrayList<>();
        rules.add(Rule.parse("permit any 10.1.1.0/24 any any any"));
        rules.add(Rule.parse("deny   any 10.1.2.0/24 any any any"));
        rules.add(Rule.parse("permit any 10.1.0.0/16 any any any"));

        List<Integer> preds = new ArrayList<>();
        for (Rule r : rules) preds.add(enc.match(r));

        Dataset empty = null;   // the AP kernel needs no topology
        Verifier v = new Verifier(dd, empty);
        Verifier.Atoms ap = v.atomicPredicates(preds, 0);
        check(ap.size() == 4, dd.name() + " three nested rules give 4 atoms, got " + ap.size());

        // And the ACL's permit set is the /16 minus the denied /24.
        int permit = enc.permitSet(rules);
        double want = Math.pow(2, Fields.TOTAL_BITS - 16) - Math.pow(2, Fields.TOTAL_BITS - 24);
        double got = dd.satCount(permit);
        check(Math.abs(got - want) / want < 1e-9,
              dd.name() + " permit set is the /16 less the denied /24");
        dd.deref(permit);
        for (int h : preds) dd.deref(h);
    }

    public static void main(String[] args) {
        System.out.println("cover decomposition");
        testCover();

        for (String name : new String[]{"jdd", "gcflobdd-abs"}) {
            System.out.println(name);
            PacketDd dd = name.equals("jdd")
                    ? new JddBackend(1 << 22, 1 << 20)
                    : new GcflobddBackend(GcflobddEngine.CONFIG_ALIGNED_BALANCED_SHARED);
            testRangeSatCount(dd);
            testKnownAtoms(dd);
            dd.close();
        }

        System.out.println(failures == 0 ? "SELFTEST ok" : "SELFTEST " + failures + " failures");
        if (failures != 0) System.exit(1);
    }
}
