package netbench;

import java.util.ArrayList;
import java.util.List;

/**
 * Turns five-tuple rules into decision-diagram predicates, using only the
 * {@link PacketDd} interface, so all three engines encode the same rule the same
 * way and any difference in the result is the engine's, not the encoding's.
 *
 * <p>Ported from {@code BDDACLWrapper.ConvertACLRule} and {@code Range}: an IP
 * prefix is a conjunction of literals over the field's top bits, a port or
 * protocol range is the disjunction of its prefix cover, and a wildcard field is
 * omitted entirely.
 *
 * <p><b>Reference discipline.</b> Every method here returns a handle carrying
 * one reference that the caller owns and must {@code deref}. Intermediates are
 * referenced before the next engine call and released straight after. This is
 * not optional bookkeeping: JDD collects garbage inside {@code mk}, so an
 * unreferenced intermediate can have its slot recycled by the very next
 * operation, and the run then silently computes a different answer. Getting this
 * wrong showed up as JDD reporting 82,050 atomic predicates where every other
 * engine reported 81,993.
 */
public final class Encoder {

    private final PacketDd dd;

    public Encoder(PacketDd dd) { this.dd = dd; }

    /** {@code and} that consumes both operands' references and returns a new one. */
    private int conj(int a, int b) {
        int r = dd.ref(dd.and(a, b));
        dd.deref(b);
        dd.deref(a);
        return r;
    }

    /** {@code or} that consumes both operands' references and returns a new one. */
    private int disj(int a, int b) {
        int r = dd.ref(dd.or(a, b));
        dd.deref(b);
        dd.deref(a);
        return r;
    }

    /**
     * Conjunction of the top {@code len} bits of {@code field} equalling the top
     * {@code len} bits of {@code value}. {@code len == 0} is the wildcard, TRUE.
     */
    public int prefix(int field, long value, int len) {
        int width = Fields.WIDTH[field];
        if (len <= 0) return dd.ref(PacketDd.TRUE);
        if (len > width) throw new IllegalArgumentException("prefix longer than field");
        int acc = dd.ref(PacketDd.TRUE);
        for (int bit = 0; bit < len; bit++) {
            // bit 0 is the field's MSB, so bit i tests value's (width-1-i)-th bit.
            boolean one = ((value >>> (width - 1 - bit)) & 1L) != 0;
            int lit = dd.var(field, bit);
            int term = dd.ref(one ? lit : dd.not(lit));
            acc = conj(acc, term);
        }
        return acc;
    }

    /**
     * {@code lo <= field <= hi} as the disjunction of the range's prefix cover.
     * A range covering the whole field is TRUE and costs nothing.
     */
    public int range(int field, int lo, int hi) {
        int width = Fields.WIDTH[field];
        long max = (1L << width) - 1;
        if (lo < 0 || hi < 0) return dd.ref(PacketDd.TRUE);   // "any"
        if (lo <= 0 && hi >= max) return dd.ref(PacketDd.TRUE);
        if (lo > hi) return dd.ref(PacketDd.FALSE);

        int acc = dd.ref(PacketDd.FALSE);
        for (long[] p : cover(lo, hi, width)) {
            acc = disj(acc, prefix(field, p[0], (int) p[1]));
        }
        return acc;
    }

    /**
     * Decompose {@code [lo, hi]} into the fewest aligned power-of-two blocks,
     * each returned as {@code {base, prefixLength}}. The textbook range-to-prefix
     * split: take the largest block that both starts at {@code lo} and fits.
     */
    public static List<long[]> cover(long lo, long hi, int width) {
        List<long[]> out = new ArrayList<>();
        while (lo <= hi) {
            // Largest aligned block starting at lo: limited by lo's lowest set bit...
            int size = (lo == 0) ? width : Long.numberOfTrailingZeros(lo);
            if (size > width) size = width;
            // ...and by how much room is left up to hi.
            while (size > 0 && lo + (1L << size) - 1 > hi) size--;
            out.add(new long[]{lo, width - size});
            long next = lo + (1L << size);
            if (next <= lo) break;   // wrapped past the top of the field
            lo = next;
        }
        return out;
    }

    /** The set of packets a single rule matches, wildcards omitted. Caller owns it. */
    public int match(Rule r) {
        int acc = dd.ref(PacketDd.TRUE);
        acc = conj(acc, prefix(Fields.SRC_IP, r.srcIp, r.srcLen));
        acc = conj(acc, prefix(Fields.DST_IP, r.dstIp, r.dstLen));
        acc = conj(acc, range(Fields.SRC_PORT, r.srcPortLo, r.srcPortHi));
        acc = conj(acc, range(Fields.DST_PORT, r.dstPortLo, r.dstPortHi));
        acc = conj(acc, range(Fields.PROTOCOL, r.protoLo, r.protoHi));
        return acc;
    }

    /**
     * The packets an ordered ACL permits: first match wins, so a rule only
     * contributes what no earlier rule already claimed. This is APKeep's
     * {@code ConvertACLs} loop. Caller owns the result.
     */
    public int permitSet(List<Rule> rules) {
        int permit = dd.ref(PacketDd.FALSE);
        int matched = dd.ref(PacketDd.FALSE);
        for (Rule r : rules) {
            int m = match(r);                                  // owned
            int fresh = dd.ref(dd.diff(m, matched));
            if (r.permit) {
                int next = dd.ref(dd.or(permit, fresh));
                dd.deref(permit);
                permit = next;
            }
            int nm = dd.ref(dd.or(matched, m));
            dd.deref(matched);
            matched = nm;
            dd.deref(fresh);
            dd.deref(m);
        }
        dd.deref(matched);
        return permit;
    }
}
