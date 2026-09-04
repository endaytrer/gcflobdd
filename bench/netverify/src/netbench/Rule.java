package netbench;

/**
 * One five-tuple classifier rule, in the text form the generator emits and every
 * backend reads:
 *
 * <pre>{@code
 * permit 10.0.1.0/24 10.0.2.0/24 0-65535 80-80 6-6
 * deny   any         10.4.0.0/16 1024-65535 22-22 6-6
 * }</pre>
 *
 * <p>A field is a wildcard when its prefix length is 0 or its range spans the
 * whole width; wildcards are dropped from the conjunction rather than encoded,
 * matching {@code BDDACLWrapper.ConvertACLRule}'s "just a shortcut" branches.
 * Priority is the line's position: earlier lines win, as in a Cisco ACL.
 */
public final class Rule {

    public final boolean permit;
    public final long srcIp;
    public final int srcLen;
    public final long dstIp;
    public final int dstLen;
    public final int srcPortLo, srcPortHi;
    public final int dstPortLo, dstPortHi;
    public final int protoLo, protoHi;

    public Rule(boolean permit, long srcIp, int srcLen, long dstIp, int dstLen,
                int srcPortLo, int srcPortHi, int dstPortLo, int dstPortHi,
                int protoLo, int protoHi) {
        this.permit = permit;
        this.srcIp = srcIp; this.srcLen = srcLen;
        this.dstIp = dstIp; this.dstLen = dstLen;
        this.srcPortLo = srcPortLo; this.srcPortHi = srcPortHi;
        this.dstPortLo = dstPortLo; this.dstPortHi = dstPortHi;
        this.protoLo = protoLo; this.protoHi = protoHi;
    }

    public static Rule parse(String line) {
        String[] t = line.trim().split("\\s+");
        if (t.length != 6) {
            throw new IllegalArgumentException("expected 6 fields, got " + t.length + ": " + line);
        }
        boolean permit = t[0].equalsIgnoreCase("permit");
        long[] s = parseCidr(t[1]);
        long[] d = parseCidr(t[2]);
        int[] sp = parseRange(t[3]);
        int[] dp = parseRange(t[4]);
        int[] pr = parseRange(t[5]);
        return new Rule(permit, s[0], (int) s[1], d[0], (int) d[1],
                sp[0], sp[1], dp[0], dp[1], pr[0], pr[1]);
    }

    public String render() {
        return (permit ? "permit" : "deny")
                + " " + renderCidr(srcIp, srcLen)
                + " " + renderCidr(dstIp, dstLen)
                + " " + renderRange(srcPortLo, srcPortHi)
                + " " + renderRange(dstPortLo, dstPortHi)
                + " " + renderRange(protoLo, protoHi);
    }

    /** A wildcard range renders as {@code any}, so the text round-trips. */
    public static String renderRange(int lo, int hi) {
        if (lo < 0 || hi < 0) return "any";
        return lo == hi ? Integer.toString(lo) : lo + "-" + hi;
    }

    /** {@code "any"} or {@code "a.b.c.d/len"} -> {@code {ip, len}}. */
    public static long[] parseCidr(String s) {
        if (s.equalsIgnoreCase("any")) return new long[]{0, 0};
        int slash = s.indexOf('/');
        String addr = slash < 0 ? s : s.substring(0, slash);
        int len = slash < 0 ? 32 : Integer.parseInt(s.substring(slash + 1));
        return new long[]{ipToLong(addr), len};
    }

    public static int[] parseRange(String s) {
        if (s.equalsIgnoreCase("any")) return new int[]{-1, -1};
        int dash = s.indexOf('-', 1);   // skip a leading sign
        if (dash < 0) { int v = Integer.parseInt(s); return new int[]{v, v}; }
        return new int[]{Integer.parseInt(s.substring(0, dash)),
                         Integer.parseInt(s.substring(dash + 1))};
    }

    public static String renderCidr(long ip, int len) {
        return len == 0 ? "any" : longToIp(ip) + "/" + len;
    }

    public static long ipToLong(String s) {
        String[] o = s.split("\\.");
        long v = 0;
        for (String part : o) v = (v << 8) | (Long.parseLong(part) & 0xff);
        return v & 0xffffffffL;
    }

    public static String longToIp(long v) {
        return ((v >> 24) & 0xff) + "." + ((v >> 16) & 0xff) + "."
                + ((v >> 8) & 0xff) + "." + (v & 0xff);
    }
}
