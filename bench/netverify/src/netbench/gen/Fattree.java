package netbench.gen;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import netbench.Rule;

/**
 * Seeded generator for a k-ary fat-tree data plane, written to plain text so
 * every backend reads the same bytes.
 *
 * <p>NDD ships no network data -- {@code /datasets/} is gitignored on every
 * branch and the drivers hardcode {@code /data/zcli-data/.../wan/purdue/pd} --
 * so the workload has to be ours. This produces the standard fat-tree of the
 * literature: {@code k} pods, {@code k/2} edge and {@code k/2} aggregation
 * switches per pod, {@code (k/2)^2} core switches, edge switch {@code i} of pod
 * {@code p} owning {@code 10.p.i.0/24}.
 *
 * <p>Files written into the output directory:
 * <ul>
 *   <li>{@code topo.txt} -- {@code dev1 port1 dev2 port2}, one L1 link per line
 *   <li>{@code fib.txt} -- {@code device port ip/len}, longest-prefix-match
 *   <li>{@code edgeports.txt} -- {@code device port}, the host-facing ports
 *   <li>{@code acl.txt} -- {@code aclName <five-tuple rule>}, first match wins
 *   <li>{@code aclmap.txt} -- {@code device port aclName}
 * </ul>
 */
public final class Fattree {

    private final int k;
    private final int half;
    private final int extraRoutes;
    private final int aclRules;
    private final Random rng;

    private final List<String> topo = new ArrayList<>();
    private final List<String> fib = new ArrayList<>();
    private final List<String> edgePorts = new ArrayList<>();
    private final List<String> acl = new ArrayList<>();
    private final List<String> aclMap = new ArrayList<>();

    public Fattree(int k, int extraRoutes, int aclRules, long seed) {
        if (k < 2 || k % 2 != 0) throw new IllegalArgumentException("k must be even and >= 2");
        this.k = k;
        this.half = k / 2;
        this.extraRoutes = extraRoutes;
        this.aclRules = aclRules;
        this.rng = new Random(seed);
    }

    private static String edge(int p, int i) { return "e_" + p + "_" + i; }
    private static String agg(int p, int i)  { return "a_" + p + "_" + i; }
    private static String core(int i, int j) { return "c_" + i + "_" + j; }

    /** {@code 10.p.i.0/24}, the subnet behind edge switch {@code i} of pod {@code p}. */
    private static long subnet(int p, int i) {
        return (10L << 24) | ((long) p << 16) | ((long) i << 8);
    }

    private void link(String a, int pa, String b, int pb) {
        topo.add(a + " " + pa + " " + b + " " + pb);
    }

    private void route(String dev, int port, long ip, int len) {
        fib.add(dev + " " + port + " " + Rule.longToIp(ip) + "/" + len);
    }

    public void build() {
        buildTopology();
        buildFib();
        buildAcls();
    }

    private void buildTopology() {
        // Edge ports 0..half-1 face hosts; half..k-1 go up to the pod's aggs.
        // Agg  ports 0..half-1 go down to the pod's edges; half..k-1 up to core.
        // Core c_i_j has one port per pod.
        for (int p = 0; p < k; p++) {
            for (int i = 0; i < half; i++) {
                for (int h = 0; h < half; h++) {
                    edgePorts.add(edge(p, i) + " " + h);
                }
                for (int a = 0; a < half; a++) {
                    link(edge(p, i), half + a, agg(p, a), i);
                }
            }
            for (int a = 0; a < half; a++) {
                for (int c = 0; c < half; c++) {
                    link(agg(p, a), half + c, core(a, c), p);
                }
            }
        }
    }

    private void buildFib() {
        // Edge: a /32 per attached host on its own port, default up (ECMP).
        for (int p = 0; p < k; p++) {
            for (int i = 0; i < half; i++) {
                long net = subnet(p, i);
                for (int h = 0; h < half; h++) {
                    route(edge(p, i), h, net + h + 2, 32);
                }
                for (int a = 0; a < half; a++) {
                    route(edge(p, i), half + a, 0, 0);
                }
            }
            // Agg: one /24 per edge in the pod, default up (ECMP).
            for (int a = 0; a < half; a++) {
                for (int i = 0; i < half; i++) {
                    route(agg(p, a), i, subnet(p, i), 24);
                }
                for (int c = 0; c < half; c++) {
                    route(agg(p, a), half + c, 0, 0);
                }
            }
        }
        // Core: one /16 per pod.
        for (int i = 0; i < half; i++) {
            for (int j = 0; j < half; j++) {
                for (int p = 0; p < k; p++) {
                    route(core(i, j), p, (10L << 24) | ((long) p << 16), 16);
                }
            }
        }
        // Extra routes stand in for an injected BGP table: random prefixes spread
        // over core down-ports, which is what makes the FIB a size ladder rather
        // than a fixed shape.
        for (int r = 0; r < extraRoutes; r++) {
            int i = rng.nextInt(half), j = rng.nextInt(half), p = rng.nextInt(k);
            int len = 8 + rng.nextInt(17);              // /8 .. /24
            long ip = (rng.nextInt(223 - 11) + 11L) << 24
                    | (long) rng.nextInt(256) << 16
                    | (long) rng.nextInt(256) << 8;
            ip &= len == 0 ? 0 : (0xffffffffL << (32 - len)) & 0xffffffffL;
            route(core(i, j), p, ip, len);
        }
    }

    /** Well-known service ports an ACL is likely to name. */
    private static final int[] SERVICES = {22, 23, 25, 53, 80, 110, 143, 443, 445, 3389, 8080};
    private static final int[] PROTOCOLS = {6, 17, 1};

    private void buildAcls() {
        for (int p = 0; p < k; p++) {
            for (int i = 0; i < half; i++) {
                String name = "acl_" + p + "_" + i;
                for (int h = 0; h < half; h++) {
                    aclMap.add(edge(p, i) + " " + h + " " + name);
                }
                for (int r = 0; r < aclRules; r++) {
                    acl.add(name + " " + randomRule().render());
                }
                // A default deny, as every real ACL ends.
                acl.add(name + " deny any any any any any");
            }
        }
    }

    private Rule randomRule() {
        boolean permit = rng.nextInt(4) != 0;              // ~75% permit
        long[] src = randomPrefix();
        long[] dst = randomPrefix();
        int[] sp = randomPortRange(true);
        int[] dp = randomPortRange(false);
        int[] pr = randomProtocol();
        return new Rule(permit, src[0], (int) src[1], dst[0], (int) dst[1],
                sp[0], sp[1], dp[0], dp[1], pr[0], pr[1]);
    }

    /** Prefixes drawn from the fat-tree's own address space, lengths skewed to /16 and /24. */
    private long[] randomPrefix() {
        int roll = rng.nextInt(10);
        if (roll == 0) return new long[]{0, 0};                       // any
        int p = rng.nextInt(k);
        if (roll <= 3) return new long[]{(10L << 24) | ((long) p << 16), 16};
        int i = rng.nextInt(half);
        if (roll <= 8) return new long[]{subnet(p, i), 24};
        return new long[]{subnet(p, i) + rng.nextInt(half) + 2, 32};
    }

    private int[] randomPortRange(boolean source) {
        int roll = rng.nextInt(10);
        if (source ? roll <= 6 : roll <= 2) return new int[]{-1, -1};  // any
        if (roll <= 7) return new int[]{1024, 65535};                  // ephemeral
        int svc = SERVICES[rng.nextInt(SERVICES.length)];
        if (roll == 9) return new int[]{svc, svc + rng.nextInt(16)};   // a small block
        return new int[]{svc, svc};
    }

    private int[] randomProtocol() {
        if (rng.nextInt(4) == 0) return new int[]{-1, -1};
        int pr = PROTOCOLS[rng.nextInt(PROTOCOLS.length)];
        return new int[]{pr, pr};
    }

    private static void write(Path dir, String file, List<String> lines) throws IOException {
        try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(dir.resolve(file)))) {
            for (String l : lines) pw.println(l);
        }
    }

    public void writeTo(Path dir) throws IOException {
        Files.createDirectories(dir);
        write(dir, "topo.txt", topo);
        write(dir, "fib.txt", fib);
        write(dir, "edgeports.txt", edgePorts);
        write(dir, "acl.txt", acl);
        write(dir, "aclmap.txt", aclMap);
    }

    public String summary() {
        return "k=" + k + " switches=" + (k * half * 2 + half * half)
                + " links=" + topo.size() + " fib=" + fib.size()
                + " edge_ports=" + edgePorts.size() + " acl_rules=" + acl.size();
    }

    public static void main(String[] args) throws IOException {
        int k = 4, routes = 0, aclRules = 8;
        long seed = 1;
        String out = null;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--k"         -> k = Integer.parseInt(args[++i]);
                case "--routes"    -> routes = Integer.parseInt(args[++i]);
                case "--acl-rules" -> aclRules = Integer.parseInt(args[++i]);
                case "--seed"      -> seed = Long.parseLong(args[++i]);
                case "--out"       -> out = args[++i];
                default -> throw new IllegalArgumentException("unknown flag " + args[i]);
            }
        }
        if (out == null) out = "data/fattree" + k + "_r" + routes + "_a" + aclRules + "_s" + seed;
        Fattree f = new Fattree(k, routes, aclRules, seed);
        f.build();
        f.writeTo(Path.of(out));
        System.out.println(f.summary() + " -> " + out);
    }
}
