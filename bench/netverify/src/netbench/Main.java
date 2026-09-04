package netbench;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import application.wan.bdd.verifier.common.GcflobddEngine;
import netbench.backend.GcflobddBackend;
import netbench.backend.JddBackend;
import netbench.backend.NddBackend;

/**
 * One driver, one workload, any backend. Prints a single machine-readable
 * {@code RESULT} line that {@code scripts/compare_netverify.sh} parses into CSV,
 * the same contract {@code tests/quantum.rs} uses.
 *
 * <pre>
 * java netbench.Main --backend jdd --data data/fattree4_... --workload all
 * </pre>
 */
public final class Main {

    /** 2^104, the size of the header space, used to normalise sat counts. */
    private static final double SPACE = Math.pow(2, Fields.TOTAL_BITS);

    private static PacketDd open(String backend, int nodes, int cache) {
        return switch (backend) {
            case "jdd"           -> new JddBackend(nodes, cache);
            case "ndd"           -> new NddBackend(nodes, nodes, cache);
            case "gcflobdd-fg"   -> new GcflobddBackend(GcflobddEngine.CONFIG_FIELD_GROUPED);
            case "gcflobdd-ab"   -> new GcflobddBackend(GcflobddEngine.CONFIG_ALIGNED_BALANCED);
            case "gcflobdd-abs"  -> new GcflobddBackend(GcflobddEngine.CONFIG_ALIGNED_BALANCED_SHARED);
            default -> throw new IllegalArgumentException("unknown backend " + backend);
        };
    }

    public static void main(String[] args) throws Exception {
        String backend = "jdd", workload = "all", dataDir = null;
        int cap = 0, nodes = 4_000_000, cache = 1_000_000;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--backend"  -> backend = args[++i];
                case "--data"     -> dataDir = args[++i];
                case "--workload" -> workload = args[++i];
                case "--cap"      -> cap = Integer.parseInt(args[++i]);
                case "--nodes"    -> nodes = Integer.parseInt(args[++i]);
                case "--cache"    -> cache = Integer.parseInt(args[++i]);
                default -> throw new IllegalArgumentException("unknown flag " + args[i]);
            }
        }
        if (dataDir == null) throw new IllegalArgumentException("--data is required");

        Dataset data = Dataset.load(Path.of(dataDir));
        Runtime rt = Runtime.getRuntime();
        rt.gc(); rt.gc();
        long heapBefore = rt.totalMemory() - rt.freeMemory();

        StringBuilder out = new StringBuilder("RESULT");
        try (PacketDd dd = open(backend, nodes, cache)) {
            Verifier v = new Verifier(dd, data);

            long t0 = System.nanoTime();
            v.buildForwarding();
            long t1 = System.nanoTime();
            v.buildAcls();
            long t2 = System.nanoTime();

            List<Integer> preds = v.allPredicates();
            List<Integer> distinct = new ArrayList<>(new java.util.LinkedHashSet<>(preds));

            // Normalised sat counts are the cross-backend oracle: a fraction of
            // the 2^104 header space, well-conditioned in a double where the raw
            // count is not.
            double fwdFraction = 0, aclFraction = 0;
            for (int p : v.portPredicate.values()) fwdFraction += dd.satCount(p) / SPACE;
            for (int p : v.aclPredicate.values())  aclFraction += dd.satCount(p) / SPACE;

            long apNs = 0, reachNs = 0;
            int atomCount = -1, pairs = -1;
            String status = "ok";

            if (workload.equals("ap") || workload.equals("reach") || workload.equals("all")) {
                long a0 = System.nanoTime();
                Verifier.Atoms atoms = v.atomicPredicates(distinct, cap);
                apNs = System.nanoTime() - a0;
                if (atoms == null) {
                    status = "cap";
                } else {
                    atomCount = atoms.size();
                    if (workload.equals("reach") || workload.equals("all")) {
                        long r0 = System.nanoTime();
                        pairs = v.reachability(atoms).pairs();
                        reachNs = System.nanoTime() - r0;
                    }
                }
            }

            int equalAclPairs = -1;
            if (workload.equals("equiv") || workload.equals("all")) {
                equalAclPairs = 0;
                List<Integer> vals = new ArrayList<>(v.aclPredicate.values());
                for (int i = 0; i < vals.size(); i++) {
                    for (int j = i + 1; j < vals.size(); j++) {
                        if (vals.get(i).intValue() == vals.get(j).intValue()) equalAclPairs++;
                    }
                }
            }

            // Size of the largest single predicate, so "diagram size" means one
            // diagram rather than the whole engine.
            int biggestNodes = -1, biggestEdges = -1, biggestConv = -1;
            int biggest = PacketDd.FALSE;
            for (int p : distinct) {
                int n = dd.diagramNodes(p);
                if (n > biggestNodes) { biggestNodes = n; biggest = p; }
            }
            if (biggestNodes >= 0) {
                biggestEdges = dd.diagramEdges(biggest);
                if (dd instanceof GcflobddBackend g) biggestConv = g.convTotal(biggest);
            }

            rt.gc(); rt.gc();
            long heapAfter = rt.totalMemory() - rt.freeMemory();

            out.append(" impl=").append(dd.name())
               .append(" workload=").append(workload)
               .append(" dataset=").append(data.name)
               .append(" fib_rules=").append(data.fib.size())
               .append(" acl_rules=").append(data.aclRuleCount())
               .append(" predicates=").append(distinct.size())
               .append(" fwd_ms=").append(ms(t1 - t0))
               .append(" acl_ms=").append(ms(t2 - t1))
               .append(" ap_ms=").append(ms(apNs))
               .append(" reach_ms=").append(ms(reachNs))
               .append(" total_ms=").append(ms(t2 - t0 + apNs + reachNs))
               .append(" atoms=").append(atomCount)
               .append(" pairs=").append(pairs)
               .append(" equal_acl_pairs=").append(equalAclPairs)
               .append(" fwd_fraction=").append(String.format("%.12f", fwdFraction))
               .append(" acl_fraction=").append(String.format("%.12f", aclFraction))
               .append(" engine_nodes=").append(dd.engineNodes())
               .append(" label_nodes=").append(dd.engineAuxNodes())
               .append(" engine_bytes=").append(dd.engineBytes())
               .append(" max_nodes=").append(biggestNodes)
               .append(" max_edges=").append(biggestEdges)
               .append(" max_conv=").append(biggestConv)
               .append(" heap_mb=").append((heapAfter - heapBefore) / (1024 * 1024))
               .append(" status=").append(status);
        }
        System.out.println(out);
    }

    private static String ms(long ns) {
        return String.format("%.3f", ns / 1_000_000.0);
    }
}
