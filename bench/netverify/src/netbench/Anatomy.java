package netbench;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import application.wan.bdd.verifier.common.GcflobddEngine;
import netbench.backend.GcflobddBackend;
import netbench.backend.JddBackend;
import netbench.backend.NddBackend;

/**
 * Why one engine is faster than another on this workload, rather than by how
 * much.
 *
 * <p>{@link Main} reports stage times. This reports the two things those times
 * are made of: how many decision-diagram operations each stage performs, and how
 * much structure each operation has to walk. Dividing one by the other gives a
 * per-operation cost that is comparable across engines, because every engine
 * runs the identical driver over the identical dataset.
 *
 * <pre>
 * java netbench.Anatomy --backend ndd --data data/ft10_r250_a8_s1
 * </pre>
 */
public final class Anatomy {

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
        String backend = "jdd", dataDir = null;
        int nodes = 4_000_000, cache = 1_000_000;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--backend" -> backend = args[++i];
                case "--data"    -> dataDir = args[++i];
                case "--nodes"   -> nodes = Integer.parseInt(args[++i]);
                case "--cache"   -> cache = Integer.parseInt(args[++i]);
                default -> throw new IllegalArgumentException("unknown flag " + args[i]);
            }
        }
        if (dataDir == null) throw new IllegalArgumentException("--data is required");

        Dataset data = Dataset.load(Path.of(dataDir));
        StringBuilder out = new StringBuilder("ANATOMY");

        try (PacketDd raw = open(backend, nodes, cache)) {
            CountingDd dd = new CountingDd(raw);
            Verifier v = new Verifier(dd, data);

            long t0 = System.nanoTime();
            v.buildForwarding();
            v.buildAcls();
            long buildNs = System.nanoTime() - t0;
            long buildOps = dd.binaryOps();

            List<Integer> distinct = new ArrayList<>(new LinkedHashSet<>(v.allPredicates()));

            dd.reset();
            long a0 = System.nanoTime();
            Verifier.Atoms atoms = v.atomicPredicates(distinct, 0);
            long apNs = System.nanoTime() - a0;
            long apOps = dd.binaryOps();

            // Structure of the predicates the loop actually conjoins: the median
            // and the largest, since a mean over a long tail says little.
            int[] sizes = new int[distinct.size()];
            int biggest = PacketDd.FALSE, biggestNodes = -1;
            for (int i = 0; i < distinct.size(); i++) {
                sizes[i] = raw.diagramNodes(distinct.get(i));
                if (sizes[i] > biggestNodes) { biggestNodes = sizes[i]; biggest = distinct.get(i); }
            }
            java.util.Arrays.sort(sizes);
            int median = sizes.length == 0 ? -1 : sizes[sizes.length / 2];

            // The same for the atoms, which is what the loop conjoins against.
            int[] atomSizes = new int[atoms.size()];
            for (int i = 0; i < atoms.size(); i++) atomSizes[i] = raw.diagramNodes(atoms.atoms.get(i));
            java.util.Arrays.sort(atomSizes);
            int atomMedian = atomSizes.length == 0 ? -1 : atomSizes[atomSizes.length / 2];
            int atomMax = atomSizes.length == 0 ? -1 : atomSizes[atomSizes.length - 1];

            out.append(" impl=").append(raw.name())
               .append(" dataset=").append(data.name)
               .append(" devices=").append(data.deviceCount())
               .append(" ports=").append(v.portPredicate.size())
               .append(" fib_rules=").append(data.fib.size())
               .append(" acl_rules=").append(data.aclRuleCount())
               .append(" predicates=").append(distinct.size())
               .append(" atoms=").append(atoms.size())
               .append(" build_ops=").append(buildOps)
               .append(" build_ns_per_op=").append(buildOps == 0 ? -1 : buildNs / buildOps)
               .append(" ap_ops=").append(apOps)
               .append(" ap_ms=").append(apNs / 1_000_000)
               .append(" ap_ns_per_op=").append(apOps == 0 ? -1 : apNs / apOps)
               .append(" pred_nodes_median=").append(median)
               .append(" pred_nodes_max=").append(biggestNodes)
               .append(" atom_nodes_median=").append(atomMedian)
               .append(" atom_nodes_max=").append(atomMax)
               .append(" pred_edges_max=").append(raw.diagramEdges(biggest));

            if (raw instanceof NddBackend n) {
                out.append(" ndd_label_nodes_max=").append(n.labelNodes(biggest))
                   .append(" ndd_fields_touched_max=").append(n.fieldsTouched(biggest));
                int fieldsSum = 0, labelSum = 0, nddSum = 0;
                for (int p : distinct) {
                    fieldsSum += n.fieldsTouched(p);
                    labelSum  += n.labelNodes(p);
                    nddSum    += n.diagramNodes(p);
                }
                out.append(" ndd_fields_touched_mean=")
                   .append(String.format("%.2f", fieldsSum / (double) distinct.size()))
                   .append(" ndd_nodes_mean=")
                   .append(String.format("%.2f", nddSum / (double) distinct.size()))
                   .append(" ndd_label_nodes_mean=")
                   .append(String.format("%.2f", labelSum / (double) distinct.size()));
            }
            // The atom count at five points through the loop. Work is the sum of
            // this trajectory, so a back-loaded one means the last predicates
            // dominate and the exponent is steeper than the final atom count
            // alone would suggest.
            List<Integer> traj = v.atomTrajectory;
            if (!traj.isEmpty()) {
                double[] pct = {50, 80, 90, 95, 99, 100};
                out.append(" atom_traj@50/80/90/95/99/100pct=");
                for (int q = 0; q < pct.length; q++) {
                    int idx = (int) Math.min(traj.size() - 1, Math.round(pct[q] / 100.0 * traj.size()) - 1);
                    out.append(traj.get(Math.max(0, idx)));
                    if (q < pct.length - 1) out.append('/');
                }
                // Where the forwarding predicates end and the ACLs begin: the
                // first is a laminar family of prefixes, the second is not.
                out.append(" fwd_predicates=").append(v.portPredicate.size());
                long sum = 0;
                for (int a : traj) sum += a;
                out.append(" atom_traj_sum=").append(sum);
            }
            out.append(" engine_nodes=").append(raw.engineNodes())
               .append(" label_nodes=").append(raw.engineAuxNodes());
        }
        System.out.println(out);
    }
}
